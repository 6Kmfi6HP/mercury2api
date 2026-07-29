// Package browser 原先用 chromedp 驱动本地 Chrome 绕过 Vercel Bot Challenge 后在页面内
// 同源 fetch 调 Inception /api/chat。经实测（见项目记录），Inception 现阶段并不对
// /api/session 与 /api/chat 强制 WAF 挑战：仅需用浏览器级 TLS/HTTP2 指纹（tls-client 伪装
// Chrome）即可放行，/api/chat 的唯一前置是 /api/session 返回的 x-session-token。
//
// 因此本包已重构为纯 Go 指纹直连，完全移除浏览器进程：
//   - 单例 tls-client.HttpClient（Chrome profile），单进程常驻复用；
//   - token 缓存：首次（或 401 失效）时 GET /api/session 取 token，复用调 /api/chat；
//   - 对外接口 NewManager/Start/WaitReady/FetchChat/Close 与旧实现保持一致，
//     internal/inception/executor 与 cmd/main 无需感知此变化。
//
// userDataDir / headless 参数仅为兼容 main 装配签名而保留，本实现已不使用（不再有浏览器）。
package browser

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
)

const (
	homeURL    = "https://chat.inceptionlabs.ai/"
	sessionURL = "https://chat.inceptionlabs.ai/api/session"
	chatURL    = "https://chat.inceptionlabs.ai/api/chat"
	ua         = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
	// fetchChatTimeout 是单次 /api/chat 调用总超时（SSE 可能较长）。
	fetchChatTimeout = 5 * time.Minute
)

// FetchResult 是 /api/chat 的归一化结果，与旧实现定义一致。
type FetchResult struct {
	Status int    // HTTP 状态码
	Body   string // 响应体（成功时是 SSE 文本，失败时是错误页/JSON）
}

// Manager 管理指纹客户端单例与 session token 缓存。无浏览器进程。
type Manager struct {
	userDataDir string // 兼容签名，已不使用
	headless    bool   // 兼容签名，已不使用（永远无浏览器）

	client  tls_client.HttpClient
	tokenMu sync.Mutex // 保护 token 缓存
	token   string

	startOnce sync.Once
	startErr  error
	ready     chan struct{} // close 后表示就绪
}

// NewManager 创建指纹管理器。userDataDir/headless 仅为兼容旧装配签名。
func NewManager(userDataDir string, headless bool) *Manager {
	return &Manager{
		userDataDir: userDataDir,
		headless:    headless,
		ready:       make(chan struct{}),
	}
}

// Start 初始化单例指纹客户端并标记就绪。幂等：sync.Once 保证只执行一次。
// 它不再启动任何浏览器进程。
func (m *Manager) Start(ctx context.Context) error {
	m.startOnce.Do(func() {
		m.startErr = m.start()
		if m.startErr == nil {
			close(m.ready)
		}
	})
	return m.startErr
}

func (m *Manager) start() error {
	c, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(),
		tls_client.WithClientProfile(profiles.Chrome_146),
	)
	if err != nil {
		return fmt.Errorf("browser.start: new tls client: %w", err)
	}
	m.client = c
	return nil
}

// WaitReady 阻塞到指纹客户端就绪或 ctx 取消。
func (m *Manager) WaitReady(ctx context.Context) error {
	select {
	case <-m.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FetchChat 用 Chrome 指纹直连 /api/chat，全量读回 SSE。串行复用单 client。
// inceptionBody 为已翻译好的 Inception 请求 JSON 字符串。
// 首/失效时取 session token，401 自动重取 token 再重试一次。
func (m *Manager) FetchChat(ctx context.Context, inceptionBody string) (*FetchResult, error) {
	if err := m.WaitReady(ctx); err != nil {
		return nil, err
	}

	res, err := m.doFetch(ctx, inceptionBody, false)
	if err != nil {
		return nil, err
	}
	// 401 通常意味着 token 失效或缺失：重取 token 再试一次（仅一次）。
	if res.Status == 401 {
		token, tErr := m.refreshToken(ctx)
		if tErr != nil {
			return res, fmt.Errorf("browser.FetchChat: refresh token: %w", tErr)
		}
		_ = token
		res, err = m.doFetch(ctx, inceptionBody, false)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// doFetch 执行单次 POST /api/chat，不带 token 失效重试。
// needToken=false 时复用缓存 token；needToken=true 强制重取后再调。
func (m *Manager) doFetch(ctx context.Context, inceptionBody string, needToken bool) (*FetchResult, error) {
	tok, err := m.tokenLocked(ctx, needToken)
	if err != nil {
		return nil, err
	}

	req, err := fhttp.NewRequestWithContext(ctx, "POST", chatURL, strings.NewReader(inceptionBody))
	if err != nil {
		return nil, fmt.Errorf("browser.fetch: new request: %w", err)
	}
	setBrowserHeaders(req)
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("x-session-token", tok)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("browser.fetch: POST /api/chat: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("browser.fetch: read body: %w", err)
	}
	return &FetchResult{Status: resp.StatusCode, Body: string(b)}, nil
}

// tokenLocked 返回当前 token（needToken=true 时先强制重取）。线程安全。
func (m *Manager) tokenLocked(ctx context.Context, needToken bool) (string, error) {
	m.tokenMu.Lock()
	defer m.tokenMu.Unlock()
	if needToken || m.token == "" {
		tok, err := m.fetchToken(ctx)
		if err != nil {
			return "", err
		}
		m.token = tok
	}
	return m.token, nil
}

// refreshToken 强制重取 token 并更新缓存，返回新 token。
func (m *Manager) refreshToken(ctx context.Context) (string, error) {
	m.tokenMu.Lock()
	defer m.tokenMu.Unlock()
	tok, err := m.fetchToken(ctx)
	if err != nil {
		return "", err
	}
	m.token = tok
	return tok, nil
}

// fetchToken GET /api/session → {"ok":true,"token":"..."}，提取 token。
func (m *Manager) fetchToken(ctx context.Context) (string, error) {
	req, err := fhttp.NewRequestWithContext(ctx, "GET", sessionURL, nil)
	if err != nil {
		return "", err
	}
	setBrowserHeaders(req)
	resp, err := m.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch token: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("fetch token: read: %w", err)
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("fetch token: upstream %d: %s", resp.StatusCode, truncateStr(string(b), 160))
	}
	tok := extractToken(string(b))
	if tok == "" {
		return "", errors.New("fetch token: empty token in response")
	}
	return tok, nil
}

// Close 释放客户端。幂等。
func (m *Manager) Close() error {
	m.tokenMu.Lock()
	defer m.tokenMu.Unlock()
	// tls-client HttpClient 无显式 Close 必需，置空便于 GC。
	m.client = nil
	return nil
}

// setBrowserHeaders 设一组真实 Chrome 请求头（顺序由 fhttp 保持）。
// 关键：必须带 Sec-Fetch-* 头族。实测（见项目记录）缺这族头会被 Vercel 判为
// 非浏览器并返回 429 + 挑战页，即便 TLS/HTTP2 指纹已伪装成 Chrome。
// 这里用浏览器内 JS fetch('/api/...') 的同源 XHR 头集合，与真实前端调用一致。
func setBrowserHeaders(req *fhttp.Request) {
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Sec-Ch-Ua", `"Chromium";v="146", "Not?A_Brand";v="24", "Google Chrome";v="146"`)
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", `"macOS"`)
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Referer", "https://chat.inceptionlabs.ai/")
}

// extractToken 从 {"ok":true,"token":"..."} 中提取 token 字段。
func extractToken(s string) string {
	i := strings.Index(s, `"token":"`)
	if i < 0 {
		return ""
	}
	s = s[i+len(`"token":"`):]
	j := strings.Index(s, `"`)
	if j < 0 {
		return ""
	}
	return s[:j]
}

func truncateStr(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// 兼容性占位：旧实现里 sse.go 中的 looksLikeChallenge 不再用于 fetch 路径，
// 但保留为可能的外部引用与诊断用途（返回 false 即可，纯直连不再有挑战页）。
var _ = looksLikeChallenge

// looksLikeChallenge 保留以兼容 sse.go 中同名函数的导出意图；纯直连不再命中。
func looksLikeChallenge(body string) bool {
	return strings.Contains(body, "Vercel Security Checkpoint") ||
		strings.Contains(body, "<title>Vercel Security") ||
		strings.Contains(body, "vercel-security-challenge")
}
