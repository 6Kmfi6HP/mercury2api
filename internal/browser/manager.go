package browser

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/chromedp"
)

const (
	homeURL  = "https://chat.inceptionlabs.ai/"
	chatPath = "/api/chat"
	// fetchChatTimeout 是页面内 fetch 一次 /api/chat 的总超时（SSE 可能较长）。
	fetchChatTimeout = 5 * time.Minute
	// navTimeout 是导航 + 等待真实页面（绕过 challenge）的超时。
	navTimeout = 60 * time.Second
)

// FetchResult 是页面内 fetch /api/chat 的归一化结果。
type FetchResult struct {
	Status int    // HTTP 状态码
	Body   string // 响应体（成功时是 SSE 文本，失败时是错误页/JSON）
}

// Manager 管理 chromedp 单例浏览器：启动、通过 Vercel challenge、在页面内
// 用 fetch() 调 Inception API。单 page 复用，所有 Evaluate 串行化（v1 简单可靠）。
type Manager struct {
	userDataDir string
	headless    bool
	allocCtx    context.Context
	allocCancel context.CancelFunc
	browserCtx  context.Context
	browserCxl  context.CancelFunc
	mu          sync.Mutex // 串行所有 Evaluate（单 page 复用）
	startOnce   sync.Once
	startErr    error
	ready       chan struct{} // close 后表示浏览器就绪
}

// NewManager 创建浏览器管理器。userDataDir 用于持久化 challenge cookie 复用；
// headless 为 true 时用无头模式（已验证 headless 可通过 Vercel challenge）。
func NewManager(userDataDir string, headless bool) *Manager {
	return &Manager{
		userDataDir: userDataDir,
		headless:    headless,
		ready:       make(chan struct{}),
	}
}

// Start 同步启动浏览器并导航到首页等待真实页面就绪。
// 内部用 sync.Once 保证只启动一次；重复调用直接返回首次错误。
func (m *Manager) Start(ctx context.Context) error {
	m.startOnce.Do(func() {
		m.startErr = m.start(ctx)
		if m.startErr == nil {
			close(m.ready)
		}
	})
	return m.startErr
}

// start 实际执行启动逻辑（仅在 Start 的 sync.Once 内调用一次）。
func (m *Manager) start(ctx context.Context) error {
	opts := append([]chromedp.ExecAllocatorOption{},
		chromedp.NoFirstRun,
		chromedp.NoDefaultBrowserCheck,
		chromedp.Flag("disable-blink-features", "AutomationControlled"),
		chromedp.Flag("disable-features", "Translate"),
	)
	if m.headless {
		opts = append(opts, chromedp.Headless)
	}
	if m.userDataDir != "" {
		opts = append(opts, chromedp.UserDataDir(m.userDataDir))
	}
	opts = append(opts, chromedp.DefaultExecAllocatorOptions[:]...)

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	m.allocCtx, m.allocCancel = allocCtx, allocCancel

	browserCtx, browserCxl := chromedp.NewContext(allocCtx)
	m.browserCtx, m.browserCxl = browserCtx, browserCxl

	// 首次 NewContext 不分配首个 target；这里手动触发一次导航拿到 page context。
	if err := m.navigateAndWait(ctx, homeURL); err != nil {
		return fmt.Errorf("browser.start: navigate home: %w", err)
	}
	return nil
}

// navigateAndWait 导航到 url 并等待真实页面就绪（challenge 页无 textarea/输入框）。
// adapt 到 Vercel challenge：若检测到 challenge 页，轮询等待其自动完成。
func (m *Manager) navigateAndWait(ctx context.Context, url string) error {
	navCtx, cancel := context.WithTimeout(ctx, navTimeout)
	defer cancel()

	// 等待页面出现可交互输入框（textarea）作为"真实页面"标志；challenge 页不会出现。
	// 同时兜底检查是否落在 challenge 页。
	var located bool
	err := chromedp.Run(m.browserCtx,
		chromedp.Navigate(url),
		chromedp.ActionFunc(func(c context.Context) error {
			// 至少等 document.readyState==complete，再进入判定
			return nil
		}),
	)
	if err != nil {
		return err
	}

	// 轮询：最多等 navTimeout 让 challenge 完成 / 页面就绪。
	deadline, _ := navCtx.Deadline()
	for {
		if time.Now().After(deadline) {
			break
		}
		state, errState := m.pageReadyState(navCtx)
		if errState != nil {
			return errState
		}
		switch state {
		case "ready":
			located = true
		case "challenge":
			// Vercel challenge 页，等待自动完成（5.0s 兜底）
			select {
			case <-time.After(2 * time.Second):
			case <-navCtx.Done():
				return navCtx.Err()
			}
			continue
		default: // "loading"
			select {
			case <-time.After(500 * time.Millisecond):
			case <-navCtx.Done():
				return navCtx.Err()
			}
			continue
		}
		if located {
			return nil
		}
	}
	return errors.New("browser: navigate timeout waiting for real page (challenge not cleared)")
}

// pageReadyState 探测当前页面状态：challenge 页 / 真实页面 / 仍加载。
func (m *Manager) pageReadyState(ctx context.Context) (string, error) {
	js := `(function(){
		var html = document.documentElement ? document.documentElement.outerHTML : '';
		var title = document.title || '';
		var bodyText = (document.body && document.body.innerText) ? document.body.innerText : '';
		if (html.indexOf('Vercel Security Checkpoint') !== -1 || title.indexOf('Vercel Security') !== -1) return 'challenge';
		// 真实页面标志：存在 textarea 或可输入区域
		if (document.querySelector('textarea')) return 'ready';
		if (document.querySelector('[contenteditable=\"true\"]')) return 'ready';
		if (document.readyState === 'complete' && (bodyText.indexOf('Inception') !== -1 || bodyText.length > 200)) return 'ready';
		return 'loading';
	})()`
	var result string
	if err := chromedp.Run(m.browserCtx,
		chromedp.Evaluate(js, &result),
	); err != nil {
		return "", err
	}
	return result, nil
}

// WaitReady 阻塞到浏览器就绪或 ctx 取消。可供 main 启动时使用。
func (m *Manager) WaitReady(ctx context.Context) error {
	select {
	case <-m.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FetchChat 在页面内用同源 fetch 调 /api/chat，全程读回 SSE 后返回完整文本。
// inceptionBody 为已翻译好的 Inception 请求 JSON 字符串。串行复用单 page。
// 检测到 challenge 拦截时自动重新导航重试一次。
func (m *Manager) FetchChat(ctx context.Context, inceptionBody string) (*FetchResult, error) {
	if err := m.WaitReady(ctx); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	res, err := m.doFetch(ctx, inceptionBody)
	if err != nil {
		return nil, err
	}
	// challenge 拦截：重新导航首页刷 challenge cookie 后重试一次
	if res.Status == 429 || looksLikeHTMLChallenge(res.Body) {
		if errNav := m.navigateAndWait(ctx, homeURL); errNav != nil {
			return nil, fmt.Errorf("browser.FetchChat: re-navigate after challenge: %w", errNav)
		}
		res, err = m.doFetch(ctx, inceptionBody)
		if err != nil {
			return nil, err
		}
		if res.Status == 429 || looksLikeHTMLChallenge(res.Body) {
			return res, errors.New("browser: still challenged after retry")
		}
	}
	return res, nil
}

// doFetch 执行单次页面内 fetch，不带重试。
func (m *Manager) doFetch(ctx context.Context, inceptionBody string) (*FetchResult, error) {
	b64 := base64.StdEncoding.EncodeToString([]byte(inceptionBody))
	// 用 IIFE 返回 Promise，配合 WithAwaitPromise 让 chromedp 等待 fetch 完成。
	// body 以 base64 注入（仅含 A-Za-z0-9+/=，无引号/反斜杠风险），页面内 atob 还原。
	js := `(async function(){
		var body = JSON.parse(atob("` + b64 + `"));
		var headers = { 'Content-Type': 'application/json' };
		try {
			var sr = await fetch('/api/session');
			if (sr.ok) {
				var sj = await sr.json();
				if (sj && sj.token) headers['x-session-token'] = sj.token;
			}
		} catch(e) { /* token 可选，失败忽略 */ }
		var r = await fetch('/api/chat', {
			method: 'POST',
			headers: headers,
			body: JSON.stringify(body)
		});
		var text = await r.text();
		return JSON.stringify({ status: r.status, body: text });
	})()`

	fetchCtx, cancel := context.WithTimeout(m.browserCtx, fetchChatTimeout)
	defer cancel()
	// 客户端取消（请求 ctx 取消）时，中止页面内 fetch。AfterFunc 在 Go 1.21+ 可用。
	if ctx != nil && ctx.Err() == nil {
		stop := context.AfterFunc(ctx, cancel)
		defer stop()
	}

	var raw string
	start := time.Now()
	err := chromedp.Run(fetchCtx,
		chromedp.Evaluate(js, &raw,
			func(p *runtime.EvaluateParams) *runtime.EvaluateParams {
				return p.WithAwaitPromise(true)
			},
		),
	)
	if err != nil {
		return nil, fmt.Errorf("browser.fetch: evaluate failed after %s: %w", time.Since(start), err)
	}
	if raw == "" {
		return nil, errors.New("browser.fetch: empty result from page")
	}

	var out struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if errUn := json.Unmarshal([]byte(raw), &out); errUn != nil {
		return nil, fmt.Errorf("browser.fetch: decode result: %w (raw=%q)", errUn, truncate(raw, 200))
	}
	return &FetchResult{Status: out.Status, Body: out.Body}, nil
}

// looksLikeHTMLChallenge 判断响应体是否为 Vercel challenge 拦截页。
func looksLikeHTMLChallenge(body string) bool {
	return strings.Contains(body, "Vercel Security Checkpoint") ||
		strings.Contains(body, "<title>Vercel Security") ||
		strings.Contains(body, "vercel-security-challenge")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// Close 关闭浏览器。幂等。
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.browserCxl != nil {
		m.browserCxl()
		m.browserCxl = nil
	}
	if m.allocCancel != nil {
		m.allocCancel()
		m.allocCancel = nil
	}
	return nil
}
