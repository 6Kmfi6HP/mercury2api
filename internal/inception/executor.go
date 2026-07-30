package inception

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"inception-proxy/internal/browser"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	clipexec "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktr "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// providerKey 是 Inception provider 的标识，须与 auth 文件的 type 字段一致，
// 也须等于 core.Manager 路由 executor 时用的 key。
const providerKey = "inception"

// ID 返回 provider 标识，供 main 装配与模型注册复用同一常量。
func ID() string { return providerKey }

// InceptionExecutor 把 CLIProxyAPI 的 SDK 调用桥接到 Inception Labs 上游。
//
// 数据流（见计划）：
//  1. SDK 透传来的 req.Payload 是原始 OpenAI Chat Completions body；
//  2. 用注册的 requestTransform 把它翻译成 Inception parts 格式 body；
//  3. browser.Manager 在页面内 fetch /api/chat，全量读回 Inception SSE；
//  4. 用注册的 responseTransform 把 SSE 翻译成 OpenAI 字节再返回（SDK 不会自动翻译）。
//
// Inception 上游无需任何凭证，auth 参数仅为满足接口契约，本实现忽略之。
type InceptionExecutor struct {
	Bm *browser.Manager
}

// Identifier 返回 provider key，供 Manager 按 auth.Provider 路由到此 executor。
func (e InceptionExecutor) Identifier() string { return providerKey }

// Execute 处理非流式请求：OpenAI body → Inception body → 浏览器 fetch → OpenAI JSON。
func (e InceptionExecutor) Execute(ctx context.Context, auth *coreauth.Auth, req clipexec.Request, opts clipexec.Options) (clipexec.Response, error) {
	respFormat := clipexec.ResponseFormatOrSource(opts) // 客户端格式（openai）

	// 1. 翻译请求：OpenAI → Inception。
	inceptionBody := sdktr.TranslateRequestByFormatName(sdktr.FormatOpenAI, FormatInception, req.Model, req.Payload, false)
	if len(inceptionBody) == 0 {
		// requestTransform 解析失败 → 请求级错误，不触发凭证轮换。
		return clipexec.Response{}, requestScopedError{err: fmt.Errorf("inception: failed to translate request body"), code: http.StatusBadRequest}
	}

	// 2. 浏览器内 fetch，全量读回 SSE。
	res, err := e.Bm.FetchChat(ctx, string(inceptionBody))
	if err != nil {
		return clipexec.Response{}, mapBrowserError(err)
	}

	// 3. 上游错误码映射。
	if res.Status >= 400 {
		return clipexec.Response{}, mapUpstreamStatus(res.Status, res.Body)
	}

	// 4. 翻译响应：Inception SSE → OpenAI JSON。
	var param any
	out := sdktr.TranslateNonStreamByFormatName(
		ctx, FormatInception, respFormat, req.Model,
		opts.OriginalRequest, inceptionBody, []byte(res.Body), &param,
	)
	if len(out) == 0 {
		return clipexec.Response{}, requestScopedError{err: errors.New("inception: empty translation output"), code: http.StatusBadGateway}
	}
	return clipexec.Response{Payload: out}, nil
}

// ExecuteStream 处理流式请求，真流式：上游 SSE 逐行读，每命中一个 text-delta
// 立刻封装成 OpenAI chunk 推入 channel，handler 逐帧 flush 给客户端，达到打字机效果。
//
// 数据流：OpenAI body → Inception body → FetchChatStream（流式响应体）→
// bufio.Scanner 逐行 → parseDeltaFromLine → openAIStreamChunker.ContentChunk → channel。
//
// 错误两段语义（对齐 SDK handlers_stream.go）：
//   - 首帧前失败（translate 失败、上游 4xx/5xx、token 取失败）：return error，
//     SDK 走 errChan → handler 返回 JSON 错误。
//   - 首帧后中途失败（上游断流、扫描错误）：StreamChunk{Err}，
//     SDK 经 WriteTerminalError 写一条错误 SSE 帧。
//
// [DONE] 与收尾 stop 帧约定：executor 发 finish_reason=stop 收尾帧，[DONE] 终止标记
// 由 SDK handler 在流结束后自动追加（openai_handlers.go WriteDone），不在此手动加。
func (e InceptionExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req clipexec.Request, opts clipexec.Options) (*clipexec.StreamResult, error) {
	inceptionBody := sdktr.TranslateRequestByFormatName(sdktr.FormatOpenAI, FormatInception, req.Model, req.Payload, true)
	if len(inceptionBody) == 0 {
		return nil, requestScopedError{err: errors.New("inception: failed to translate request body"), code: http.StatusBadRequest}
	}

	resp, err := e.Bm.FetchChatStream(ctx, string(inceptionBody))
	if err != nil {
		return nil, mapBrowserError(err)
	}

	// 首帧前：上游 4xx/5xx → 读 body 一段用于错误信息，return error（JSON 错误，不进 SSE）。
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		resp.Body.Close()
		return nil, mapUpstreamStatus(resp.StatusCode, string(bodyBytes))
	}

	// 成功：启 goroutine 增量推流。channel 由本 executor 自管 close 时机。
	ch := make(chan clipexec.StreamChunk, 16)
	go func() {
		defer resp.Body.Close()
		defer close(ch)

		chunker := newOpenAIStreamChunker(req.Model)
		// send 推一帧，ctx 取消则放弃整流。
		send := func(payload []byte) bool {
			if payload == nil {
				return true
			}
			select {
			case ch <- clipexec.StreamChunk{Payload: payload}:
				return true
			case <-ctx.Done():
				return false
			}
		}

		// role 首帧。
		if !send(chunker.RoleChunk()) {
			return
		}

		// 逐行读上游 SSE，4MB 上限与 parseDeltas 一致，防长 text-delta 截断。
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		stopped := false
		for sc.Scan() {
			line := sc.Text()
			if isDoneLine(line) {
				if !send(chunker.StopChunk()) {
					return
				}
				stopped = true
				break
			}
			if delta, ok := parseDeltaFromLine(line); ok {
				if !send(chunker.ContentChunk(delta)) {
					return
				}
			}
		}
		// 上游不发 [DONE] 时补收尾帧（兜底）。
		if !stopped {
			if !send(chunker.StopChunk()) {
				return
			}
		}
		if errScan := sc.Err(); errScan != nil {
			select {
			case ch <- clipexec.StreamChunk{Err: requestScopedError{err: fmt.Errorf("inception: stream read: %w", errScan), code: http.StatusBadGateway}}:
			case <-ctx.Done():
			}
		}
	}()

	return &clipexec.StreamResult{Chunks: ch}, nil
}

// Refresh 无凭证可刷新，直接返回原 auth。
func (e InceptionExecutor) Refresh(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

// CountTokens 当前不支持，返回 not implemented。
func (e InceptionExecutor) CountTokens(ctx context.Context, auth *coreauth.Auth, req clipexec.Request, opts clipexec.Options) (clipexec.Response, error) {
	return clipexec.Response{}, errors.New("inception: count tokens not implemented")
}

// HttpRequest 委托浏览器管理器执行；Inception 必须走浏览器同源 fetch 旁路 WAF，
// 不支持任意 *http.Request 直连，故返回 not supported。
func (e InceptionExecutor) HttpRequest(ctx context.Context, auth *coreauth.Auth, req *http.Request) (*http.Response, error) {
	return nil, errors.New("inception: direct HttpRequest not supported (must route via browser fetch)")
}

// ─── 错误类型 ────────────────────────────────────────────────────────────

// requestScopedError 标记请求级错误：Manager 不应据此判定凭证失效或跨凭证重试。
// 同时实现 StatusCode() 以便 Manager 更新 auth 状态。
type requestScopedError struct {
	err  error
	code int
}

func (r requestScopedError) Error() string { return r.err.Error() }
func (r requestScopedError) Unwrap() error { return r.err }

// IsRequestScoped 告知 Manager 此失败与当前请求相关，而非凭证问题。
func (r requestScopedError) IsRequestScoped() bool { return true }

// StatusCode 满足 executor.StatusError 接口。
func (r requestScopedError) StatusCode() int {
	if r.code == 0 {
		return http.StatusBadGateway
	}
	return r.code
}

// credentialError 表示凭证/上游鉴权失败（401/402），可跨凭证重试。
type credentialError struct {
	err  error
	code int
}

func (c credentialError) Error() string   { return c.err.Error() }
func (c credentialError) Unwrap() error   { return c.err }
func (c credentialError) StatusCode() int { return c.code }

// mapBrowserError 把浏览器层错误映射为合适的执行器错误。
// challenge 仍未通过 / 浏览器不可用 → 503（请求级，避免污染凭证状态）。
func mapBrowserError(err error) error {
	msg := err.Error()
	if strings.Contains(msg, "challeng") {
		return requestScopedError{err: err, code: http.StatusServiceUnavailable}
	}
	return requestScopedError{err: err, code: http.StatusServiceUnavailable}
}

// mapUpstreamStatus 把上游 HTTP 错误码映射为执行器错误。
// 401/402 → 凭证级（虽无凭证，保持语义一致）；5xx → 502 上游错误；
// 429 → 429；其余 → 502。challenge 页命中时已被 FetchChat 兜底重试。
func mapUpstreamStatus(status int, body string) error {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusPaymentRequired:
		return credentialError{err: fmt.Errorf("inception upstream %d: %s", status, truncateBody(body)), code: status}
	case status == http.StatusTooManyRequests:
		return credentialError{err: fmt.Errorf("inception upstream rate limited: %s", truncateBody(body)), code: status}
	case status >= 500:
		return requestScopedError{err: fmt.Errorf("inception upstream %d: %s", status, truncateBody(body)), code: http.StatusBadGateway}
	default:
		return requestScopedError{err: fmt.Errorf("inception upstream %d: %s", status, truncateBody(body)), code: http.StatusBadGateway}
	}
}

func truncateBody(body string) string {
	const max = 200
	body = strings.ReplaceAll(body, "\n", " ")
	if len(body) > max {
		return body[:max] + "…"
	}
	return body
}
