package inception

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"

	sdktr "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

// FormatInception 是 Inception Labs 上游 chat 协议的翻译器格式标识。
// 注册方向：sdktr.FormatOpenAI → FormatInception。
const FormatInception sdktr.Format = "inception"

// init 向默认 translator registry 注册 OpenAI ↔ Inception 双向转换。
// 调用方：executor.Execute/ExecuteStream 内主动用这些转换把 SDK 透传来的
// 原始 OpenAI body 翻译为 Inception body，再把 Inception SSE 翻译回 OpenAI 字节。
//
// 注册方向约定（见 registry.Register）：Register(from=OpenAI, to=Inception, reqT, respT)。
// 响应翻译查询反向取回：TranslateNonStream(ctx, from=Inception, to=OpenAI, ...)。
func init() {
	sdktr.Register(
		sdktr.FormatOpenAI,
		FormatInception,
		requestOpenAIToInception,
		sdktr.ResponseTransform{
			Stream:    streamInceptionToOpenAI,
			NonStream: nonStreamInceptionToOpenAI,
		},
	)
}

// ─── OpenAI 请求 → Inception 请求 ──────────────────────────────────────────

type openAIMessage struct {
	Role             string          `json:"role"`
	Content          json.RawMessage `json:"content,omitempty"`
	ReasoningContent json.RawMessage `json:"reasoning_content,omitempty"` // 思考内容，回传给上游不必要
}

type openAIRequest struct {
	Model           string          `json:"model"`
	Messages        []openAIMessage `json:"messages"`
	Stream          bool            `json:"stream"`
	ReasoningEffort string          `json:"reasoning_effort,omitempty"`
	ThinkingEffort  string          `json:"thinking_effort,omitempty"`
	Temperature     *float64        `json:"temperature,omitempty"`
	MaxTokens       *int            `json:"max_tokens,omitempty"`
	// 其余字段忽略（Inception 上游只识别 messages / id / thinking）。
}

type inceptionPart struct {
	Type string `json:"type"` // 恒为 "text"
	Text string `json:"text"`
}

type inceptionMessage struct {
	ID    string          `json:"id"`
	Role  string          `json:"role"`
	Parts []inceptionPart `json:"parts"`
}

type inceptionRequest struct {
	Messages []inceptionMessage `json:"messages"`
	ID       string             `json:"id"`
	Thinking string             `json:"thinking,omitempty"`
}

// requestOpenAIToInception 把 OpenAI Chat Completions 请求体翻译为 Inception
// AI SDK v5 UIMessage `parts` 格式。规则：
//   - 每条 message 合并 content（string 或 array of text parts）为单个 text part；
//   - role 透传 system/user/assistant；
//   - reasoning_effort / thinking_effort 归一为 thinking（low/medium/high），其余省略；
//   - 忽略 temperature/max_tokens 等被上游忽略的字段；
//   - 若 body 非法则返回 nil，由 executor 兜底。
func requestOpenAIToInception(model string, raw []byte, stream bool) []byte {
	if len(raw) == 0 {
		return nil
	}
	var req openAIRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil
	}

	out := inceptionRequest{
		ID:       "chat-" + uuid.NewString(),
		Messages: make([]inceptionMessage, 0, len(req.Messages)),
	}
	out.Thinking = normalizeThinking(req.ReasoningEffort, req.ThinkingEffort)

	for i, m := range req.Messages {
		role := strings.TrimSpace(m.Role)
		if role == "" {
			role = "user"
		}
		text, ok := contentToText(m.Content, m.ReasoningContent)
		if !ok {
			continue // 跳过无文本内容的消息（如纯图片/工具消息），避免上游 400
		}
		out.Messages = append(out.Messages, inceptionMessage{
			ID:   msgID(i),
			Role: role,
			Parts: []inceptionPart{
				{Type: "text", Text: text},
			},
		})
	}

	b, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	return b
}

// contentToText 把 OpenAI content（string 或 parts 数组或 content_text 抽象）
// 合并为单段纯文本。reasoning 字段在 OpenAI assistant 回放里偶有内容，拼接进 user 回放。
func contentToText(content, reasoning json.RawMessage) (string, bool) {
	var parts []string
	if t := extractText(content); t != "" {
		parts = append(parts, t)
	}
	if t := extractText(reasoning); t != "" {
		parts = append(parts, t)
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, "\n"), true
}

// extractText 处理 OpenAI content 的几种合法形态：
//   - string：直接返回；
//   - 数组：遍历取 type=="text" 的 text 字段、type=="input_text"/"output_text" 的 text，其余忽略；
//   - null/缺失：返回 ""。
func extractText(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	// 尝试 string
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	// 尝试数组
	var arr []map[string]json.RawMessage
	if json.Unmarshal(raw, &arr) == nil {
		var b strings.Builder
		for _, item := range arr {
			typ, _ := jsonStringValue(item["type"])
			txt, _ := jsonStringValue(item["text"])
			if txt == "" {
				continue
			}
			switch typ {
			case "text", "input_text", "output_text":
				b.WriteString(txt)
				if !strings.HasSuffix(txt, "\n") {
					b.WriteString("\n")
				}
			}
		}
		return strings.TrimSpace(b.String())
	}
	return ""
}

// jsonStringValue 从 RawMessage 解析出 string（字段可能是字符串字面量）。
func jsonStringValue(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, true
	}
	return "", false
}

// normalizeThinking 把 OpenAI 的 reasoning_effort / thinking_effort 归一为
// Inception 接受的 "low"/"medium"/"high"。minimal→low；未知值省略字段（等价不传 thinking）。
func normalizeThinking(efforts ...string) string {
	for _, e := range efforts {
		e = strings.ToLower(strings.TrimSpace(e))
		switch e {
		case "low", "medium", "high":
			return e
		case "minimal":
			return "low"
		}
	}
	return ""
}

func msgID(idx int) string {
	return "msg-" + uuid.NewString()
}

// ─── Inception SSE → OpenAI 响应 ──────────────────────────────────────────

type openAIChoice struct {
	Index        int            `json:"index"`
	Message      openAIMessage2 `json:"message"`
	FinishReason string         `json:"finish_reason"`
}

type openAIMessage2 struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIPlayerResponse struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []openAIChoice `json:"choices"`
	Usage   openAIUsage    `json:"usage"`
}

type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type openAIChunkChoice struct {
	Index        int         `json:"index"`
	Delta        openAIDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

type openAIDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type openAIChunk struct {
	ID      string              `json:"id"`
	Object  string              `json:"object"`
	Created int64               `json:"created"`
	Model   string              `json:"model"`
	Choices []openAIChunkChoice `json:"choices"`
}

// nowUnix 返回当前 Unix 秒。transform 测试不便注入时间，统一取 time.Now。
func nowUnix() int64 { return time.Now().Unix() }

// nonStreamInceptionToOpenAI 把 Inception 全量 SSE 文本翻译为单个 OpenAI
// chat.completion JSON。解析所有 text-delta 顺序拼接为 content。
func nonStreamInceptionToOpenAI(ctx context.Context, model string, originalReq, translatedReq, raw []byte, param *any) []byte {
	deltas := parseDeltas(string(raw))
	content := strings.Join(deltas, "")
	resp := openAIPlayerResponse{
		ID:      "chatcmpl-" + uuid.NewString(),
		Object:  "chat.completion",
		Created: nowUnix(),
		Model:   model,
		Choices: []openAIChoice{{
			Index:        0,
			Message:      openAIMessage2{Role: "assistant", Content: content},
			FinishReason: "stop",
		}},
		Usage: openAIUsage{},
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return nil
	}
	return b
}

// openAIStreamChunker 负责把 Inception 逐行 text-delta 增量封装为 OpenAI
// chat.completion.chunk JSON 字节。流式 executor 与全量 streamInceptionToOpenAI
// 共用同一封装逻辑，保证流式/全量输出逐字节一致。
//
// 一个 chunker 实例对应一次流式响应：id 与 created 在整个流内恒定（符合 OpenAI 语义）。
type openAIStreamChunker struct {
	id      string
	created int64
	model   string
}

// newOpenAIStreamChunker 构造流式封装器，生成贯穿整流的 id 与 created。
func newOpenAIStreamChunker(model string) *openAIStreamChunker {
	return &openAIStreamChunker{
		id:      "chatcmpl-" + uuid.NewString(),
		created: nowUnix(),
		model:   model,
	}
}

// row 构造单个 chunk 字节。finish 传 nil 表示无 finish_reason。
func (c *openAIStreamChunker) row(delta openAIDelta, finish *string) []byte {
	ch := openAIChunk{
		ID:      c.id,
		Object:  "chat.completion.chunk",
		Created: c.created,
		Model:   c.model,
		Choices: []openAIChunkChoice{{Index: 0, Delta: delta, FinishReason: finish}},
	}
	b, err := json.Marshal(ch)
	if err != nil {
		return nil
	}
	return b
}

// RoleChunk 返回流首帧（delta.role="assistant"），符合 OpenAI 流式惯例。
func (c *openAIStreamChunker) RoleChunk() []byte {
	return c.row(openAIDelta{Role: "assistant"}, nil)
}

// ContentChunk 返回一个 text-delta 对应的 content 帧。
// delta 为空时返回 nil（由调用方跳过，避免发空 content 帧）。
func (c *openAIStreamChunker) ContentChunk(delta string) []byte {
	if delta == "" {
		return nil
	}
	return c.row(openAIDelta{Content: delta}, nil)
}

// StopChunk 返回流收尾帧（空 delta + finish_reason=stop）。
func (c *openAIStreamChunker) StopChunk() []byte {
	stop := "stop"
	return c.row(openAIDelta{}, &stop)
}

// streamInceptionToOpenAI 把 Inception 全量 SSE 翻译为多个 OpenAI
// chat.completion.chunk 行。每个 text-delta 产一个 chunk；结束追加收尾帧。
// SDK handler 会以 "data: %s\n\n" 包裹每个 [][]byte 元素发往客户端，
// 并在流结束后自动追加 data: [DONE]（openai_handlers.go），这里不手动追加。
func streamInceptionToOpenAI(ctx context.Context, model string, originalReq, translatedReq, raw []byte, param *any) [][]byte {
	c := newOpenAIStreamChunker(model)
	deltas := parseDeltas(string(raw))

	chunks := make([][]byte, 0, len(deltas)+2)
	if first := c.RoleChunk(); first != nil {
		chunks = append(chunks, first)
	}
	for _, d := range deltas {
		if b := c.ContentChunk(d); b != nil {
			chunks = append(chunks, b)
		}
	}
	if b := c.StopChunk(); b != nil {
		chunks = append(chunks, b)
	}
	return chunks
}
