package inception

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
)

// inception 事件 schema（来自 /api/chat 的 SSE，AI SDK v5 UIMessage stream 协议）。
// 只解析回答生成相关字段，reasoning/filename 等事件忽略。
type sseEvent struct {
	Type string `json:"type"`
	// text-delta 的事件携带 delta 字符串
	Delta string `json:"delta"`
	// 仅在调试或扩展时使用
	ID string `json:"id"`
	// tool/file/finish 等事件可能用到（v1 暂不处理）
	FinishReason string `json:"finishReason"`
}

// parseDeltas 解析 Inception SSE 文本，返回所有 text-delta 的 delta 字符串，
// 按 SSE 到达顺序拼接即为完整 assistant 回答。
// reasoning-* / text-start / text-end 等事件被忽略。
// 遇到 [DONE] 结束扫描（但其后若有残留 delta 也忽略）。
func parseDeltas(raw string) []string {
	var deltas []string
	sc := bufio.NewScanner(strings.NewReader(raw))
	// Inception 的 text-delta 可能较长，放大缓冲
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		// SSE 行形如 "data: {json}" 或 "data: [DONE]"
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var ev sseEvent
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			// 非 JSON 行（如注释/事件名）跳过
			continue
		}
		if ev.Type == "text-delta" {
			deltas = append(deltas, ev.Delta)
		}
	}
	return deltas
}

// parseFinished 判断 SSE 流是否包含 [DONE] 终止标记。
func parseFinished(raw string) bool {
	return strings.Contains(raw, "data: [DONE]") || strings.Contains(raw, "[DONE]")
}

// looksLikeChallenge 判断响应体是否是 Vercel Bot Challenge 拦截页。
func looksLikeChallenge(body string) bool {
	return strings.Contains(body, "Vercel Security Checkpoint") ||
		strings.Contains(body, "<title>Vercel Security Checkpoint</title>")
}

// jsonCompact 返回 JSON 的紧凑字节表示（用于调试/比较），失败时返回原值拷贝。
func jsonCompact(v []byte) []byte {
	var buf bytes.Buffer
	if json.Compact(&buf, v) == nil {
		return buf.Bytes()
	}
	return append([]byte(nil), v...)
}
