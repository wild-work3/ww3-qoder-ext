package qoder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"wild-work3/qoder-ext/internal/protocol/chat"
	"wild-work3/qoder-ext/internal/protocol/errkind"
)

// Emitter 是流式事件的发射回调（对应 plugin.Emit）。
type Emitter func(event string, payload map[string]any) error

// ChatStream 发 chat 请求并返回原始嵌套 SSE body 流（调用方负责 Close）。
//
// 非 2xx 时 rc 为 nil、respBody 为上游响应体、err 为 nil；只有传输层失败才返回 err。
// 这个「错误也是正常返回」的形状是刻意的：交给上层 Classify 统一分类。
func (c *Client) ChatStream(a *Auth, req *chat.GenerateRequest) (rc io.ReadCloser, status int, respBody []byte, err error) {
	// 本地模型校验：上游对未知模型会静默回落默认模型并返回 200，必须本地拒绝。
	// 但尚未刷新过模型列表且本区无静态表时不误杀（避免全拒）。
	if !c.KnownModel(req.Model) {
		return nil, 400, []byte(`{"error":{"message":"model_not_found","code":"model_not_found"}}`),
			&Error{Kind: errkind.NotFound, Msg: "未知模型: " + UpstreamModelName(req.Model)}
	}
	modelKey := c.modelKey(UpstreamModelName(req.Model))
	if modelKey == "" {
		modelKey = UpstreamModelName(req.Model)
	}
	entry := c.modelEntry(modelKey) // 上游真值 format/source；未命中为 nil

	// 思考开关：reasoning_effort 或 thinking 非 nil/false → 启用推理。
	enableReasoning := false
	reasoningEffort := ""
	if req.ReasoningEffort != "" {
		enableReasoning = true
		reasoningEffort = req.ReasoningEffort
	} else if req.Thinking {
		enableReasoning = true
	}

	contextWindow := resolveContextWindow(0, entry)

	msgs := make([]map[string]any, 0, len(req.Messages))
	raw, _ := json.Marshal(req.Messages)
	_ = json.Unmarshal(raw, &msgs)

	var tools []any
	if len(req.Tools) > 0 {
		tb, _ := json.Marshal(req.Tools)
		_ = json.Unmarshal(tb, &tools)
	}

	rawBody, err := buildAgentBody(c.Endpoints, msgs, modelKey, entry, tools, enableReasoning, reasoningEffort, contextWindow)
	if err != nil {
		return nil, 0, nil, &Error{Kind: errkind.BadParams, Msg: "build qoder body: " + err.Error()}
	}
	encoded := qoderEncode(rawBody)
	url := c.gateway() + EpChat
	httpReq, err := http.NewRequest(http.MethodPost, url, strings.NewReader(encoded))
	if err != nil {
		return nil, 0, nil, err
	}
	sess, err := NewCosySession(c.Endpoints, a.MachineID, a.MachineToken, a.MachineType, a.Nickname, a.UID, a.AccessToken, a.RefreshToken, "")
	if err != nil {
		return nil, 0, nil, &Error{Kind: errkind.Server, Msg: "cosy session: " + err.Error()}
	}
	if err := sess.ApplyHeaders(httpReq, c.Endpoints, encoded, url, a.UID, "text/event-stream", true, modelKey); err != nil {
		return nil, 0, nil, &Error{Kind: errkind.Server, Msg: "cosy headers: " + err.Error()}
	}

	resp, err := c.streamHTTP().Do(httpReq)
	if err != nil {
		return nil, 0, nil, &Error{Kind: errkind.Conn, Msg: err.Error()}
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		return nil, resp.StatusCode, body, nil
	}
	return NewIdleReader(resp.Body, c.IdleTimeout), resp.StatusCode, nil, nil
}

// Stream 走真实上游流式对话，把增量翻译成协议事件并 emit。
// 返回最终用量；错误一律用 *Error 携带 errkind。
//
// 注意：本实现不伪造 "[DONE]"（完成语义由 finish_reason/usage/completed 表达），
// 也不在截断后合成假成功。
func (c *Client) Stream(ctx context.Context, a *Auth, req *chat.GenerateRequest, emit Emitter) (*chat.Usage, error) {
	if a == nil {
		return nil, &Error{Kind: errkind.SessionDead, Msg: "缺少凭据"}
	}
	if req == nil || strings.TrimSpace(req.Model) == "" {
		return nil, &Error{Kind: errkind.BadParams, Msg: "缺少 model"}
	}
	if len(req.Messages) == 0 {
		return nil, &Error{Kind: errkind.BadParams, Msg: "缺少 messages"}
	}

	rc, status, respBody, err := c.ChatStream(a, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, &Error{Kind: errkind.Cancel, Msg: "已取消"}
		}
		var e *Error
		if errors.As(err, &e) {
			return nil, e
		}
		return nil, &Error{Kind: errkind.Conn, Msg: "请求上游失败: " + err.Error()}
	}

	// 注意：非 2xx 时 rc 为 nil（错误体已在 ChatStream 里读走）。
	// 必须先判 nil 再 defer Close，否则 defer rc.Close() 会空指针 panic。
	if rc == nil {
		return nil, &Error{
			Kind: Classify(status, string(respBody)),
			Msg:  fmt.Sprintf("上游 %d: %s", status, truncate(string(respBody), 300)),
		}
	}
	defer rc.Close()

	if status >= 300 {
		return nil, &Error{
			Kind: Classify(status, string(respBody)),
			Msg:  fmt.Sprintf("上游 %d: %s", status, truncate(string(respBody), 300)),
		}
	}

	// 取消时关闭响应体，打断阻塞的读取。
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = rc.Close()
		case <-stop:
		}
	}()

	start := time.Now()
	usage := &chat.Usage{}
	var (
		firstTokenAt time.Time
		validEvents  int
		finish       string
		truncated    bool
	)

	onChunk := func(chunk map[string]any) error {
		if e, ok := chunk["error"].(map[string]any); ok && e != nil {
			msg := str(e["message"])
			if msg == "" {
				msg = str(e["code"])
			}
			return &Error{Kind: Classify(200, msg), Msg: "上游错误: " + orDefault(msg, str(e["code"]))}
		}
		validEvents++
		if us, ok := chunk["usage"].(map[string]any); ok {
			applyUsage(usage, us)
		}
		choices, _ := chunk["choices"].([]any)
		for _, cv := range choices {
			ch, _ := cv.(map[string]any)
			if ch == nil {
				continue
			}
			if fr := str(ch["finish_reason"]); fr != "" {
				finish = fr
			}
			delta, _ := ch["delta"].(map[string]any)
			if delta == nil {
				continue
			}
			if r := reasoningDeltaOf(delta); r != "" && emit != nil {
				if err := emit("reasoning.delta", map[string]any{"content": r}); err != nil {
					return err
				}
			}
			if content, ok := delta["content"].(string); ok && content != "" {
				if firstTokenAt.IsZero() {
					firstTokenAt = time.Now()
				}
				if emit != nil {
					if err := emit("text.delta", map[string]any{"content": content}); err != nil {
						return err
					}
				}
			}
			if tcs, ok := delta["tool_calls"].([]any); ok {
				if err := emitToolCalls(tcs, emit); err != nil {
					return err
				}
			}
		}
		return nil
	}

	parseErr := ParseNestedSSE(rc, onChunk, &truncated)
	usage.TimeMs = time.Since(start).Milliseconds()
	if !firstTokenAt.IsZero() {
		usage.FirstTokenMs = firstTokenAt.Sub(start).Milliseconds()
	}

	if parseErr != nil {
		if ctx.Err() != nil {
			return nil, &Error{Kind: errkind.Cancel, Msg: "已取消"}
		}
		var e *Error
		if errors.As(parseErr, &e) {
			return nil, e
		}
		return nil, &Error{Kind: errkind.Conn, Msg: "读取上游流失败: " + parseErr.Error()}
	}
	if truncated {
		return nil, &Error{Kind: errkind.Truncated, Msg: "上游流已中断（响应不完整）"}
	}
	if validEvents == 0 {
		return nil, &Error{Kind: errkind.Server, Msg: "上游返回空流（未收到任何有效事件）"}
	}

	if emit != nil {
		if err := emit("finish_reason", map[string]any{"reason": finish}); err != nil {
			return nil, err
		}
		if err := emit("usage", usageFields(usage)); err != nil {
			return nil, err
		}
		if err := emit("completed", map[string]any{"finish_reason": finish}); err != nil {
			return nil, err
		}
	}
	return usage, nil
}

// applyUsage 把上游 usage 增量合并进最终用量（末帧覆盖）。
func applyUsage(u *chat.Usage, m map[string]any) {
	if u == nil || m == nil {
		return
	}
	if v, ok := m["prompt_tokens"]; ok {
		u.PromptTokens = intOf(v)
	}
	if v, ok := m["completion_tokens"]; ok {
		u.CompletionTokens = intOf(v)
	}
	if v, ok := m["total_tokens"]; ok {
		u.TotalTokens = intOf(v)
	}
	cached, cachedReported := 0, false
	for _, k := range []string{"prompt_cache_hit_tokens", "cache_read_input_tokens"} {
		if v, ok := m[k]; ok {
			cached, cachedReported = intOf(v), true
			break
		}
	}
	write, writeReported := 0, false
	if d, ok := m["prompt_tokens_details"].(map[string]any); ok {
		if v, ok := d["cached_tokens"]; ok && !cachedReported {
			cached, cachedReported = intOf(v), true
		}
		if v, ok := d["cache_write_tokens"]; ok {
			write, writeReported = intOf(v), true
		}
	}
	if cachedReported || writeReported {
		u.Cache = &chat.CacheUsage{
			Cached:         cached,
			CachedReported: cachedReported,
			Write:          write,
			WriteReported:  writeReported,
		}
	}
	markReported(u)
}

func intOf(v any) int {
	switch n := v.(type) {
	case float64:
		if n < 0 {
			return 0
		}
		return int(n)
	case int:
		if n < 0 {
			return 0
		}
		return n
	case json.Number:
		i, err := n.Int64()
		if err != nil || i < 0 {
			return 0
		}
		return int(i)
	}
	return 0
}

// reasoningDeltaOf 取出思维链增量，兼容多种上游字段命名。
func reasoningDeltaOf(d map[string]any) string {
	if s, ok := d["reasoning_content"].(string); ok && s != "" {
		return s
	}
	if s, ok := d["reasoning"].(string); ok && s != "" {
		return s
	}
	if rd, ok := d["reasoning_details"].([]any); ok {
		var sb strings.Builder
		for _, item := range rd {
			m, _ := item.(map[string]any)
			if m == nil {
				continue
			}
			if s, ok := m["text"].(string); ok && s != "" {
				sb.WriteString(s)
			}
		}
		if sb.Len() > 0 {
			return sb.String()
		}
	}
	return ""
}

// emitToolCalls 把一次增量里的工具调用分片发射成 tool_call.delta。
func emitToolCalls(tcs []any, emit Emitter) error {
	if emit == nil {
		return nil
	}
	for i, tcv := range tcs {
		tc, _ := tcv.(map[string]any)
		if tc == nil {
			continue
		}
		idx := i
		if v, ok := tc["index"].(float64); ok {
			idx = int(v)
		}
		p := map[string]any{"index": idx}
		if s := str(tc["id"]); s != "" {
			p["id"] = s
		}
		if s := str(tc["type"]); s != "" {
			p["type"] = s
		}
		if fn, ok := tc["function"].(map[string]any); ok {
			if s := str(fn["name"]); s != "" {
				p["name"] = s
			}
			if v, ok := fn["arguments"]; ok {
				if s := fmt.Sprint(v); s != "" {
					p["arguments_fragment"] = s
				}
			}
		}
		if err := emit("tool_call.delta", p); err != nil {
			return err
		}
	}
	return nil
}

// usageFields 把 usage 摊平成事件 payload。
func usageFields(u *chat.Usage) map[string]any {
	if u == nil {
		return map[string]any{}
	}
	// 三维 token 的来源标志（W3-34）：值来自上游同一 usage 对象，明确 0 也是有效实报。
	// 不带标志时 core 只能把「全 0」当未知处理（旧副本兼容路径），无法区分明确 0。
	m := map[string]any{
		"prompt_tokens":       u.PromptTokens,
		"completion_tokens":   u.CompletionTokens,
		"total_tokens":        u.TotalTokens,
		"prompt_reported":     u.PromptReported,
		"completion_reported": u.CompletionReported,
		"total_reported":      u.TotalReported,
	}
	if u.TimeMs > 0 {
		m["time_ms"] = u.TimeMs
	}
	if u.FirstTokenMs > 0 {
		m["first_token_ms"] = u.FirstTokenMs
	}
	if u.Cache != nil {
		m["cache"] = map[string]any{
			"cached":          u.Cache.Cached,
			"cached_reported": u.Cache.CachedReported,
			"write":           u.Cache.Write,
			"write_reported":  u.Cache.WriteReported,
		}
	}
	return m
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func orDefault(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

// markReported 标记三维 token 来自上游 usage 对象；本地推导出来的 total 不算实报。
func markReported(u *chat.Usage) {
	if u == nil {
		return
	}
	derived := u.TotalTokens == 0 && u.PromptTokens > 0 && u.CompletionTokens > 0
	if derived {
		u.TotalTokens = u.PromptTokens + u.CompletionTokens
	}
	u.MarkReported()
	if derived {
		u.TotalReported = false
	}
}
