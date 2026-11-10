package qoder

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"wild-work3/qoder-ext/internal/protocol/errkind"
)

// envelope 是 Qoder 系 SSE 的外层信封。
//
// body 是**字符串**（内层 JSON 文本），与 qwenwork 的 json.RawMessage 形状不同，
// 不能照抄那边的写法。
type envelope struct {
	Body            string              `json:"body"`
	Headers         map[string][]string `json:"headers"`
	StatusCodeValue int                 `json:"statusCodeValue"`
	StatusCode      string              `json:"statusCode"`
}

// errText 提取上游错误文案；非错误信封返回空串。
// body 形如 {"code":"403","message":"<再嵌一层的 JSON 文本>"}，文案取 message 原文。
func (e *envelope) errText() string {
	if e.StatusCodeValue < 400 {
		return ""
	}
	var withMsg struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(e.Body), &withMsg) == nil && withMsg.Message != "" {
		return withMsg.Message
	}
	return truncate(e.Body, 300)
}

// retryAfterRe 从错误文案里取建议退避秒数。上游同处既给 retryAfterSeconds 也给
// waitTime，取前者、回落后者；只匹配十进制整数。
//
// 必须容忍键名两侧的反斜杠：该字段在原始信封 body 里被逐层转义，解出一层后仍带反斜杠。
var (
	retryAfterRe = regexp.MustCompile(`\\*"retryAfterSeconds\\*"\s*:\s*(\d+)`)
	waitTimeRe   = regexp.MustCompile(`\\*"waitTime\\*"\s*:\s*(\d+)`)
)

// maxRetryAfter 退避时长上限：超了按未声明处理。
const maxRetryAfter = 3600

func parseRetryAfter(s string) int {
	for _, re := range []*regexp.Regexp{retryAfterRe, waitTimeRe} {
		if m := re.FindStringSubmatch(s); m != nil {
			var n int
			if _, err := fmt.Sscanf(m[1], "%d", &n); err == nil && n > 0 && n <= maxRetryAfter {
				return n
			}
		}
	}
	return 0
}

// queuedNotice 识别「模型排队中」帧：外层 headers 带 Retry-After，
// 内层带 queueCount / retryAfterSeconds / waitTime。
func queuedNotice(vals map[string]any, headers map[string][]string) *Error {
	code, _ := vals["code"].(string)
	queued, _ := vals["isQueued"].(bool)
	if code != "10605" && !queued {
		return nil
	}
	e := &Error{Kind: errkind.Queued, Msg: "upstream model queued: " + truncate(fmt.Sprint(vals), 200)}
	if f, ok := vals["retryAfterSeconds"].(float64); ok && f > 0 {
		e.RetryAfter = int64(f)
	}
	if e.RetryAfter == 0 {
		for k, vs := range headers {
			if !strings.EqualFold(k, "Retry-After") || len(vs) == 0 {
				continue
			}
			if n, err := strconv.Atoi(strings.TrimSpace(vs[0])); err == nil && n > 0 {
				e.RetryAfter = int64(n)
			}
		}
	}
	return e
}

// quotaExhaustedCode 识别「账号额度/计费限制」帧，返回命中的 code（未命中返回 ""）。
func quotaExhaustedCode(vals map[string]any) string {
	if code, _ := vals["code"].(string); code == "110" || code == "112" {
		return code
	}
	if b, ok := vals["isQuotaExceeded"].(bool); ok && b {
		return "isQuotaExceeded"
	}
	if _, ok := vals["pricingUrl"]; ok {
		return "pricingUrl"
	}
	return ""
}

// envelopeNotice 识别「HTTP 200 里包着的业务错误帧」：这种帧没有 choices，
// 当普通 chunk 透传只会让客户端拿到空 delta 干等。返回 nil 表示「不是错误帧」。
//
//	10605 / isQueued        → Queued（模型级拥塞）
//	110 / 112 / pricingUrl  → HardCredit（账号级额度耗尽）
func envelopeNotice(body string, headers map[string][]string) *Error {
	if body == "" || strings.Contains(body, `"choices"`) {
		return nil
	}
	vals := qoderUnwrap(body)
	if q := queuedNotice(vals, headers); q != nil {
		return q
	}
	if code := quotaExhaustedCode(vals); code != "" {
		return &Error{
			Kind: errkind.HardCredit,
			Msg:  "upstream quota exhausted (code " + code + "): " + truncate(body, 200),
		}
	}
	return nil
}

// qoderUnwrap 把「message 字段里再套一层 JSON 字符串」逐层解开，收集各层键值
// （深层覆盖浅层）。上游三层套娃：{code:403} → {code:10605} → {...}。
func qoderUnwrap(body string) map[string]any {
	vals := map[string]any{}
	cur := body
	for depth := 0; depth < 6 && cur != ""; depth++ {
		var m map[string]any
		if json.Unmarshal([]byte(cur), &m) != nil {
			break
		}
		for k, v := range m {
			vals[k] = v
		}
		next, _ := m["message"].(string)
		if next == "" || next == cur {
			break
		}
		cur = next
	}
	return vals
}

// ParseNestedSSE 逐行解析嵌套 SSE 信封，把内层 chunk 交给 onChunk。
//
// 帧处理顺序（不可交换）：
//  1. envelopeNotice：排队 / 额度耗尽 / 业务错误帧——必须早于 onChunk，这类帧没有 choices；
//  2. StatusCodeValue>=400 → *Error（带 retryAfter）；
//  3. Body=="[DONE]" → 上游显式收尾（对客户端不可见）；
//  4. 其余解内层 chunk 交给 onChunk。
//
// truncated 在半帧 / 连接中断时置 true（截断优先于错误）。
func ParseNestedSSE(r io.Reader, onChunk func(map[string]any) error, truncated *bool) error {
	set := func() {
		if truncated != nil {
			*truncated = true
		}
	}
	br := bufio.NewReaderSize(r, 256*1024)
	for {
		line, err := br.ReadString('\n')
		if err != nil && err != io.EOF {
			set() // 连接中断 / 读超时：可证实的截断
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "data:") {
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			var env envelope
			outerOK := payload != "" && json.Unmarshal([]byte(payload), &env) == nil
			if outerOK && env.Body != "" {
				if e := envelopeNotice(env.Body, env.Headers); e != nil {
					return e
				}
				if env.StatusCodeValue >= 400 {
					msg := env.errText()
					if msg == "" {
						msg = truncate(env.Body, 300)
					}
					return &Error{
						Kind:       Classify(env.StatusCodeValue, msg),
						Msg:        fmt.Sprintf("upstream %d: %s", env.StatusCodeValue, msg),
						RetryAfter: int64(parseRetryAfter(env.Body + "\n" + msg)),
					}
				}
			}
			innerOK := false
			if outerOK && env.Body != "" {
				if env.Body == "[DONE]" {
					return nil // 上游显式收尾
				}
				var chunk map[string]any
				innerOK = json.Unmarshal([]byte(env.Body), &chunk) == nil
				if innerOK {
					if err := onChunk(chunk); err != nil {
						return err
					}
				}
			}
			if err == io.EOF && !(outerOK && innerOK) {
				set() // EOF 处这一行（内层或外层）不是完整 JSON：半个帧
			}
		}
		if err == io.EOF {
			if strings.TrimSpace(line) != "" {
				set()
			}
			return nil
		}
	}
}

// MergeToolCallDelta 把增量 tool_call 合并进已累积的调用（id/type/name 覆盖，arguments 拼接）。
func MergeToolCallDelta(dst map[string]any, delta map[string]any) {
	if dst == nil || delta == nil {
		return
	}
	if v, ok := delta["id"]; ok && fmt.Sprint(v) != "" {
		dst["id"] = v
	}
	if v, ok := delta["type"]; ok && fmt.Sprint(v) != "" {
		dst["type"] = v
	}
	dfn, _ := delta["function"].(map[string]any)
	if dfn == nil {
		return
	}
	fn, _ := dst["function"].(map[string]any)
	if fn == nil {
		fn = map[string]any{}
		dst["function"] = fn
	}
	if v, ok := dfn["name"]; ok && fmt.Sprint(v) != "" {
		fn["name"] = v
	}
	if v, ok := dfn["arguments"]; ok {
		fn["arguments"] = fmt.Sprint(fn["arguments"]) + fmt.Sprint(v)
	}
}

// retryAfterString 给日志用。
func retryAfterString(e *Error) string {
	if e == nil || e.RetryAfter <= 0 {
		return ""
	}
	return (time.Duration(e.RetryAfter) * time.Second).String()
}
