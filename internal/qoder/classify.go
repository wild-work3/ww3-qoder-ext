package qoder

import (
	"context"
	"errors"
	"strings"

	"wild-work3/qoder-ext/internal/protocol/errkind"
)

// Error 携带上游错误的 errkind 分类，跨 IPC 时由 ErrKindOf 取出 Kind。
type Error struct {
	Kind       errkind.Kind
	Msg        string
	RetryAfter int64 // 秒；仅 Queued 等可重试错误有意义（上游 Retry-After / retryAfterSeconds）
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Msg == "" {
		return string(e.Kind)
	}
	return e.Msg
}

// RetryAfterOf 从 error 中提取上游建议的重试等待（秒）；非 *Error 或未给时返回 0。
func RetryAfterOf(err error) int64 {
	var e *Error
	if errors.As(err, &e) {
		return e.RetryAfter
	}
	return 0
}

// ErrKindOf 从任意 error 提取统一错误分类。
func ErrKindOf(err error) errkind.Kind {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) && e.Kind != "" {
		return e.Kind
	}
	switch {
	case errors.Is(err, context.Canceled):
		return errkind.Cancel
	case errors.Is(err, context.DeadlineExceeded):
		return errkind.IdleTimeout
	}
	return errkind.Server
}

var (
	hardCreditMarkers = []string{
		"insufficient credit", "no credit", "credit exhausted", "out of credit",
		"credit not enough", "not enough credit", "payment required",
		"积分不足", "额度不足", "余额不足", "积分用完", "额度用尽", "没有积分",
	}
	quotaMarkers = []string{
		"quota exceeded", "quota exhaust", "isquotaexceeded", "pricingurl",
	}
	softRateMarkers = []string{
		"rate limit", "too many requests", "usage limit", "请求过于频繁",
	}
	resourceLimitMarkers  = []string{"resource_limit", "resource limit"}
	contentBlockedMarkers = []string{
		"blocked by security policy", "unapproved channel", "illegal api invocation",
	}
	promptTooLongMarkers = []string{"prompt is too long", "11115"}
	accountFaultMarkers  = []string{"request illegal", "trial not activated"}
	queuedMarkers        = []string{`"isqueued":true`, `"is_queued":true`, `"isqueued": true`}
)

func containsAny(low string, markers []string) bool {
	for _, m := range markers {
		if strings.Contains(low, m) {
			return true
		}
	}
	return false
}

// hasCode 判断 JSON body 内是否出现指定整数 code（`"code":110` / `"code":"110"` 等）。
func hasCode(low string, codes ...string) bool {
	for _, key := range []string{`"code"`, `"error_code"`, `"errorcode"`} {
		idx := 0
		for {
			i := strings.Index(low[idx:], key)
			if i < 0 {
				break
			}
			rest := low[idx+i+len(key):]
			j := 0
			for j < len(rest) && (rest[j] == ':' || rest[j] == ' ' || rest[j] == '\t' || rest[j] == '"') {
				j++
			}
			k := j
			for k < len(rest) && rest[k] >= '0' && rest[k] <= '9' {
				k++
			}
			if k > j {
				got := rest[j:k]
				for _, c := range codes {
					if got == c {
						return true
					}
				}
			}
			idx += i + len(key)
		}
	}
	return false
}

// isQueued 判断错误体是否表示上游排队。
func isQueued(body string) bool {
	low := strings.ToLower(body)
	return hasCode(low, "10605") || containsAny(low, queuedMarkers)
}

// Classify 把上游 HTTP 状态与响应体映射为统一错误分类。
//
// 顺序与旧版一致：402/401/10605/429 优先，其次额度类，再兜底状态码区间。
func Classify(status int, body string) errkind.Kind {
	low := strings.ToLower(body)
	switch {
	case status == 402:
		return errkind.HardCredit
	case status == 401 || strings.Contains(low, "token_expire"):
		return errkind.SessionDead
	case isQueued(low):
		return errkind.Queued
	case status == 429:
		return errkind.SoftRate
	case hasCode(low, "110", "112") || containsAny(low, quotaMarkers):
		return errkind.HardCredit
	case containsAny(low, resourceLimitMarkers):
		return errkind.SoftRate
	case containsAny(low, hardCreditMarkers):
		return errkind.HardCredit
	case containsAny(low, softRateMarkers):
		return errkind.SoftRate
	case status == 404:
		return errkind.NotFound
	case status >= 500:
		return errkind.Server
	case status >= 400:
		switch {
		case containsAny(low, contentBlockedMarkers):
			return errkind.ContentBlocked
		case (status == 400 || status == 404) && containsAny(low, promptTooLongMarkers):
			return errkind.PromptTooLong
		case status == 403 && strings.TrimSpace(body) == "":
			return errkind.WafBlock
		case containsAny(low, accountFaultMarkers):
			return errkind.AccountFault
		}
		return errkind.Client
	}
	return errkind.Client
}
