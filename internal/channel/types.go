// Package channel 实现 qoder-ext 渠道插件的 daemon/worker 层。
package channel

import (
	"encoding/json"

	"wild-work3/qoder-ext/internal/protocol/errkind"
	"wild-work3/qoder-ext/internal/protocol/jsonl"
)

// Raw 是协议层原始 JSON。
type Raw = jsonl.RawMessage

// Err 是协议层错误对象。
type Err = jsonl.ErrObj

// errf 构造协议错误对象。
func errf(kind errkind.Kind, scope errkind.Scope, format string, a ...any) *Err {
	return jsonl.Errorf(string(kind), string(scope), format, a...)
}

// errfQ 在 errf 基础上透传上游建议的重试等待（排队/限流类，qoder.Error.RetryAfter 秒）。
// core 侧经 rpc.RetryAfterOf 读出后既用于调度冷却时长，也随错误透传给下游客户端退避
// （旧版 Queued.RetryAfter 语义）。
func errfQ(kind errkind.Kind, scope errkind.Scope, retryAfterSec int64, format string, a ...any) *Err {
	e := errf(kind, scope, format, a...)
	if retryAfterSec > 0 {
		e.RetryAfterMs = retryAfterSec * 1000 // 插件内以秒解析（Retry-After/retryAfterSeconds），IPC 契约是毫秒
	}
	return e
}

// mustJSON 序列化 v；失败即 panic（内部不变量，不应发生）。
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
