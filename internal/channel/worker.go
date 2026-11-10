package channel

import (
	"context"
	"encoding/json"

	"wild-work3/qoder-ext/internal/protocol/chat"
	"wild-work3/qoder-ext/internal/protocol/errkind"
	"wild-work3/qoder-ext/internal/protocol/plugin"
	"wild-work3/qoder-ext/internal/qoder"
)

// generate 处理 chat.generate 流式请求。
//
// 关键隔离：请求模型前缀必须与当前渠道实例的 region 一致；不一致直接
// ModelBlocked，绝不静默用错区账号执行。
func (d *Daemon) generate(ctx context.Context, p Raw, emit plugin.Emit) (Raw, *Err) {
	var req chat.GenerateRequest
	if err := json.Unmarshal(p, &req); err != nil {
		return nil, errf(errkind.BadParams, errkind.ScopeRequest, "请求体解析失败: %v", err)
	}

	cfg := parseConfig(p)
	// 会话面（chat.generate）的渠道配置内嵌在请求体的 channel_config 里，
	// 控制面才用 params.config；两者必须合并，否则 region/base_url 丢失，
	// 会拿默认上游基址去请求真实站点（联调与跨区校验都会失效）。
	cfg = mergeChannelConfig(cfg, req)
	if cfg.Region == "" {
		cfg.Region = qoder.NormalizeRegion(req.Model)
	}
	if pfx := qoder.ModelPrefixOf(req.Model); pfx != "" && pfx != cfg.regionPrefix() {
		return nil, errf(errkind.ModelBlocked, errkind.ScopeModel,
			"模型 %s 属于 %s 区，当前渠道实例是 %s 区，请把模型路由到对应渠道",
			req.Model, pfx, cfg.regionPrefix())
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 300
	}

	acct, c, err := d.account(req.AccountUID, cfg)
	if err != nil {
		k := qoder.ErrKindOf(err)
		return nil, errfQ(k, scopeOfKind(k), qoder.RetryAfterOf(err), "%v", err)
	}

	Log.Infof("chat.generate uid=%s region=%s model=%s msgs=%d tools=%d",
		acct.UID, cfg.regionPrefix(), req.Model, len(req.Messages), len(req.Tools))

	u, err := c.Stream(ctx, acct, &req, qoder.Emitter(func(event string, payload map[string]any) error {
		return emit(event, payload)
	}))
	if err != nil {
		if ctx.Err() != nil {
			return nil, errf(errkind.Cancel, errkind.ScopeRequest, "请求已取消")
		}
		k := qoder.ErrKindOf(err)
		d.recordChat(acct, req.Model, u, false, string(k))
		return nil, errfQ(k, scopeOfKind(k), qoder.RetryAfterOf(err), "%v", err)
	}
	if u == nil {
		u = &chat.Usage{}
	}
	d.recordChat(acct, req.Model, u, true, "")
	return mustJSON(u), nil
}

// scopeOfKind 把错误种类映射到作用域。
func scopeOfKind(k errkind.Kind) errkind.Scope {
	switch k {
	case errkind.HardCredit, errkind.SoftRate, errkind.SessionDead, errkind.Auth, errkind.AccountFault:
		return errkind.ScopeAccount
	case errkind.NotFound, errkind.ModelBlocked, errkind.PromptTooLong:
		return errkind.ScopeModel
	case errkind.Server, errkind.WafBlock, errkind.Conn:
		return errkind.ScopeChannel
	default:
		return errkind.ScopeRequest
	}
}

// recordChat 写一条统计；未上报 token 记 -1（不写 0）。
func (d *Daemon) recordChat(acct *qoder.Auth, model string, u *chat.Usage, ok bool, kind string) {
	if d.stat == nil {
		return
	}
	r := statRecord(model, ok, kind)
	if acct != nil {
		r.Account = qoder.AuthFileName(acct)
		r.Region = acct.RegionOf()
	}
	if u != nil {
		if u.PromptTokens > 0 {
			r.PromptTokens = u.PromptTokens
		}
		if u.CompletionTokens > 0 {
			r.CompletionTokens = u.CompletionTokens
		}
		if u.TotalTokens > 0 {
			r.TotalTokens = u.TotalTokens
		} else if u.PromptTokens > 0 || u.CompletionTokens > 0 {
			r.TotalTokens = u.PromptTokens + u.CompletionTokens
		}
		r.TimeMs = int(u.TimeMs)
		r.FirstTokenMs = int(u.FirstTokenMs)
		if u.Cache != nil {
			r.CachedTokens = u.Cache.Cached
			r.CachedReported = u.Cache.CachedReported || u.Cache.WriteReported
		}
	}
	d.stat.Append(r)
}
