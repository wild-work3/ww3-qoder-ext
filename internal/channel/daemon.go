// Package channel 实现 qoder-ext 渠道插件（渠道 ID：qoder）。
//
// 单插件、多区：region 配置字段区分 qoder / qodercn / qodercom 三个变体。
//
// 数据目录（<data>）：
//
//	<data>/auths/qoder-<uid>.json     凭据（token 只落此处，永不过 IPC）
//	<data>/auths/flows/<flow_id>.json 登录流程状态（跨进程：begin/status）
//	<data>/logs/qoder.log             日志
//	<data>/stat/                      渠道自记统计
package channel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"wild-work3/qoder-ext/internal/protocol/authflow"

	"wild-work3/qoder-ext/internal/protocol/chat"
	"wild-work3/qoder-ext/internal/protocol/cli"
	"wild-work3/qoder-ext/internal/protocol/errkind"
	"wild-work3/qoder-ext/internal/protocol/meta"
	"wild-work3/qoder-ext/internal/protocol/plugin"
	"wild-work3/qoder-ext/internal/protocol/stat"
	"wild-work3/qoder-ext/internal/qoder"
)

// PluginID 是渠道标识（core 侧 channel_id）。
const PluginID = "qoder"

// Log 是插件的日志器；nil 时静默（CLI 早退场景）。
var Log *cli.Logger

// Config 是渠道配置（channel_config）。
type Config struct {
	Region  string `json:"region,omitempty"`
	Proxy   string `json:"proxy,omitempty"`
	Timeout int    `json:"timeout,omitempty"`
	BaseURL string `json:"base_url,omitempty"`
}

// regionPrefix 返回归一化后的 region（未知/空 → qoder）。
func (c Config) regionPrefix() string { return qoder.NormalizeRegion(c.Region) }

// Daemon 持有渠道的全部私有序状态。
type Daemon struct {
	dataDir string
	store   *qoder.Store
	login   *qoder.LoginManager
	stat    *stat.Store
}

// NewDaemon 构造渠道实例。
func NewDaemon(dataDir string) *Daemon {
	store := qoder.NewStore(dataDir)
	return &Daemon{
		dataDir: dataDir,
		store:   store,
		login:   qoder.NewLoginManager(dataDir, store),
		stat:    stat.Open(dataDir),
	}
}

// Serve 按执行形态跑协议循环（call=一次性；stdio=常连）。
func Serve(ctx context.Context, mode, dataDir string, in io.Reader, out io.Writer) error {
	r := registry(NewDaemon(dataDir))
	switch mode {
	case plugin.ModeCall:
		return r.ServeOnce(ctx, in, out)
	default:
		return r.ServeStdio(ctx, in, out)
	}
}

// Manifest 上报插件元数据与能力清单。
func Manifest() meta.Manifest {
	return meta.Manifest{
		ID:   PluginID,
		Name: "Qoder",
		DefaultChannels: []meta.DefaultChannel{
			{ID: "qoder", Name: "Qoder", Config: map[string]any{"region": "qoder"}},
			{ID: "qodercn", Name: "QoderCN", Config: map[string]any{"region": "qodercn"}},
			{ID: "qodercom", Name: "QoderCOM", Config: map[string]any{"region": "qodercom"}},
		},
		Version: "1.0.0",
		Kind:    "channel",
		Protocol: meta.ProtocolRng{
			Min: 1,
			Max: 1,
		},
		Execution: meta.Execution{
			Control:            plugin.ModeCall,
			Conversation:       plugin.ModeCall,
			MaxWorkers:         4,
			IdleTimeoutSeconds: 0,
		},
		Capabilities: meta.Capabilities{
			Control: []string{
				"models.list",
				"config.schema",
				"config.get",
				"config.set",
				"auth.methods",
				"auth.begin",
				"auth.status",
				"auth.finish",
				"auth.cancel",
				"accounts.list",
				"accounts.resource_detail",
				"accounts.refresh",
				"accounts.checkin",
				"credits.get",
				"stats.get",
				"health.check",
				"ping",
			},
			Conversation: []string{"chat.generate"},
		},
		Icon:        "qoder",
		Color:       "#7c3aed",
		Description: "Qoder 渠道插件：单插件多区（qoder / qodercn / qodercom），COSY 签名 + 嵌套 SSE。",
	}
}

// registry 装配方法表（只注册能力声明里有的方法）。
func registry(d *Daemon) *plugin.Registry {
	r := plugin.New()
	r.Lifecycle(Manifest())
	r.Handle("config.schema", func(ctx context.Context, _ Raw) (Raw, *Err) {
		return mustJSON(configSchema()), nil
	})
	r.Handle("config.get", func(ctx context.Context, p Raw) (Raw, *Err) {
		return mustJSON(map[string]any{"config": parseConfig(p)}), nil
	})
	r.Handle("config.set", func(ctx context.Context, p Raw) (Raw, *Err) {
		return mustJSON(map[string]any{"ok": true, "config": parseConfig(p)}), nil
	})
	r.Handle("auth.methods", d.authMethods)
	r.Handle("auth.begin", d.authBegin)
	r.Handle("auth.status", d.authStatus)
	r.Handle("auth.finish", d.authFinish)
	r.Handle("auth.cancel", d.authCancel)
	r.Handle("accounts.list", d.accountsList)
	r.Handle("accounts.resource_detail", d.resourceDetail)
	r.Handle("accounts.refresh", d.accountRefresh)
	r.Handle("accounts.checkin", d.accountCheckin)
	r.Handle("models.list", d.modelsList)
	r.Handle("credits.get", d.creditsGet)
	r.Handle("stats.get", d.statsGet)
	r.HandleStream("chat.generate", d.generate)
	return r
}

// ---- 登录（device_poll + PKCE，无回环、无粘贴）----

func (d *Daemon) authMethods(ctx context.Context, p Raw) (Raw, *Err) {
	return mustJSON(authflow.MethodsResult{
		Methods: []authflow.Method{{
			ID:          "device",
			Type:        authflow.TypeDevicePoll,
			Label:       "设备码登录",
			Description: "打开授权页并在浏览器完成授权，本插件自动轮询获取凭据。",
			Default:     true,
		}},
		Note: "登录成功后凭据保存在插件私有目录，不会经 IPC 传输。",
	}), nil
}

func (d *Daemon) authBegin(ctx context.Context, p Raw) (Raw, *Err) {
	var req authflow.BeginRequest
	if err := json.Unmarshal(p, &req); err != nil {
		return nil, errf(errkind.BadParams, errkind.ScopeRequest, "参数解析失败: %v", err)
	}
	region := qoder.NormalizeRegion(firstNonEmpty(req.Region, req.Values["region"]))
	f, err := d.login.Begin(region, parseConfig(p).BaseURL)
	if err != nil {
		k := qoder.ErrKindOf(err)
		return nil, errf(k, scopeOfKind(k), "发起登录失败: %v", err)
	}
	return mustJSON(authflow.BeginResult{
		FlowID:           f.ID,
		Type:             authflow.TypeDevicePoll,
		Method:           orDefault(req.Method, "device"),
		AuthURL:          f.AuthURL,
		State:            string(authflow.StateWaitingUser),
		Prompt:           "请在浏览器中完成授权，本插件会自动轮询。",
		CanPasteCallback: false,
		NextPollMS:       2000,
		ExpiresAt:        time.Now().Add(10 * time.Minute).Format(time.RFC3339),
	}), nil
}

func (d *Daemon) authStatus(ctx context.Context, p Raw) (Raw, *Err) {
	var req authflow.StatusRequest
	if err := json.Unmarshal(p, &req); err != nil {
		return nil, errf(errkind.BadParams, errkind.ScopeRequest, "参数解析失败: %v", err)
	}
	f, err := d.login.Flows().Load(req.FlowID)
	if err != nil {
		return mustJSON(authflow.StatusResult{
			State: authflow.StateFailed,
			Error: &authflow.Error{Kind: string(errkind.NotFound), Message: "登录流程不存在或已过期"},
		}), nil
	}
	res := authflow.StatusResult{State: authflow.State(f.State), Hint: f.Error}
	switch f.State {
	case qoder.FlowStarting, qoder.FlowWaitingUser:
		r, perr := d.login.PollOnce(ctx, f)
		if perr != nil {
			if errors.Is(perr, qoder.ErrPending) {
				res.State = authflow.StateWaitingUser
				res.NextPollMS = 2000
				return mustJSON(res), nil
			}
			k := qoder.ErrKindOf(perr)
			if k == errkind.Auth || k == errkind.SessionDead || k == errkind.BadParams {
				_ = d.login.Flows().Update(f.ID, func(ff *qoder.Flow) {
					ff.State = qoder.FlowFailed
					ff.Error = perr.Error()
				})
				return mustJSON(authflow.StatusResult{
					State: authflow.StateFailed,
					Error: &authflow.Error{Kind: string(k), Message: perr.Error()},
				}), nil
			}
			// 瞬时错误：保持等待，交由下一轮轮询重试。
			res.State = authflow.StateWaitingUser
			res.Hint = perr.Error()
			res.NextPollMS = 2000
			return mustJSON(res), nil
		}
		authFile, cerr := d.login.Complete(f, r)
		if cerr != nil {
			k := qoder.ErrKindOf(cerr)
			return mustJSON(authflow.StatusResult{
				State: authflow.StateFailed,
				Error: &authflow.Error{Kind: string(k), Message: cerr.Error()},
			}), nil
		}
		res.State = authflow.StateSuccess
		res.Account = &authflow.Account{
			UID:       r.UID,
			Nickname:  orDefault(r.Nickname, r.UID),
			ChannelID: PluginID,
			Region:    f.Region,
			LoginKind: string(authflow.TypeDevicePoll),
			AuthFile:  authFile,
		}
		if a, lerr := d.store.Load(authFile); lerr == nil {
			res.Account.UID = orDefault(a.UID, r.UID)
			res.Account.Nickname = orDefault(a.Nickname, res.Account.Nickname)
			res.Account.Region = a.RegionOf()
			res.Account.ExpiresAt = a.ExpiresAtText()
		}
	case qoder.FlowSuccess:
		res.State = authflow.StateSuccess
		if f.AuthFile != "" {
			a, lerr := d.store.Load(f.AuthFile)
			if lerr == nil {
				res.Account = &authflow.Account{
					UID:       orDefault(a.UID, f.UID),
					Nickname:  orDefault(a.Nickname, orDefault(f.Nickname, a.UID)),
					ChannelID: PluginID,
					Region:    a.RegionOf(),
					ExpiresAt: a.ExpiresAtText(),
					LoginKind: string(authflow.TypeDevicePoll),
					AuthFile:  f.AuthFile,
				}
			}
		}
	case qoder.FlowFailed:
		res.State = authflow.StateFailed
		res.Error = &authflow.Error{Kind: string(errkind.Auth), Message: orDefault(f.Error, "登录失败")}
	case qoder.FlowExpired:
		res.State = authflow.StateExpired
		res.Error = &authflow.Error{Kind: string(errkind.Auth), Message: "登录流程已过期，请重新发起"}
	case qoder.FlowCancelled:
		res.State = authflow.StateCancelled
	}
	return mustJSON(res), nil
}

// authFinish：device_poll 不接受人工回调。
func (d *Daemon) authFinish(ctx context.Context, p Raw) (Raw, *Err) {
	return nil, errf(errkind.BadParams, errkind.ScopeRequest,
		"该登录方式（device_poll）不需要粘贴回调地址，请等待自动轮询完成")
}

func (d *Daemon) authCancel(ctx context.Context, p Raw) (Raw, *Err) {
	var req authflow.CancelRequest
	_ = json.Unmarshal(p, &req)
	ok := d.login.Cancel(req.FlowID) == nil
	return mustJSON(map[string]any{"ok": ok}), nil
}

// ---- 账号 / 模型 / 额度 ----

// accountsList 上报账号事实（凭据内容绝不上报）。
func (d *Daemon) accountsList(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	out := []meta.AccountInfo{}
	list := d.store.List()
	for _, a := range list {
		if a.RegionOf() != cfg.regionPrefix() {
			continue
		}
		status := "ok"
		if a.Expired() {
			status = "expired"
		}
		out = append(out, meta.AccountInfo{
			UID:       a.AuthFile,
			Nickname:  orDefault(a.Nickname, a.UID),
			Status:    status,
			LoginKind: string(authflow.TypeDevicePoll),
			ExpiresAt: a.ExpiresAtText(),
			ChannelID: PluginID,
			Region:    a.RegionOf(),
			Prefixes:  []string{a.RegionOf()},
			AuthFile:  a.AuthFile,
		})
	}
	return mustJSON(map[string]any{"accounts": out}), nil
}

// account 取出（必要时刷新）指定账号；name 为空时取当前区第一个。
func (d *Daemon) account(name string, cfg Config) (*qoder.Auth, *qoder.Client, error) {
	prefix := cfg.regionPrefix()
	var picked *qoder.Auth
	if name != "" {
		if a, err := d.store.Load(name); err == nil {
			picked = a
		}
	}
	if picked == nil {
		list := d.store.List()
		for i := range list {
			it := list[i]
			if it.RegionOf() != prefix {
				continue
			}
			if name != "" && it.AuthFile != name && it.UID != name {
				continue
			}
			cp := it
			picked = &cp
			break
		}
	}
	if picked == nil {
		return nil, nil, &qoder.Error{Kind: errkind.Auth, Msg: "该渠道尚未登录，请先在面板完成登录"}
	}

	ep := qoder.EndpointsFor(picked.RegionOf())
	if cfg.BaseURL != "" {
		ep = ep.OverrideBase(cfg.BaseURL)
	}
	c := qoder.NewClient(ep, time.Duration(cfg.Timeout)*time.Second)
	if err := c.SetProxy(cfg.Proxy); err != nil && Log != nil {
		Log.Errorf("设置代理失败: %v", err)
	}
	if picked.Expired() && picked.RefreshToken != "" {
		if err := c.RefreshToken(context.Background(), picked); err != nil {
			return nil, nil, err
		}
		if err := d.store.Save(picked); err != nil && Log != nil {
			Log.Errorf("刷新后写回凭据失败: %v", err)
		}
	}
	return picked, c, nil
}

// modelsList 上报模型目录（模型 ID 带 <region>/ 前缀）。
func (d *Daemon) modelsList(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	prefix := cfg.regionPrefix()

	// qoder 区有静态模型表；cn/com 动态拉取。
	if dm := qoder.StaticModels(prefix); len(dm) > 0 && cfg.BaseURL == "" {
		ms := make([]meta.ModelInfo, 0, len(dm))
		for _, m := range dm {
			ctxv := int(m.ContextWindow)
			if ctxv <= 0 {
				ctxv = 180000
			}
			ms = append(ms, meta.ModelInfo{
				ID:      qoder.PublicModelID(prefix, m.Key),
				Name:    orDefault(m.DisplayName, m.Key),
				Context: &ctxv,
				Capabilities: meta.Capabilities2{
					Tools:     true,
					Reasoning: m.IsReasoning,
					Vision:    m.IsVL,
				},
			})
		}
		return mustJSON(map[string]any{"models": ms}), nil
	}

	acct, c, err := d.account("", cfg)
	if err != nil {
		k := qoder.ErrKindOf(err)
		return nil, errf(k, scopeOfKind(k), "%v", err)
	}
	upstream, err := c.FetchModels(ctx, acct)
	if err != nil {
		k := qoder.ErrKindOf(err)
		return nil, errf(k, scopeOfKind(k), "拉取模型失败: %v", err)
	}
	ms := make([]meta.ModelInfo, 0, len(upstream))
	for _, m := range upstream {
		m.ID = qoder.PublicModelID(prefix, m.ID)
		ms = append(ms, m)
	}
	return mustJSON(map[string]any{"models": ms}), nil
}

// resourceDetail 上报额度明细（ResourceItem 裸数组）。
func (d *Daemon) resourceDetail(ctx context.Context, p Raw) (Raw, *Err) {
	acct, c, err := d.account(uidOf(p), parseConfig(p))
	if err != nil {
		k := qoder.ErrKindOf(err)
		return nil, errf(k, scopeOfKind(k), "%v", err)
	}
	_, items, err := c.UserResourceDetail(ctx, acct)
	if err != nil {
		k := qoder.ErrKindOf(err)
		return nil, errf(k, scopeOfKind(k), "查询额度失败: %v", err)
	}
	if items == nil {
		items = []meta.ResourceItem{}
	}
	return mustJSON(items), nil
}

// creditsGet 返回余额与明细（null = 上游未提供 ≠ 0）。
func (d *Daemon) creditsGet(ctx context.Context, p Raw) (Raw, *Err) {
	acct, c, err := d.account(uidOf(p), parseConfig(p))
	if err != nil {
		return mustJSON(map[string]any{"balance": nil, "items": []meta.ResourceItem{}}), nil
	}
	balance, items, err := c.UserResourceDetail(ctx, acct)
	if err != nil {
		return mustJSON(map[string]any{"balance": nil, "items": []meta.ResourceItem{}}), nil
	}
	if items == nil {
		items = []meta.ResourceItem{}
	}
	return mustJSON(map[string]any{"balance": balance, "items": items, "unit": "credits"}), nil
}

// accountRefresh 刷新账号（token 续期 + 额度回读）。
func (d *Daemon) accountRefresh(ctx context.Context, p Raw) (Raw, *Err) {
	acct, c, err := d.account(uidOf(p), parseConfig(p))
	if err != nil {
		k := qoder.ErrKindOf(err)
		return nil, errf(k, scopeOfKind(k), "%v", err)
	}
	if acct.RefreshToken != "" {
		if err := c.RefreshToken(ctx, acct); err != nil {
			k := qoder.ErrKindOf(err)
			return nil, errf(k, scopeOfKind(k), "刷新凭据失败: %v", err)
		}
		if err := d.store.Save(acct); err != nil {
			return nil, errf(errkind.AccountFault, errkind.ScopeAccount, "写回凭据失败: %v", err)
		}
	}
	msg := "已刷新"
	if balance, _, err := c.UserResourceDetail(ctx, acct); err == nil {
		msg = fmt.Sprintf("已刷新，剩余 %.2f 积分", balance)
	}
	return mustJSON(map[string]any{"ok": true, "message": msg}), nil
}

// accountCheckin 执行签到（仅 qodercn 支持 campaigns）。
func (d *Daemon) accountCheckin(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	var req struct {
		Targets []string `json:"targets"`
	}
	_ = json.Unmarshal(p, &req)
	targets := req.Targets
	if len(targets) == 0 {
		if name := uidOf(p); name != "" {
			targets = []string{name}
		} else {
			list := d.store.List()
			for i := range list {
				if list[i].RegionOf() == cfg.regionPrefix() {
					targets = append(targets, list[i].AuthFile)
				}
			}
		}
	}
	if len(targets) == 0 {
		return nil, errf(errkind.Auth, errkind.ScopeAccount, "该渠道尚未登录，请先在面板完成登录")
	}
	msgs := []string{}
	okCount := 0
	for _, name := range targets {
		acct, c, err := d.account(name, cfg)
		if err != nil {
			msgs = append(msgs, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		label := orDefault(acct.Nickname, name)
		items, err := c.Checkin(ctx, acct)
		if err != nil {
			msgs = append(msgs, fmt.Sprintf("%s: %v", label, err))
			continue
		}
		okCount++
		if len(items) == 0 {
			msgs = append(msgs, fmt.Sprintf("%s: 已签到", label))
		} else {
			msgs = append(msgs, fmt.Sprintf("%s: 签到成功", label))
		}
	}
	if okCount == 0 {
		return nil, errf(errkind.Client, errkind.ScopeAccount, "签到失败: %s", strings.Join(msgs, "; "))
	}
	return mustJSON(map[string]any{"ok": true, "message": strings.Join(msgs, "; ")}), nil
}

// ---- 配置 / 统计 ----

func configSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"region": map[string]any{
				"type":        "string",
				"title":       "区域",
				"enum":        []string{qoder.RegionQoder, qoder.RegionQoderCN, qoder.RegionQoderCOM},
				"default":     qoder.RegionQoder,
				"description": "qoder=Qoder CLI（国内，静态模型表）；qodercn=Qoder 国内；qodercom=Qoder 国际。",
			},
			"proxy": map[string]any{
				"type":  "string",
				"title": "代理",
			},
			"timeout": map[string]any{
				"type":    "integer",
				"title":   "超时（秒）",
				"default": 300,
			},
			"base_url": map[string]any{
				"type":        "string",
				"title":       "上游基址覆盖",
				"description": "留空使用默认网关；调试时可指向 mock。",
			},
		},
	}
}

func parseConfig(p Raw) Config {
	var wrap struct {
		Config Config `json:"config"`
	}
	_ = json.Unmarshal(p, &wrap)
	if wrap.Config.Timeout <= 0 {
		wrap.Config.Timeout = 300
	}
	return wrap.Config
}

// mergeChannelConfig 用请求内嵌的渠道配置覆盖外层配置（非空字段优先）。
// 会话面（chat.generate）把渠道配置放在请求体的 channel_config 里；
// 不合并会导致 region/base_url 丢失，进而拿默认基址请求真实上游。
func mergeChannelConfig(cfg Config, req chat.GenerateRequest) Config {
	if len(req.ChannelConfig) == 0 {
		return cfg
	}
	var in Config
	_ = json.Unmarshal(req.ChannelConfig, &in)
	if in.Region != "" {
		cfg.Region = in.Region
	}
	if in.Proxy != "" {
		cfg.Proxy = in.Proxy
	}
	if in.BaseURL != "" {
		cfg.BaseURL = in.BaseURL
	}
	if in.Timeout > 0 {
		cfg.Timeout = in.Timeout
	}
	return cfg
}

func uidOf(p Raw) string {
	var wrap struct {
		UID string `json:"uid"`
	}
	_ = json.Unmarshal(p, &wrap)
	return wrap.UID
}

func (d *Daemon) statsGet(ctx context.Context, p Raw) (Raw, *Err) {
	cfg := parseConfig(p)
	var req struct {
		Days int `json:"days"`
	}
	_ = json.Unmarshal(p, &req)
	if req.Days <= 0 {
		req.Days = 7
	}
	if d.stat == nil {
		return mustJSON(stat.Summary{Days: []stat.Day{}}), nil
	}
	prefix := cfg.regionPrefix()
	recs := d.stat.Query(req.Days)
	filtered := recs[:0]
	for _, r := range recs {
		if qoder.NormalizeRegion(r.Region) == prefix {
			filtered = append(filtered, r)
		}
	}
	return mustJSON(stat.Summarize(filtered)), nil
}

// statRecord 构造一条基础统计记录（未上报 token 记 -1，不写 0）。
func statRecord(model string, ok bool, kind string) stat.Record {
	return stat.Record{
		Kind:             stat.KindChat,
		Channel:          PluginID,
		Model:            model,
		OK:               ok,
		ErrKind:          kind,
		PromptTokens:     -1,
		CompletionTokens: -1,
		TotalTokens:      -1,
	}
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
