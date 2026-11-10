package qoder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"wild-work3/qoder-ext/internal/protocol/errkind"
	"wild-work3/qoder-ext/internal/protocol/meta"
)

// DynamicModel 是上游模型列表中的一条（chat/assistant/developer 场景同构）。
type DynamicModel struct {
	Key            string  `json:"key"`
	DisplayName    string  `json:"display_name"`
	Enable         bool    `json:"enable"`
	IsDefault      bool    `json:"is_default"`
	IsReasoning    bool    `json:"is_reasoning"`
	IsVL           bool    `json:"is_vl"`
	MaxInputTokens int64   `json:"max_input_tokens"`
	PriceFactor    float64 `json:"price_factor"`
	Format         string  `json:"format"`
	Source         string  `json:"source"`

	// ContextWindow 是 context_config 默认档（is_default）的 token_count。
	ContextWindow int64 `json:"-"`
	// AvailableWindows 是 context_config 全部档位（升序）；空表示无档位表。
	AvailableWindows []int64 `json:"-"`
}

// contextConfig 形状：{"<label>": {"is_default":bool,"token_count":int}}
type contextConfig map[string]struct {
	IsDefault  bool  `json:"is_default"`
	TokenCount int64 `json:"token_count"`
}

// tokenCount 取上游声明的上下文窗口：只认标了 is_default 的一档。
// 多档同时标默认时取最小值（确定性 + 保守），不依赖 map 迭代序。
func (cc contextConfig) tokenCount() int64 {
	var best int64
	for _, cfg := range cc {
		if !cfg.IsDefault || cfg.TokenCount <= 0 {
			continue
		}
		if best == 0 || cfg.TokenCount < best {
			best = cfg.TokenCount
		}
	}
	return best
}

// windows 返回 context_config 全部档位（升序、去重、>0）。
func (cc contextConfig) windows() []int64 {
	seen := make(map[int64]struct{}, len(cc))
	out := make([]int64, 0, len(cc))
	for _, cfg := range cc {
		if cfg.TokenCount <= 0 {
			continue
		}
		if _, ok := seen[cfg.TokenCount]; ok {
			continue
		}
		seen[cfg.TokenCount] = struct{}{}
		out = append(out, cfg.TokenCount)
	}
	// 档位数 ≤4，插入排序即可。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// modelWire 在 DynamicModel 之外带上 context_config。
type modelWire struct {
	DynamicModel
	ContextConfig json.RawMessage `json:"context_config"`
}

func (w modelWire) parseContextConfig() (int64, []int64) {
	if len(w.ContextConfig) == 0 {
		return 0, nil
	}
	var cc contextConfig
	if err := json.Unmarshal(w.ContextConfig, &cc); err != nil {
		return 0, nil
	}
	return cc.tokenCount(), cc.windows()
}

// parseSceneModels 解析模型列表响应：assistant→developer→chat 三级回退。
func parseSceneModels(apiResp map[string]json.RawMessage) ([]DynamicModel, error) {
	for _, scene := range []string{"assistant", "developer", "chat"} {
		raw, ok := apiResp[scene]
		if !ok {
			continue
		}
		var wires []modelWire
		if err := json.Unmarshal(raw, &wires); err != nil {
			continue
		}
		enabled := make([]DynamicModel, 0, len(wires))
		for _, w := range wires {
			if !w.Enable || w.Key == "" {
				continue
			}
			m := w.DynamicModel
			m.ContextWindow, m.AvailableWindows = w.parseContextConfig()
			enabled = append(enabled, m)
		}
		if len(enabled) > 0 {
			return enabled, nil
		}
	}
	return nil, fmt.Errorf("no enabled models in assistant/developer/chat scenes")
}

// FetchModels 拉取上游动态模型并返回 meta.ModelInfo。
// 同时缓存 客户端名→key 映射与 key→条目表，供 chat 路由与取 format/source。
func (c *Client) FetchModels(ctx context.Context, a *Auth) ([]meta.ModelInfo, error) {
	dyn, err := c.fetchModels(ctx, a)
	if err != nil {
		return nil, err
	}
	mm := make(map[string]string, len(dyn))
	entries := make(map[string]DynamicModel, len(dyn))
	out := make([]meta.ModelInfo, 0, len(dyn))
	for _, m := range dyn {
		name := NormalizeModelName(m.DisplayName)
		if name == "" {
			name = m.Key
		}
		mm[name] = m.Key
		entries[m.Key] = m
		mi := meta.ModelInfo{
			ID:   name,
			Name: m.DisplayName,
			Capabilities: meta.Capabilities2{
				Tools:     true,
				Vision:    m.IsVL,
				Reasoning: m.IsReasoning,
			},
		}
		ctxWindow := int(defaultContextWindow)
		// 上下文窗口：max(档位表) > is_default 档 > max_input_tokens。
		switch {
		case len(m.AvailableWindows) > 0:
			ctxWindow = int(m.AvailableWindows[len(m.AvailableWindows)-1])
		case m.ContextWindow > 0:
			ctxWindow = int(m.ContextWindow)
		case m.MaxInputTokens > 0:
			ctxWindow = int(m.MaxInputTokens)
		}
		mi.Context = &ctxWindow
		out = append(out, mi)
	}
	c.setModelEntries(entries, mm)
	if len(out) == 0 {
		return nil, &Error{Kind: errkind.NotFound, Msg: "models api returned empty list"}
	}
	return out, nil
}

// fetchModels 调上游动态模型接口。GET 无 body，COSY 签名用空串 ""。
func (c *Client) fetchModels(ctx context.Context, a *Auth) ([]DynamicModel, error) {
	if a == nil || a.AccessToken == "" {
		return nil, &Error{Kind: errkind.SessionDead, Msg: "缺少访问令牌"}
	}
	rawURL := strings.TrimRight(c.Endpoints.Models, "/") + EpModels
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	sess, err := NewCosySession(c.Endpoints, a.MachineID, a.MachineToken, a.MachineType, a.Nickname, a.UID, a.AccessToken, a.RefreshToken, "")
	if err != nil {
		return nil, &Error{Kind: errkind.Server, Msg: "cosy session: " + err.Error()}
	}
	if err := sess.ApplyHeaders(req, c.Endpoints, "", rawURL, a.UID, "application/json", false, ""); err != nil {
		return nil, &Error{Kind: errkind.Server, Msg: "cosy headers: " + err.Error()}
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, &Error{Kind: errkind.Conn, Msg: err.Error()}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, &Error{
			Kind: Classify(resp.StatusCode, string(body)),
			Msg:  fmt.Sprintf("models api status %d: %s", resp.StatusCode, truncate(string(body), 300)),
		}
	}
	var apiResp map[string]json.RawMessage
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, &Error{Kind: errkind.Server, Msg: "models parse: " + err.Error()}
	}
	return parseSceneModels(apiResp)
}

// setModelEntries 同批写入 客户端名→key 与 key→条目表。
func (c *Client) setModelEntries(entries map[string]DynamicModel, mm map[string]string) {
	c.modelMu.Lock()
	defer c.modelMu.Unlock()
	c.entries = entries
	c.modelMap = mm
}

// KnownModel 报告客户端模型名是否可服务。
// 判定顺序：动态表 → 静态表；若两者都为空（从未刷新且无静态表）视为未知知识，返回 true 不误杀。
func (c *Client) KnownModel(name string) bool {
	n := UpstreamModelName(name)
	c.modelMu.RLock()
	k, ok := c.modelMap[n]
	hasDyn := len(c.modelMap) > 0
	c.modelMu.RUnlock()
	if ok && k != "" {
		return true
	}
	if StaticModelKey(c.Endpoints.Region, n) != "" {
		return true
	}
	return !hasDyn && len(StaticModels(c.Endpoints.Region)) == 0
}

// modelKey 把客户端模型名解析为上游 key：动态表优先，静态兜底。
func (c *Client) modelKey(name string) string {
	c.modelMu.RLock()
	k, ok := c.modelMap[name]
	c.modelMu.RUnlock()
	if ok && k != "" {
		return k
	}
	if k := StaticModelKey(c.Endpoints.Region, name); k != "" {
		return k
	}
	return name
}

// modelEntry 返回上游 key 对应的动态条目（可能为 nil）。
func (c *Client) modelEntry(key string) *DynamicModel {
	c.modelMu.RLock()
	defer c.modelMu.RUnlock()
	if m, ok := c.entries[key]; ok {
		cp := m
		return &cp
	}
	return nil
}

// NormalizeModelName 把 display_name 转成 OpenAI 风格客户端名。
func NormalizeModelName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	b.Grow(len(s))
	prevDash := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.':
			b.WriteRune(r)
			prevDash = false
		case r == ' ' || r == '_' || r == '-':
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
		default:
			b.WriteRune(r)
			prevDash = false
		}
	}
	return strings.Trim(b.String(), "-")
}

// truncate 截断字符串用于日志/错误信息，避免把整段上游响应写出来。
func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
