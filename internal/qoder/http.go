package qoder

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"wild-work3/qoder-ext/internal/protocol/errkind"
	"wild-work3/qoder-ext/internal/protocol/meta"
)

// Client 是 Qoder 上游客户端。
//
// 业务 API（Bearer）走 HTTP；COSY 签名的对话/模型接口走 Gateway（或 qodercom 的
// api1/api2）。两条通道共用同一 Transport：连接池复用，但强制 HTTP/1.1。
type Client struct {
	Endpoints Endpoints

	HTTP        *http.Client // 业务 API（有整体超时）
	StreamHTTP  *http.Client // SSE 对话（无整体超时，靠 IdleTimeout 兜底）
	IdleTimeout time.Duration
	tr          *http.Transport // 两条通道共用的连接池（SetProxy 时原地更新）

	modelMu  sync.RWMutex
	modelMap map[string]string       // 客户端名 → 上游 key
	entries  map[string]DynamicModel // 上游 key → 条目
}

// NewClient 构造默认客户端（业务 API 180s 超时）。
func NewClient(ep Endpoints, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 180 * time.Second
	}
	tr := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
		// Qoder gateway 对 HTTP/2 不友好（stream INTERNAL_ERROR），强制 HTTP/1.1。
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
	}
	return &Client{
		Endpoints:   ep,
		HTTP:        &http.Client{Timeout: timeout, Transport: tr},
		StreamHTTP:  &http.Client{Transport: tr},
		IdleTimeout: DefaultIdleTimeout,
		tr:          tr,
		modelMap:    map[string]string{},
		entries:     map[string]DynamicModel{},
	}
}

// SetProxy 设置上游代理（http:// 或 socks5://）。空串表示直连。
// 两条通道共用同一 Transport，故调一次即同时生效。
func (c *Client) SetProxy(proxy string) error {
	proxy = strings.TrimSpace(proxy)
	if proxy == "" {
		return nil
	}
	u, err := url.Parse(proxy)
	if err != nil {
		return &Error{Kind: errkind.BadParams, Msg: "代理地址无效: " + err.Error()}
	}
	if c.tr != nil {
		c.tr.Proxy = http.ProxyURL(u)
	}
	return nil
}

func (c *Client) streamHTTP() *http.Client {
	if c.StreamHTTP != nil {
		return c.StreamHTTP
	}
	return c.HTTP
}

func (c *Client) base() string    { return c.Endpoints.OpenAPI }
func (c *Client) gateway() string { return c.Endpoints.Gateway }

// RefreshToken 用 refresh_token 轮换访问令牌（dt/drt）。
// 401/403 → SessionDead（需重新 OAuth 登录）。
func (c *Client) RefreshToken(ctx context.Context, a *Auth) error {
	if a == nil {
		return &Error{Kind: errkind.Auth, Msg: "缺少账号"}
	}
	if strings.TrimSpace(a.RefreshToken) == "" {
		return &Error{Kind: errkind.SessionDead, Msg: "没有可用的 refresh token"}
	}
	body, _ := json.Marshal(map[string]string{"refresh_token": a.RefreshToken})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base()+EpDTRefresh, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", clientUA)

	// refresh 用独立短超时客户端，避免挂住整体请求。
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return &Error{Kind: errkind.Conn, Msg: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return &Error{Kind: errkind.SessionDead, Msg: truncate(string(raw), 200)}
	}
	if resp.StatusCode >= 400 {
		return &Error{
			Kind: Classify(resp.StatusCode, string(raw)),
			Msg:  fmt.Sprintf("deviceToken refresh http %d: %s", resp.StatusCode, truncate(string(raw), 200)),
		}
	}
	var out struct {
		Token        string `json:"token"`
		DeviceToken  string `json:"device_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresAt    string `json:"expires_at"`
		ExpiresIn    int64  `json:"expires_in"` // 毫秒
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return &Error{Kind: errkind.Server, Msg: "deviceToken refresh parse: " + err.Error()}
	}
	dt := out.Token
	if dt == "" {
		dt = out.DeviceToken
	}
	if dt == "" || out.RefreshToken == "" {
		return &Error{Kind: errkind.Server, Msg: "deviceToken refresh: incomplete token pair"}
	}
	a.AccessToken = dt
	a.RefreshToken = out.RefreshToken
	now := time.Now()
	switch {
	case out.ExpiresIn > 0:
		a.ExpiresAt = now.Add(time.Duration(out.ExpiresIn) * time.Millisecond).Unix()
	case out.ExpiresAt != "":
		if t, err := time.Parse(time.RFC3339, out.ExpiresAt); err == nil {
			a.ExpiresAt = t.Unix()
		}
	}
	if a.ExpiresAt == 0 {
		// dt 观测寿命 30d。
		a.ExpiresAt = now.Add(30 * 24 * time.Hour).Unix()
	}
	return nil
}

// get 发起带 Bearer 的 GET。
func (c *Client) get(ctx context.Context, path, token string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base()+path, nil)
	if err != nil {
		return 0, nil, err
	}
	ApplyBearer(req, token)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, nil, &Error{Kind: errkind.Conn, Msg: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw, nil
}

// quotaPayload 是 /api/v2/quota/usage 的响应形状。
type quotaPayload struct {
	UserQuota struct {
		Total     float64 `json:"total"`
		Used      float64 `json:"used"`
		Remaining float64 `json:"remaining"`
	} `json:"userQuota"`
	AddOnQuota struct {
		Total     float64 `json:"total"`
		Used      float64 `json:"used"`
		Remaining float64 `json:"remaining"`
	} `json:"addOnQuota"`
	IsQuotaExceeded bool `json:"isQuotaExceeded"`
}

func (c *Client) fetchQuota(ctx context.Context, a *Auth) (*quotaPayload, error) {
	if a == nil || a.AccessToken == "" {
		return nil, &Error{Kind: errkind.Auth, Msg: "缺少访问令牌"}
	}
	status, raw, err := c.get(ctx, EpQuotaUsage, a.AccessToken)
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &Error{Kind: Classify(status, string(raw)), Msg: truncate(string(raw), 200)}
	}
	var q quotaPayload
	if err := json.Unmarshal(raw, &q); err != nil {
		return nil, &Error{Kind: errkind.Server, Msg: "quota parse: " + err.Error()}
	}
	return &q, nil
}

// UserResource 查询账号当前可花费积分（基础 + 赠送聚合）。
func (c *Client) UserResource(ctx context.Context, a *Auth) (float64, error) {
	q, err := c.fetchQuota(ctx, a)
	if err != nil {
		return 0, err
	}
	return q.UserQuota.Remaining + q.AddOnQuota.Remaining, nil
}

// UserResourceDetail 查询积分明细：userQuota + addOnQuota 两个条目。
func (c *Client) UserResourceDetail(ctx context.Context, a *Auth) (float64, []meta.ResourceItem, error) {
	q, err := c.fetchQuota(ctx, a)
	if err != nil {
		return 0, nil, err
	}
	total := q.UserQuota.Remaining + q.AddOnQuota.Remaining
	f := func(v float64) *float64 { return &v }
	items := []meta.ResourceItem{{
		Name: "用户套餐", Total: f(q.UserQuota.Total), Used: f(q.UserQuota.Used),
		Remain: f(q.UserQuota.Remaining), Unit: "credits", Usable: true, Key: "userQuota",
	}}
	if q.AddOnQuota.Total > 0 || q.AddOnQuota.Remaining > 0 {
		items = append(items, meta.ResourceItem{
			Name: "赠送额度", Total: f(q.AddOnQuota.Total), Used: f(q.AddOnQuota.Used),
			Remain: f(q.AddOnQuota.Remaining), Unit: "credits", Usable: true, Key: "addOnQuota",
		})
	}
	return total, items, nil
}
