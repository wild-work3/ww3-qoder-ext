package qoder

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ErrPending 表示 device_poll 尚未获批（上游 404/202）。
var ErrPending = errors.New("login pending")

// LoginResult 是一次成功 device_poll 的结果（**含 token，仅插件内部使用**）。
type LoginResult struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int64
	UID          string
	Nickname     string
}

// makePKCE 生成 PKCE verifier/challenge。
// verifier 走 RFC 7636 允许的无保留字符集，challenge = base64url(sha256(verifier))。
func makePKCE() (verifier, challenge string, err error) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
	buf := make([]byte, 64)
	if _, err = rand.Read(buf); err != nil {
		return "", "", err
	}
	sb := make([]byte, 64)
	for i, b := range buf {
		sb[i] = alphabet[int(b)%len(alphabet)]
	}
	verifier = string(sb)
	sum := sha256.Sum256([]byte(verifier))
	challenge = base64.RawURLEncoding.EncodeToString(sum[:])
	return verifier, challenge, nil
}

// LoginManager 管理 device_poll 登录流程（状态落盘 <data>/auths/flows/）。
type LoginManager struct {
	flows *FlowStore
	store *Store
	http  *http.Client
}

func NewLoginManager(dataDir string, store *Store) *LoginManager {
	return &LoginManager{
		flows: NewFlowStore(dataDir),
		store: store,
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

func (m *LoginManager) Flows() *FlowStore { return m.flows }

// Begin 发起一次 device_poll 登录，返回流程（含 authURL）。
func (m *LoginManager) Begin(region, base string) (*Flow, error) {
	ep := EndpointsFor(region)
	if base = strings.TrimRight(strings.TrimSpace(base), "/"); base != "" {
		ep = ep.OverrideBase(base) // 联调：config.base_url 同时覆盖 device_poll 轮询基址
	}
	verifier, challenge, err := makePKCE()
	if err != nil {
		return nil, err
	}
	nonce := newSimpleID()
	machineID := ""
	authURL := fmt.Sprintf("%s/device/selectAccounts?challenge=%s&challenge_method=S256&nonce=%s&client_id=%s",
		ep.Site(), challenge, nonce, ep.ClientID)
	if ep.IsCLI() {
		// qoder 区：nonce 用 UUID，且额外带 machine_id / redirect_uri。
		nonce = uuid4()
		machineID = uuid4()
		authURL = fmt.Sprintf("%s/device/selectAccounts?challenge=%s&challenge_method=S256&nonce=%s&machine_id=%s&client_id=%s&redirect_uri=%s",
			ep.Site(), challenge, nonce, machineID, ep.ClientID, ep.RedirectURI)
	}

	f := &Flow{
		ID:         uuid4(),
		Region:     ep.Region,
		Base:       ep.OpenAPI,
		Verifier:   verifier,
		Nonce:      nonce,
		State:      FlowWaitingUser,
		AuthURL:    authURL,
		CreatedAt:  time.Now().UnixMilli(),
		MachineID:  machineID,
		NextPollMS: 2000,
	}
	if err := m.flows.Save(f); err != nil {
		return nil, err
	}
	return f, nil
}

// PollOnce 轮询一次设备授权；处于 pending 时返回 ErrPending。
func (m *LoginManager) PollOnce(ctx context.Context, f *Flow) (*LoginResult, error) {
	if f == nil {
		return nil, fmt.Errorf("缺少流程")
	}
	ep := EndpointsFor(f.Region)
	if f.Base != "" {
		ep = ep.OverrideBase(f.Base)
	}
	url := fmt.Sprintf("%s%s?nonce=%s&verifier=%s&challenge_method=S256",
		ep.OpenAPI, EpDevicePoll, f.Nonce, f.Verifier)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", clientUA)
	req.Header.Set("Accept", "application/json")
	resp, err := m.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	// 404（未授权）与 202（已接受待处理）都继续等；其余 >=400 视为失败。
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusAccepted {
		return nil, ErrPending
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}

	var tok map[string]any
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	t := getString(tok, "token")
	if t == "" {
		t = getString(tok, "device_token")
	}
	if t == "" {
		return nil, ErrPending
	}
	refresh := getString(tok, "refresh_token")
	uid := getString(tok, "user_id")
	if uid == "" {
		uid = getString(tok, "uid")
	}
	nickname := getString(tok, "name")
	if nickname == "" {
		nickname = getString(tok, "nickname")
	}
	return &LoginResult{
		AccessToken:  t,
		RefreshToken: refresh,
		ExpiresIn:    int64(getFloat(tok, "expires_in")),
		UID:          uid,
		Nickname:     nickname,
	}, nil
}

// Complete 把一次成功登录落盘为凭据，返回凭据文件标识。
func (m *LoginManager) Complete(f *Flow, r *LoginResult) (string, error) {
	if r == nil || strings.TrimSpace(r.UID) == "" {
		return "", fmt.Errorf("missing uid in result")
	}
	expiresAt := time.Now().Unix() + int64((48 * time.Hour).Seconds())
	if r.ExpiresIn > 0 {
		expiresAt = time.Now().Unix() + r.ExpiresIn
	}
	ep := EndpointsFor(f.Region)
	a := &Auth{
		AccessToken:  r.AccessToken,
		RefreshToken: r.RefreshToken,
		ExpiresAt:    expiresAt,
		Domain:       ep.Domain,
		UID:          r.UID,
		Nickname:     r.Nickname,
	}
	if f.MachineID != "" {
		a.MachineID = f.MachineID
	}
	EnsureFingerprint(a)
	if err := m.store.Save(a); err != nil {
		return "", err
	}
	name := AuthFileName(a)
	f.State = FlowSuccess
	f.UID = r.UID
	f.Nickname = r.Nickname
	f.AuthFile = name
	_ = m.flows.Save(f)
	return name, nil
}

// Cancel 标记流程取消。
func (m *LoginManager) Cancel(id string) error {
	return m.flows.Update(id, func(f *Flow) {
		f.State = FlowCancelled
	})
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

func getFloat(m map[string]any, key string) float64 {
	if m == nil {
		return 0
	}
	switch v := m[key].(type) {
	case float64:
		return v
	case json.Number:
		f, _ := v.Float64()
		return f
	case int64:
		return float64(v)
	case int:
		return float64(v)
	}
	return 0
}
