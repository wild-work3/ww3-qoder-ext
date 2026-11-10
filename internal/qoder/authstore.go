package qoder

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Auth 是一条渠道凭据。
//
// AccessToken 是上游的 dt- 设备令牌，RefreshToken 是 drt- 刷新令牌。两者都只
// 存在于插件私有目录，绝不会经 IPC 回传给核心——核心只能拿到 AccountInfo 里的事实字段。
type Auth struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    int64  `json:"expiresAt"` // Unix 秒
	Domain       string `json:"domain,omitempty"`
	MachineID    string `json:"machineId,omitempty"`
	MachineToken string `json:"machineToken,omitempty"`
	MachineType  string `json:"machineType,omitempty"`

	// UID / Nickname 是上游账号身份，来自登录结果与 userinfo。
	UID      string `json:"uid"`
	Nickname string `json:"nickname,omitempty"`

	// AuthFile 是凭据文件名（不含 .json），即核心眼里的 account_uid。不落盘。
	AuthFile string `json:"-"`
}

// RegionOf 从凭据文件名前缀判定区域；文件名缺失时退化为 qoder 区。
func (a *Auth) RegionOf() string {
	if a == nil {
		return RegionQoder
	}
	if name := a.AuthFile; name != "" {
		if i := strings.IndexByte(name, '-'); i > 0 {
			return NormalizeRegion(name[:i])
		}
	}
	return RegionQoder
}

// ExpiresAtText 把 Unix 秒到期时间格式化成 RFC3339（核心侧 AccountInfo.ExpiresAt 是字符串）。
func (a *Auth) ExpiresAtText() string {
	if a == nil || a.ExpiresAt <= 0 {
		return ""
	}
	return time.Unix(a.ExpiresAt, 0).Format(time.RFC3339)
}

// Expired 报告令牌是否已到需要刷新的时间点（留 60 秒余量）。
func (a *Auth) Expired() bool {
	if a == nil || a.ExpiresAt <= 0 {
		return true
	}
	return time.Now().Unix() >= a.ExpiresAt-60
}

// AuthFileName 返回凭据文件名（不含 .json）。
func AuthFileName(a *Auth) string {
	if a == nil {
		return ""
	}
	if a.AuthFile != "" {
		return a.AuthFile
	}
	return AuthName(a.RegionOf(), a.UID)
}

// Store 管理 <data>/auths 下的凭据文件。
type Store struct {
	dir string
	mu  sync.Mutex
}

// NewStore 返回以 dataDir 为数据根目录的凭据仓库。
func NewStore(dataDir string) *Store {
	return &Store{dir: dataDir}
}

// Dir 是凭据目录（<data>/auths）。
func (s *Store) Dir() string { return filepath.Join(s.dir, "auths") }

// Path 返回某个凭据名对应的文件路径。
func (s *Store) Path(name string) string {
	return filepath.Join(s.Dir(), cleanName(name)+".json")
}

func cleanName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.TrimSuffix(name, ".json")
	name = filepath.Base(name)
	if name == "." || name == ".." {
		return ""
	}
	return name
}

// authDoc 是磁盘上的固定嵌套形：{"auth":{...},"account":{...}}。
type authDoc struct {
	Auth    authBody `json:"auth"`
	Account acctBody `json:"account"`
}

type authBody struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    int64  `json:"expiresAt"`
	Domain       string `json:"domain,omitempty"`
	MachineID    string `json:"machineId,omitempty"`
	MachineToken string `json:"machineToken,omitempty"`
	MachineType  string `json:"machineType,omitempty"`
}

type acctBody struct {
	UID      string `json:"uid"`
	Nickname string `json:"nickname,omitempty"`
}

func toDoc(a *Auth) authDoc {
	return authDoc{
		Auth: authBody{
			AccessToken:  a.AccessToken,
			RefreshToken: a.RefreshToken,
			ExpiresAt:    a.ExpiresAt,
			Domain:       a.Domain,
			MachineID:    a.MachineID,
			MachineToken: a.MachineToken,
			MachineType:  a.MachineType,
		},
		Account: acctBody{UID: a.UID, Nickname: a.Nickname},
	}
}

// Save 原子写入凭据（tmp 0600 → rename），目录 0700。
func (s *Store) Save(a *Auth) error {
	if a == nil || a.UID == "" {
		return errors.New("qoder: 凭据缺少 uid，无法落盘")
	}
	name := cleanName(AuthFileName(a))
	if name == "" {
		return errors.New("qoder: 凭据文件名无效")
	}
	dir := s.Dir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(toDoc(a), "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	tmp, err := os.CreateTemp(dir, "."+name+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name+".json")); err != nil {
		return err
	}
	a.AuthFile = name
	return nil
}

// Load 读取一条凭据。name 可带或不带 .json。
func (s *Store) Load(name string) (*Auth, error) {
	name = cleanName(name)
	if name == "" {
		return nil, errors.New("qoder: 凭据名无效")
	}
	data, err := os.ReadFile(filepath.Join(s.Dir(), name+".json"))
	if err != nil {
		return nil, err
	}
	a, err := parseAuthFile(data)
	if err != nil {
		return nil, fmt.Errorf("qoder: 解析凭据 %s 失败: %w", name, err)
	}
	a.AuthFile = name
	if a.UID == "" {
		if i := strings.IndexByte(name, '-'); i > 0 {
			a.UID = name[i+1:]
		}
	}
	return a, nil
}

// List 列出本插件全部凭据（按文件名排序）。
func (s *Store) List() []Auth {
	entries, err := os.ReadDir(s.Dir())
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		if !isCredName(name) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]Auth, 0, len(names))
	for _, name := range names {
		a, err := s.Load(name)
		if err != nil {
			continue
		}
		out = append(out, *a)
	}
	return out
}

// isCredName 判断文件名是否属于本插件的凭据（<region>-<uid>）。
func isCredName(name string) bool {
	i := strings.IndexByte(name, '-')
	if i <= 0 || i == len(name)-1 {
		return false
	}
	switch name[:i] {
	case RegionQoder, RegionQoderCN, RegionQoderCOM:
		return true
	}
	return false
}

// Find 按核心传下来的 account_uid（凭据文件名）查找凭据。
func (s *Store) Find(name string) (*Auth, error) {
	return s.Load(name)
}

// Delete 删除一条凭据。
func (s *Store) Delete(name string) error {
	name = cleanName(name)
	if name == "" {
		return errors.New("qoder: 凭据名无效")
	}
	return os.Remove(filepath.Join(s.Dir(), name+".json"))
}

// parseAuthFile 解析凭据，兼容固定的嵌套形与历史扁平形。
func parseAuthFile(data []byte) (*Auth, error) {
	var probe struct {
		Auth    json.RawMessage `json:"auth"`
		Account *acctBody       `json:"account"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, err
	}
	if len(probe.Auth) > 0 && string(probe.Auth) != "null" {
		var body authBody
		if err := json.Unmarshal(probe.Auth, &body); err != nil {
			return nil, err
		}
		a := &Auth{
			AccessToken:  body.AccessToken,
			RefreshToken: body.RefreshToken,
			ExpiresAt:    body.ExpiresAt,
			Domain:       body.Domain,
			MachineID:    body.MachineID,
			MachineToken: body.MachineToken,
			MachineType:  body.MachineType,
		}
		if probe.Account != nil {
			a.UID = probe.Account.UID
			a.Nickname = probe.Account.Nickname
		}
		return a, nil
	}

	// 扁平形：直接是 Auth 的字段集合。
	var flat struct {
		AccessToken  string `json:"accessToken"`
		RefreshToken string `json:"refreshToken"`
		ExpiresAt    int64  `json:"expiresAt"`
		Domain       string `json:"domain,omitempty"`
		MachineID    string `json:"machineId,omitempty"`
		MachineToken string `json:"machineToken,omitempty"`
		MachineType  string `json:"machineType,omitempty"`
		UID          string `json:"uid"`
		Nickname     string `json:"nickname,omitempty"`
	}
	if err := json.Unmarshal(data, &flat); err != nil {
		return nil, err
	}
	return &Auth{
		AccessToken:  flat.AccessToken,
		RefreshToken: flat.RefreshToken,
		ExpiresAt:    flat.ExpiresAt,
		Domain:       flat.Domain,
		MachineID:    flat.MachineID,
		MachineToken: flat.MachineToken,
		MachineType:  flat.MachineType,
		UID:          flat.UID,
		Nickname:     flat.Nickname,
	}, nil
}
