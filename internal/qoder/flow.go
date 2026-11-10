package qoder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// flowTTL 是流程文件的存活窗口：超过即视为过期（GC 会清理）。
const flowTTL = 5 * time.Minute

// 流程状态（与协议 authflow.State* 对齐）。
const (
	FlowStarting    = "starting"
	FlowWaitingUser = "waiting_user"
	FlowSuccess     = "success"
	FlowFailed      = "failed"
	FlowExpired     = "expired"
	FlowCancelled   = "cancelled"
)

// Flow 是一次 device_poll 登录的落盘状态。
// 只存轮询所需的最小信息（verifier/nonce），**不含任何 token**。
type Flow struct {
	ID          string `json:"id"`
	Region      string `json:"region"`
	Verifier    string `json:"verifier"`
	Nonce       string `json:"nonce"`
	Base        string `json:"base,omitempty"`
	State       string `json:"state"`
	AuthURL     string `json:"authURL"`
	CreatedAt   int64  `json:"createdAt"`
	UserCode    string `json:"userCode,omitempty"`
	Error       string `json:"error,omitempty"`
	UID         string `json:"uid,omitempty"`
	Nickname    string `json:"nickname,omitempty"`
	AuthFile    string `json:"authFile,omitempty"`
	NextPollMS  int64  `json:"nextPollMS,omitempty"`
	MachineID   string `json:"machineID,omitempty"`
	MachineName string `json:"machineName,omitempty"`
}

// FlowStore 负责 <data>/auths/flows/<id>.json 的原子读写。
type FlowStore struct {
	Dir string
	mu  sync.Mutex
}

func NewFlowStore(dataDir string) *FlowStore {
	return &FlowStore{Dir: filepath.Join(dataDir, "auths", "flows")}
}

func (s *FlowStore) path(id string) string {
	return filepath.Join(s.Dir, flowFileName(id)+".json")
}

// flowFileName 防止流程 id 里混入路径分隔符造成目录穿越。
func flowFileName(id string) string {
	base := filepath.Base(strings.TrimSpace(id))
	if base == "" || base == "." || base == ".." || strings.ContainsAny(base, `/\`) {
		return "invalid"
	}
	return base
}

// Save 原子落盘（tempfile 0600 → Rename），目录 0700。
func (s *FlowStore) Save(f *Flow) error {
	if f == nil || strings.TrimSpace(f.ID) == "" {
		return fmt.Errorf("流程缺少 ID")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.Dir, ".flow-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path(f.ID))
}

func (s *FlowStore) Load(id string) (*Flow, error) {
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("缺少 flow_id")
	}
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		return nil, err
	}
	var f Flow
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("流程文件损坏: %w", err)
	}
	if f.ID == "" {
		f.ID = flowFileName(id)
	}
	return &f, nil
}

// Update 读改写；回调返回错误则不落盘。
func (s *FlowStore) Update(id string, fn func(*Flow)) error {
	f, err := s.Load(id)
	if err != nil {
		return err
	}
	fn(f)
	return s.Save(f)
}

// Delete 删除流程文件；不存在视为成功。
func (s *FlowStore) Delete(id string) error {
	err := os.Remove(s.path(id))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// GC 清理超期的终态流程，返回删除个数。
func (s *FlowStore) GC() int {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return 0
	}
	cutoff := time.Now().Add(-flowTTL * 3).UnixMilli()
	removed := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		f, err := s.Load(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil || f.CreatedAt > cutoff {
			continue
		}
		switch f.State {
		case FlowSuccess, FlowFailed, FlowExpired, FlowCancelled:
			if s.Delete(f.ID) == nil {
				removed++
			}
		}
	}
	return removed
}
