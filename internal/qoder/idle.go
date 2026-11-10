package qoder

import (
	"errors"
	"io"
	"sync"
	"time"
)

// DefaultIdleTimeout 是 SSE 流空闲上限：超过即视为上游卡死。
const DefaultIdleTimeout = 90 * time.Second

// ErrIdleTimeout 表示上游流长时间无数据。
var ErrIdleTimeout = errors.New("idle timeout: 上游流空闲超时")

// NewIdleReader 包装 io.ReadCloser，超过 idle 无数据即关闭底层并返回 ErrIdleTimeout。
func NewIdleReader(rc io.ReadCloser, idle time.Duration) io.ReadCloser {
	if rc == nil || idle <= 0 {
		return rc
	}
	r := &idleReader{rc: rc, idle: idle}
	r.timer = time.AfterFunc(idle, r.fire)
	return r
}

type idleReader struct {
	rc   io.ReadCloser
	idle time.Duration

	mu    sync.Mutex
	timer *time.Timer
	fired bool
}

func (r *idleReader) fire() {
	r.mu.Lock()
	r.fired = true
	r.mu.Unlock()
	_ = r.rc.Close()
}

func (r *idleReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	r.mu.Lock()
	if r.fired {
		r.mu.Unlock()
		if n > 0 {
			return n, nil
		}
		return n, ErrIdleTimeout
	}
	if n > 0 && r.timer != nil {
		r.timer.Reset(r.idle)
	}
	r.mu.Unlock()
	return n, err
}

func (r *idleReader) Close() error {
	r.mu.Lock()
	if r.timer != nil {
		r.timer.Stop()
	}
	r.mu.Unlock()
	return r.rc.Close()
}
