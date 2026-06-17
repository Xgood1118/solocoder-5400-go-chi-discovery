package election

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
)

type LeaderElection interface {
	IsLeader() bool
	Start(ctx context.Context) error
	Stop()
}

type NoopElection struct {
	isLeader bool
}

func NewNoopElection() *NoopElection {
	return &NoopElection{isLeader: true}
}

func (n *NoopElection) IsLeader() bool {
	return n.isLeader
}

func (n *NoopElection) Start(ctx context.Context) error {
	return nil
}

func (n *NoopElection) Stop() {}

type EtcdElection struct {
	nodeID      string
	etcdEndpoints []string
	leaseTTL    int64
	isLeader    bool
	mu          sync.RWMutex
	logger      hclog.Logger
	etcdClient  interface{}
	cancelFunc  context.CancelFunc
}

func NewEtcdElection(nodeID string, etcdEndpoints []string, leaseTTL int64, logger hclog.Logger) *EtcdElection {
	return &EtcdElection{
		nodeID:        nodeID,
		etcdEndpoints: etcdEndpoints,
		leaseTTL:      leaseTTL,
		logger:        logger,
	}
}

func (e *EtcdElection) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

func (e *EtcdElection) Start(ctx context.Context) error {
	client, err := e.createEtcdClient()
	if err != nil {
		e.logger.Warn("etcd client not available, falling back to noop election", "error", err)
		return nil
	}
	e.etcdClient = client

	runCtx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel

	go e.runElectionLoop(runCtx)

	return nil
}

func (e *EtcdElection) Stop() {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
}

func (e *EtcdElection) createEtcdClient() (interface{}, error) {
	return nil, fmt.Errorf("etcd client not imported (optional dependency)")
}

func (e *EtcdElection) runElectionLoop(ctx context.Context) {
	defer func() {
		e.mu.Lock()
		e.isLeader = false
		e.mu.Unlock()
	}()

	ticker := time.NewTicker(time.Duration(e.leaseTTL) * time.Second / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tryAcquireLeadership(ctx)
		}
	}
}

func (e *EtcdElection) tryAcquireLeadership(ctx context.Context) {
	lockFile := fmt.Sprintf("/tmp/discovery-leader-%s.lock", e.nodeID)
	f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		e.mu.Lock()
		e.isLeader = false
		e.mu.Unlock()
		return
	}
	defer f.Close()
	defer os.Remove(lockFile)

	_, _ = f.WriteString(fmt.Sprintf("%s\n%d", e.nodeID, time.Now().Unix()))

	e.mu.Lock()
	e.isLeader = true
	e.mu.Unlock()

	leaseCtx, cancel := context.WithTimeout(ctx, time.Duration(e.leaseTTL)*time.Second)
	defer cancel()

	<-leaseCtx.Done()
}

type FileLockElection struct {
	nodeID     string
	lockPath   string
	leaseTTL   int64
	isLeader   bool
	mu         sync.RWMutex
	logger     hclog.Logger
	cancelFunc context.CancelFunc
}

func NewFileLockElection(nodeID string, dataDir string, leaseTTL int64, logger hclog.Logger) *FileLockElection {
	return &FileLockElection{
		nodeID:   nodeID,
		lockPath: fmt.Sprintf("%s/leader.lock", dataDir),
		leaseTTL: leaseTTL,
		logger:   logger,
	}
}

func (f *FileLockElection) IsLeader() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.isLeader
}

func (f *FileLockElection) Start(ctx context.Context) error {
	if err := os.MkdirAll(f.lockPath[:len(f.lockPath)-len("/leader.lock")], 0755); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	f.cancelFunc = cancel

	go f.runElectionLoop(runCtx)

	return nil
}

func (f *FileLockElection) Stop() {
	if f.cancelFunc != nil {
		f.cancelFunc()
	}
}

func (f *FileLockElection) runElectionLoop(ctx context.Context) {
	defer func() {
		f.mu.Lock()
		f.isLeader = false
		f.mu.Unlock()
		_ = os.Remove(f.lockPath)
	}()

	ticker := time.NewTicker(time.Duration(f.leaseTTL) * time.Second / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.tryAcquireLeadership()
		}
	}
}

func (f *FileLockElection) tryAcquireLeadership() {
	info, err := os.Stat(f.lockPath)
	if err == nil {
		if time.Since(info.ModTime()) < time.Duration(f.leaseTTL)*time.Second {
			content, err := os.ReadFile(f.lockPath)
			if err == nil && string(content) == f.nodeID {
				f.mu.Lock()
				f.isLeader = true
				f.mu.Unlock()
			} else {
				f.mu.Lock()
				f.isLeader = false
				f.mu.Unlock()
			}
			return
		}
		_ = os.Remove(f.lockPath)
	}

	file, err := os.OpenFile(f.lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		f.mu.Lock()
		f.isLeader = false
		f.mu.Unlock()
		return
	}
	defer file.Close()

	_, _ = file.WriteString(f.nodeID)

	f.mu.Lock()
	f.isLeader = true
	f.mu.Unlock()

	f.logger.Info("acquired leadership", "node", f.nodeID)
}
