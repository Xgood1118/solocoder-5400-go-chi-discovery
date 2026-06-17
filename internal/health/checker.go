package health

import (
	"context"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"

	"discovery/internal/dto"
	"discovery/internal/registry"
)

type Checker interface {
	Check(entry *dto.ServiceEntry) dto.ServiceStatus
}

type HTTPChecker struct{}

func NewHTTPChecker() *HTTPChecker {
	return &HTTPChecker{}
}

func (c *HTTPChecker) Check(entry *dto.ServiceEntry) dto.ServiceStatus {
	if entry.HealthCheck.HTTP == "" {
		return dto.StatusPassing
	}

	url := entry.HealthCheck.HTTP
	timeout := entry.HealthCheck.Timeout.Duration()
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return dto.StatusCritical
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return dto.StatusCritical
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return dto.StatusPassing
	}
	if resp.StatusCode >= 400 {
		return dto.StatusCritical
	}
	return dto.StatusWarning
}

type TCPChecker struct{}

func NewTCPChecker() *TCPChecker {
	return &TCPChecker{}
}

func (c *TCPChecker) Check(entry *dto.ServiceEntry) dto.ServiceStatus {
	addr := entry.HealthCheck.TCP
	if addr == "" {
		addr = net.JoinHostPort(entry.Address, strconv.Itoa(entry.Port))
	}

	timeout := entry.HealthCheck.Timeout.Duration()
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return dto.StatusCritical
	}
	defer conn.Close()

	return dto.StatusPassing
}

type TTLChecker struct{}

func NewTTLChecker() *TTLChecker {
	return &TTLChecker{}
}

func (c *TTLChecker) Check(entry *dto.ServiceEntry) dto.ServiceStatus {
	ttl := entry.HealthCheck.TTL.Duration()
	if ttl == 0 {
		ttl = entry.TTL
	}
	if ttl == 0 {
		ttl = 30 * time.Second
	}

	elapsed := time.Since(entry.LastHeartbeat)
	if elapsed < ttl {
		return dto.StatusPassing
	}
	if elapsed < ttl*2 {
		return dto.StatusWarning
	}
	return dto.StatusCritical
}

type ScriptChecker struct{}

func NewScriptChecker() *ScriptChecker {
	return &ScriptChecker{}
}

func (c *ScriptChecker) Check(entry *dto.ServiceEntry) dto.ServiceStatus {
	if entry.HealthCheck.Script == "" {
		return dto.StatusPassing
	}

	timeout := entry.HealthCheck.Timeout.Duration()
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", entry.HealthCheck.Script)
	err := cmd.Run()

	if err != nil {
		if ctx.Err() != nil {
			return dto.StatusWarning
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() == 0 {
				return dto.StatusPassing
			}
			return dto.StatusCritical
		}
		return dto.StatusCritical
	}

	return dto.StatusPassing
}

type HealthCheckManager struct {
	registry   *registry.Registry
	checkers   map[dto.HealthCheckType]Checker
	checks     map[string]context.CancelFunc
	checksMu   sync.Mutex
	logger     hclog.Logger
	resultChan chan checkResult
}

type checkResult struct {
	serviceID string
	status    dto.ServiceStatus
}

func NewHealthCheckManager(r *registry.Registry, logger hclog.Logger) *HealthCheckManager {
	m := &HealthCheckManager{
		registry:   r,
		checkers:   make(map[dto.HealthCheckType]Checker),
		checks:     make(map[string]context.CancelFunc),
		logger:     logger,
		resultChan: make(chan checkResult, 100),
	}

	m.checkers[dto.CheckHTTP] = NewHTTPChecker()
	m.checkers[dto.CheckTCP] = NewTCPChecker()
	m.checkers[dto.CheckTTL] = NewTTLChecker()
	m.checkers[dto.CheckScript] = NewScriptChecker()

	go m.processResults()

	return m
}

func (m *HealthCheckManager) StartCheck(entry *dto.ServiceEntry) {
	m.checksMu.Lock()
	defer m.checksMu.Unlock()

	if cancel, ok := m.checks[entry.ServiceID]; ok {
		cancel()
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.checks[entry.ServiceID] = cancel

	go m.runCheckLoop(ctx, entry)
}

func (m *HealthCheckManager) StopCheck(serviceID string) {
	m.checksMu.Lock()
	defer m.checksMu.Unlock()

	if cancel, ok := m.checks[serviceID]; ok {
		cancel()
		delete(m.checks, serviceID)
	}
}

func (m *HealthCheckManager) runCheckLoop(ctx context.Context, entry *dto.ServiceEntry) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("health check panic recovered", "service", entry.ServiceID, "panic", r)
		}
	}()

	checkType := inferCheckType(entry)
	interval := entry.HealthCheck.Interval.Duration()
	if interval == 0 {
		interval = 10 * time.Second
	}

	gracePeriod := interval * 2
	graceUntil := entry.RegisteredAt.Add(gracePeriod)

	initialDelay := time.Until(graceUntil)
	if initialDelay > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(initialDelay):
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runCheck(entry, checkType)
		}
	}
}

func inferCheckType(entry *dto.ServiceEntry) dto.HealthCheckType {
	if entry.HealthCheck.Type != "" {
		return entry.HealthCheck.Type
	}
	if entry.HealthCheck.HTTP != "" {
		return dto.CheckHTTP
	}
	if entry.HealthCheck.TCP != "" {
		return dto.CheckTCP
	}
	if entry.HealthCheck.Script != "" {
		return dto.CheckScript
	}
	return dto.CheckTTL
}

func (m *HealthCheckManager) runCheck(entry *dto.ServiceEntry, checkType dto.HealthCheckType) {
	checker, ok := m.checkers[checkType]
	if !ok {
		m.logger.Warn("unknown check type", "type", checkType)
		return
	}

	status := checker.Check(entry)
	m.resultChan <- checkResult{
		serviceID: entry.ServiceID,
		status:    status,
	}
}

func (m *HealthCheckManager) processResults() {
	for result := range m.resultChan {
		m.registry.UpdateHealthStatus(result.serviceID, result.status)
	}
}

func (m *HealthCheckManager) StartAll(entries []*dto.ServiceEntry) {
	for _, entry := range entries {
		m.StartCheck(entry)
	}
}

func (m *HealthCheckManager) StopAll() {
	m.checksMu.Lock()
	defer m.checksMu.Unlock()

	for id, cancel := range m.checks {
		cancel()
		delete(m.checks, id)
	}
}
