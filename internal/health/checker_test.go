package health

import (
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"discovery/internal/dto"
	"discovery/internal/registry"
)

func TestInferCheckType_HTTP(t *testing.T) {
	entry := &dto.ServiceEntry{
		ServiceID:   "test-1",
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		HealthCheck: dto.HealthCheckConfig{
			HTTP: "http://localhost:8000/health",
		},
		RegisteredAt: time.Now(),
	}

	checkType := inferCheckType(entry)
	if checkType != dto.CheckHTTP {
		t.Errorf("expected http check type, got %s", checkType)
	}
}

func TestInferCheckType_TCP(t *testing.T) {
	entry := &dto.ServiceEntry{
		ServiceID:   "test-1",
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		HealthCheck: dto.HealthCheckConfig{
			TCP: "localhost:8000",
		},
		RegisteredAt: time.Now(),
	}

	checkType := inferCheckType(entry)
	if checkType != dto.CheckTCP {
		t.Errorf("expected tcp check type, got %s", checkType)
	}
}

func TestInferCheckType_Script(t *testing.T) {
	entry := &dto.ServiceEntry{
		ServiceID:   "test-1",
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		HealthCheck: dto.HealthCheckConfig{
			Script: "echo ok",
		},
		RegisteredAt: time.Now(),
	}

	checkType := inferCheckType(entry)
	if checkType != dto.CheckScript {
		t.Errorf("expected script check type, got %s", checkType)
	}
}

func TestInferCheckType_ExplicitType(t *testing.T) {
	entry := &dto.ServiceEntry{
		ServiceID:   "test-1",
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		HealthCheck: dto.HealthCheckConfig{
			Type: dto.CheckHTTP,
			HTTP: "http://localhost:8000/health",
			TCP:  "localhost:8000",
		},
		RegisteredAt: time.Now(),
	}

	checkType := inferCheckType(entry)
	if checkType != dto.CheckHTTP {
		t.Errorf("expected http check type (explicit), got %s", checkType)
	}
}

func TestInferCheckType_DefaultTTL(t *testing.T) {
	entry := &dto.ServiceEntry{
		ServiceID:    "test-1",
		ServiceName:  "test-service",
		Address:      "127.0.0.1",
		Port:         8000,
		HealthCheck:  dto.HealthCheckConfig{},
		RegisteredAt: time.Now(),
	}

	checkType := inferCheckType(entry)
	if checkType != dto.CheckTTL {
		t.Errorf("expected ttl check type (default), got %s", checkType)
	}
}

func TestTTLChecker_Basic(t *testing.T) {
	checker := NewTTLChecker()

	entry := &dto.ServiceEntry{
		ServiceID:     "test-1",
		ServiceName:   "test-service",
		Address:       "127.0.0.1",
		Port:          8000,
		LastHeartbeat: time.Now(),
		TTL:           30 * time.Second,
		RegisteredAt:  time.Now(),
	}

	status := checker.Check(entry)
	if status != dto.StatusPassing {
		t.Errorf("expected passing status, got %s", status)
	}
}

func TestTTLChecker_Warning(t *testing.T) {
	checker := NewTTLChecker()

	entry := &dto.ServiceEntry{
		ServiceID:     "test-1",
		ServiceName:   "test-service",
		Address:       "127.0.0.1",
		Port:          8000,
		LastHeartbeat: time.Now().Add(-40 * time.Second),
		TTL:           30 * time.Second,
		RegisteredAt:  time.Now(),
	}

	status := checker.Check(entry)
	if status != dto.StatusWarning {
		t.Errorf("expected warning status, got %s", status)
	}
}

func TestTTLChecker_Critical(t *testing.T) {
	checker := NewTTLChecker()

	entry := &dto.ServiceEntry{
		ServiceID:     "test-1",
		ServiceName:   "test-service",
		Address:       "127.0.0.1",
		Port:          8000,
		LastHeartbeat: time.Now().Add(-70 * time.Second),
		TTL:           30 * time.Second,
		RegisteredAt:  time.Now(),
	}

	status := checker.Check(entry)
	if status != dto.StatusCritical {
		t.Errorf("expected critical status, got %s", status)
	}
}

func TestHealthCheckGracePeriod(t *testing.T) {
	reg := registry.NewRegistry("dc1", 3)
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "test",
		Level:  hclog.Error,
		Output: nil,
	})
	mgr := NewHealthCheckManager(reg, logger)
	defer mgr.StopAll()

	req := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		HealthCheck: dto.HealthCheckConfig{
			Type:     dto.CheckHTTP,
			HTTP:     "http://127.0.0.1:9999/nonexistent",
			Interval: dto.Duration(100 * time.Millisecond),
			Timeout:  dto.Duration(50 * time.Millisecond),
		},
		TTL: dto.Duration(30 * time.Second),
	}
	entry, err := reg.Register(req)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	mgr.StartCheck(entry)

	time.Sleep(250 * time.Millisecond)

	e, ok := reg.GetService(entry.ServiceID)
	if !ok {
		t.Fatal("service should still exist during grace period")
	}
	if e.Status != dto.StatusPassing {
		t.Errorf("service should still be passing during grace period, got %s", e.Status)
	}
	if e.FailCount > 0 {
		t.Errorf("fail count should be 0 during grace period, got %d", e.FailCount)
	}
}

func TestHTTPChecker_EmptyURL(t *testing.T) {
	checker := NewHTTPChecker()

	entry := &dto.ServiceEntry{
		ServiceID:    "test-1",
		ServiceName:  "test-service",
		Address:      "127.0.0.1",
		Port:         8000,
		HealthCheck:  dto.HealthCheckConfig{},
		RegisteredAt: time.Now(),
	}

	status := checker.Check(entry)
	if status != dto.StatusPassing {
		t.Errorf("expected passing status for empty HTTP config, got %s", status)
	}
}

func TestTCPChecker_ConnectionRefused(t *testing.T) {
	checker := NewTCPChecker()

	entry := &dto.ServiceEntry{
		ServiceID:   "test-1",
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        19999,
		HealthCheck: dto.HealthCheckConfig{
			Type:    dto.CheckTCP,
			Timeout: dto.Duration(100 * time.Millisecond),
		},
		RegisteredAt: time.Now(),
	}

	status := checker.Check(entry)
	if status != dto.StatusCritical {
		t.Errorf("expected critical status for refused connection, got %s", status)
	}
}

func TestScriptChecker_EmptyScript(t *testing.T) {
	checker := NewScriptChecker()

	entry := &dto.ServiceEntry{
		ServiceID:    "test-1",
		ServiceName:  "test-service",
		Address:      "127.0.0.1",
		Port:         8000,
		HealthCheck:  dto.HealthCheckConfig{},
		RegisteredAt: time.Now(),
	}

	status := checker.Check(entry)
	if status != dto.StatusPassing {
		t.Errorf("expected passing status for empty script, got %s", status)
	}
}
