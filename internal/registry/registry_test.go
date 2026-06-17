package registry

import (
	"sync"
	"testing"
	"time"

	"discovery/internal/dto"
)

func TestRegisterConcurrent(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	serviceIDs := make([]string, numGoroutines)
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			req := &dto.ServiceRegisterRequest{
				ServiceName: "test-service",
				Address:     "127.0.0.1",
				Port:        8000 + idx,
				Tags:        []string{"env=test", "version=v1"},
				TTL:         10 * time.Second,
			}
			entry, err := reg.Register(req)
			if err != nil {
				t.Errorf("register failed: %v", err)
				return
			}
			mu.Lock()
			serviceIDs[idx] = entry.ServiceID
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	for i := 0; i < numGoroutines; i++ {
		if serviceIDs[i] == "" {
			t.Errorf("service %d not registered", i)
		}
	}

	entries := reg.GetAllEntries()
	if len(entries) != numGoroutines {
		t.Errorf("expected %d entries, got %d", numGoroutines, len(entries))
	}
}

func TestDeregisterConcurrent(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	const numGoroutines = 100
	serviceIDs := make([]string, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		req := &dto.ServiceRegisterRequest{
			ServiceName: "test-service",
			Address:     "127.0.0.1",
			Port:        8000 + i,
			TTL:         10 * time.Second,
		}
		entry, _ := reg.Register(req)
		serviceIDs[i] = entry.ServiceID
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id string) {
			defer wg.Done()
			err := reg.Deregister(id)
			if err != nil {
				t.Errorf("deregister failed: %v", err)
			}
		}(serviceIDs[i])
	}

	wg.Wait()

	entries := reg.GetAllEntries()
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestHeartbeatTimeoutCleanup(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	req := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		TTL:         1 * time.Second,
	}
	entry, err := reg.Register(req)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	reg.mu.Lock()
	reg.services[entry.ServiceID].LastHeartbeat = time.Now().Add(-5 * time.Second)
	reg.mu.Unlock()
	reg.updateSnapshot()

	expired := reg.CleanupExpired()
	if len(expired) != 1 {
		t.Errorf("expected 1 expired service, got %d", len(expired))
	}

	if expired[0] != entry.ServiceID {
		t.Errorf("expected service %s to be expired, got %s", entry.ServiceID, expired[0])
	}

	entries := reg.GetAllEntries()
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after cleanup, got %d", len(entries))
	}
}

func TestHeartbeatKeepsService(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	req := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		TTL:         500 * time.Millisecond,
	}
	entry, _ := reg.Register(req)

	for i := 0; i < 5; i++ {
		time.Sleep(200 * time.Millisecond)
		err := reg.Heartbeat(entry.ServiceID)
		if err != nil {
			t.Fatalf("heartbeat failed: %v", err)
		}
	}

	expired := reg.CleanupExpired()
	if len(expired) != 0 {
		t.Errorf("expected 0 expired services, got %d", len(expired))
	}

	entries := reg.GetAllEntries()
	if len(entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(entries))
	}
}

func TestTagFilterExact(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	entries := []struct {
		name string
		tags []string
		port int
	}{
		{"order-service", []string{"env=prod", "version=v1"}, 8000},
		{"order-service", []string{"env=prod", "version=v2"}, 8001},
		{"order-service", []string{"env=staging", "version=v1"}, 8002},
		{"order-service", []string{"env=prod", "version=v1", "tier=backend"}, 8003},
		{"payment-service", []string{"env=prod", "version=v1"}, 9000},
	}

	for _, e := range entries {
		req := &dto.ServiceRegisterRequest{
			ServiceName: e.name,
			Address:     "127.0.0.1",
			Port:        e.port,
			Tags:        e.tags,
			TTL:         30 * time.Second,
		}
		_, _ = reg.Register(req)
	}

	filter := ParseTagQuery("env=prod", "", false)
	instances := reg.ListServiceInstances("order-service", filter, "", false, false)
	if len(instances) != 3 {
		t.Errorf("expected 3 instances with env=prod, got %d", len(instances))
	}

	filter = ParseTagQuery("env=prod,version=v1", "", false)
	instances = reg.ListServiceInstances("order-service", filter, "", false, false)
	if len(instances) != 2 {
		t.Errorf("expected 2 instances with env=prod and version=v1, got %d", len(instances))
	}

	filter = ParseTagQuery("", "version=v2", false)
	instances = reg.ListServiceInstances("order-service", filter, "", false, false)
	if len(instances) != 3 {
		t.Errorf("expected 3 instances without version=v2, got %d", len(instances))
	}

	filter = ParseTagQuery("env=prod", "tier=backend", false)
	instances = reg.ListServiceInstances("order-service", filter, "", false, false)
	if len(instances) != 2 {
		t.Errorf("expected 2 instances with env=prod and without tier=backend, got %d", len(instances))
	}
}

func TestTagFilterRegex(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	entries := []struct {
		name string
		tags []string
		port int
	}{
		{"order-service", []string{"env=prod", "version=v1.0.0"}, 8000},
		{"order-service", []string{"env=prod", "version=v1.2.3"}, 8001},
		{"order-service", []string{"env=prod", "version=v2.0.0"}, 8002},
		{"order-service", []string{"env=staging", "version=v1.0.0"}, 8003},
	}

	for _, e := range entries {
		req := &dto.ServiceRegisterRequest{
			ServiceName: e.name,
			Address:     "127.0.0.1",
			Port:        e.port,
			Tags:        e.tags,
			TTL:         30 * time.Second,
		}
		_, _ = reg.Register(req)
	}

	filter := ParseTagQuery("version=v1\\..*", "", true)
	instances := reg.ListServiceInstances("order-service", filter, "", false, false)
	if len(instances) != 3 {
		t.Errorf("expected 3 instances with version matching v1.*, got %d", len(instances))
	}

	filter = ParseTagQuery("version=v2\\..*", "", true)
	instances = reg.ListServiceInstances("order-service", filter, "", false, false)
	if len(instances) != 1 {
		t.Errorf("expected 1 instance with version matching v2.*, got %d", len(instances))
	}
}

func TestWatchEvents(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	ch, cleanup := reg.Watch("order-service")
	defer cleanup()

	time.Sleep(50 * time.Millisecond)

	req1 := &dto.ServiceRegisterRequest{
		ServiceName: "order-service",
		Address:     "127.0.0.1",
		Port:        8000,
		Tags:        []string{"env=prod"},
		TTL:         30 * time.Second,
	}
	entry1, _ := reg.Register(req1)

	select {
	case event := <-ch:
		if event.Type != "register" {
			t.Errorf("expected register event, got %s", event.Type)
		}
		if event.Service != "order-service" {
			t.Errorf("expected service order-service, got %s", event.Service)
		}
		if len(event.Data) != 1 {
			t.Errorf("expected 1 instance in event data, got %d", len(event.Data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for register event")
	}

	req2 := &dto.ServiceRegisterRequest{
		ServiceName: "order-service",
		Address:     "127.0.0.1",
		Port:        8001,
		Tags:        []string{"env=prod"},
		TTL:         30 * time.Second,
	}
	entry2, _ := reg.Register(req2)

	select {
	case event := <-ch:
		if event.Type != "register" {
			t.Errorf("expected register event, got %s", event.Type)
		}
		if len(event.Data) != 2 {
			t.Errorf("expected 2 instances in event data, got %d", len(event.Data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second register event")
	}

	_ = reg.Deregister(entry1.ServiceID)

	select {
	case event := <-ch:
		if event.Type != "deregister" {
			t.Errorf("expected deregister event, got %s", event.Type)
		}
		if len(event.Data) != 1 {
			t.Errorf("expected 1 instance in event data after deregister, got %d", len(event.Data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for deregister event")
	}

	reg.UpdateHealthStatus(entry2.ServiceID, dto.StatusCritical)

	select {
	case event := <-ch:
		if event.Type != "update" {
			t.Errorf("expected update event, got %s", event.Type)
		}
		if event.Data[0].Status != dto.StatusCritical {
			t.Errorf("expected status critical, got %s", event.Data[0].Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for update event")
	}
}

func TestWatchMultipleClients(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	ch1, cleanup1 := reg.Watch("order-service")
	defer cleanup1()

	ch2, cleanup2 := reg.Watch("order-service")
	defer cleanup2()

	time.Sleep(50 * time.Millisecond)

	req := &dto.ServiceRegisterRequest{
		ServiceName: "order-service",
		Address:     "127.0.0.1",
		Port:        8000,
		TTL:         30 * time.Second,
	}
	_, _ = reg.Register(req)

	timeout := time.After(2 * time.Second)
	received1 := false
	received2 := false

	for !received1 || !received2 {
		select {
		case <-ch1:
			received1 = true
		case <-ch2:
			received2 = true
		case <-timeout:
			t.Fatalf("timeout: received1=%v, received2=%v", received1, received2)
		}
	}
}

func TestHealthStatusUpdate(t *testing.T) {
	reg := NewRegistry("dc1", 2)

	req := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		TTL:         30 * time.Second,
	}
	entry, _ := reg.Register(req)

	reg.UpdateHealthStatus(entry.ServiceID, dto.StatusWarning)
	e, _ := reg.GetService(entry.ServiceID)
	if e.Status != dto.StatusWarning {
		t.Errorf("expected status warning, got %s", e.Status)
	}
	if e.FailCount != 1 {
		t.Errorf("expected fail count 1, got %d", e.FailCount)
	}

	reg.UpdateHealthStatus(entry.ServiceID, dto.StatusCritical)
	e, _ = reg.GetService(entry.ServiceID)
	if e.Status != dto.StatusCritical {
		t.Errorf("expected status critical, got %s", e.Status)
	}
	if e.FailCount != 2 {
		t.Errorf("expected fail count 2, got %d", e.FailCount)
	}

	reg.UpdateHealthStatus(entry.ServiceID, dto.StatusCritical)

	_, exists := reg.GetService(entry.ServiceID)
	if exists {
		t.Error("service should have been deregistered after max failures")
	}
}

func TestListServices(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	services := []struct {
		name string
		tags []string
		port int
	}{
		{"order-service", []string{"env=prod", "version=v1"}, 8000},
		{"order-service", []string{"env=prod", "version=v2"}, 8001},
		{"payment-service", []string{"env=prod", "version=v1"}, 9000},
		{"user-service", []string{"env=staging", "version=v1"}, 10000},
	}

	for _, s := range services {
		req := &dto.ServiceRegisterRequest{
			ServiceName: s.name,
			Address:     "127.0.0.1",
			Port:        s.port,
			Tags:        s.tags,
			TTL:         30 * time.Second,
		}
		_, _ = reg.Register(req)
	}

	result := reg.ListServices(dto.TagFilter{}, "", false)
	if len(result) != 3 {
		t.Errorf("expected 3 services, got %d", len(result))
	}

	filter := ParseTagQuery("env=prod", "", false)
	result = reg.ListServices(filter, "", false)
	if len(result) != 2 {
		t.Errorf("expected 2 services with env=prod, got %d", len(result))
	}

	if _, ok := result["order-service"]; !ok {
		t.Error("order-service should be in result")
	}
	if _, ok := result["payment-service"]; !ok {
		t.Error("payment-service should be in result")
	}
}

func TestGetServiceHealth(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	req1 := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		TTL:         30 * time.Second,
	}
	entry1, _ := reg.Register(req1)

	req2 := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8001,
		TTL:         30 * time.Second,
	}
	entry2, _ := reg.Register(req2)

	req3 := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8002,
		TTL:         30 * time.Second,
	}
	entry3, _ := reg.Register(req3)

	reg.UpdateHealthStatus(entry2.ServiceID, dto.StatusWarning)
	reg.UpdateHealthStatus(entry3.ServiceID, dto.StatusCritical)

	health := reg.GetServiceHealth("test-service", "", false)
	if health.Total != 3 {
		t.Errorf("expected total 3, got %d", health.Total)
	}
	if health.Healthy != 1 {
		t.Errorf("expected 1 healthy, got %d", health.Healthy)
	}
	if health.Warning != 1 {
		t.Errorf("expected 1 warning, got %d", health.Warning)
	}
	if health.Critical != 1 {
		t.Errorf("expected 1 critical, got %d", health.Critical)
	}

	_ = entry1
}

func TestDatacenterFiltering(t *testing.T) {
	reg := NewRegistry("dc1", 3)

	req1 := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.1",
		Port:        8000,
		Datacenter:  "dc1",
		TTL:         30 * time.Second,
	}
	_, _ = reg.Register(req1)

	req2 := &dto.ServiceRegisterRequest{
		ServiceName: "test-service",
		Address:     "127.0.0.2",
		Port:        8000,
		Datacenter:  "dc2",
		TTL:         30 * time.Second,
	}
	_, _ = reg.Register(req2)

	instances := reg.ListServiceInstances("test-service", dto.TagFilter{}, "dc1", false, false)
	if len(instances) != 1 {
		t.Errorf("expected 1 instance in dc1, got %d", len(instances))
	}
	if instances[0].Address != "127.0.0.1" {
		t.Errorf("expected address 127.0.0.1, got %s", instances[0].Address)
	}

	instances = reg.ListServiceInstances("test-service", dto.TagFilter{}, "", true, false)
	if len(instances) != 2 {
		t.Errorf("expected 2 instances across all datacenters, got %d", len(instances))
	}
}
