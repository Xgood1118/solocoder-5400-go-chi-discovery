package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"discovery/internal/dto"
)

type Registry struct {
	mu          sync.RWMutex
	services    map[string]*dto.ServiceEntry
	snapshot    atomic.Value
	watchers    map[string]map[chan dto.WatchEvent]struct{}
	watcherMu   sync.RWMutex
	datacenter  string
	maxFailures int
}

func NewRegistry(datacenter string, maxFailures int) *Registry {
	r := &Registry{
		services:    make(map[string]*dto.ServiceEntry),
		watchers:    make(map[string]map[chan dto.WatchEvent]struct{}),
		datacenter:  datacenter,
		maxFailures: maxFailures,
	}
	r.updateSnapshot()
	return r
}

func (r *Registry) generateServiceID(req *dto.ServiceRegisterRequest) string {
	if req.ServiceID != "" {
		return req.ServiceID
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("%s-%s-%d-%d", req.ServiceName, req.Address, req.Port, time.Now().UnixNano())))
	return hex.EncodeToString(h[:])[:16]
}

func (r *Registry) Register(req *dto.ServiceRegisterRequest) (*dto.ServiceEntry, error) {
	if req.ServiceName == "" || req.Address == "" || req.Port <= 0 {
		return nil, fmt.Errorf("invalid request")
	}

	serviceID := r.generateServiceID(req)
	dc := req.Datacenter
	if dc == "" {
		dc = r.datacenter
	}

	ttl := req.TTL
	if ttl == 0 {
		ttl = req.HealthCheck.TTL
	}
	if ttl == 0 {
		ttl = 30 * time.Second
	}

	interval := req.CheckInterval
	if interval == 0 {
		interval = req.HealthCheck.Interval
	}
	if interval == 0 {
		interval = 10 * time.Second
	}

	entry := &dto.ServiceEntry{
		ServiceID:     serviceID,
		ServiceName:   req.ServiceName,
		Address:       req.Address,
		Port:          req.Port,
		Tags:          req.Tags,
		Datacenter:    dc,
		HealthCheck:   req.HealthCheck,
		Status:        dto.StatusPassing,
		LastHeartbeat: time.Now(),
		RegisteredAt:  time.Now(),
		TTL:           ttl,
		FailCount:     0,
	}

	if entry.HealthCheck.Interval == 0 {
		entry.HealthCheck.Interval = interval
	}
	if entry.HealthCheck.Timeout == 0 {
		entry.HealthCheck.Timeout = 5 * time.Second
	}

	r.mu.Lock()
	r.services[serviceID] = entry
	r.mu.Unlock()

	r.updateSnapshot()
	r.notifyWatchers(req.ServiceName, "register")

	return entry, nil
}

func (r *Registry) Deregister(serviceID string) error {
	r.mu.Lock()
	entry, exists := r.services[serviceID]
	if !exists {
		r.mu.Unlock()
		return fmt.Errorf("service not found")
	}
	delete(r.services, serviceID)
	r.mu.Unlock()

	r.updateSnapshot()
	r.notifyWatchers(entry.ServiceName, "deregister")

	return nil
}

func (r *Registry) Heartbeat(serviceID string) error {
	r.mu.Lock()
	entry, exists := r.services[serviceID]
	if !exists {
		r.mu.Unlock()
		return fmt.Errorf("service not found")
	}
	entry.LastHeartbeat = time.Now()
	entry.FailCount = 0
	if entry.Status != dto.StatusPassing {
		entry.Status = dto.StatusPassing
		r.mu.Unlock()
		r.updateSnapshot()
		r.notifyWatchers(entry.ServiceName, "update")
		return nil
	}
	r.mu.Unlock()
	return nil
}

func (r *Registry) GetService(serviceID string) (*dto.ServiceEntry, bool) {
	snapshot := r.snapshot.Load().(map[string]*dto.ServiceEntry)
	entry, ok := snapshot[serviceID]
	return entry, ok
}

func (r *Registry) ListServices(filter dto.TagFilter, datacenter string, allDC bool) map[string][]string {
	snapshot := r.snapshot.Load().(map[string]*dto.ServiceEntry)
	result := make(map[string][]string)

	for _, entry := range snapshot {
		if !allDC && datacenter != "" && entry.Datacenter != datacenter {
			continue
		}
		if !r.matchTags(entry.Tags, filter) {
			continue
		}
		if _, ok := result[entry.ServiceName]; !ok {
			result[entry.ServiceName] = make([]string, 0)
		}
		for _, tag := range entry.Tags {
			found := false
			for _, existing := range result[entry.ServiceName] {
				if existing == tag {
					found = true
					break
				}
			}
			if !found {
				result[entry.ServiceName] = append(result[entry.ServiceName], tag)
			}
		}
	}

	return result
}

func (r *Registry) ListServiceInstances(name string, filter dto.TagFilter, datacenter string, allDC bool, onlyHealthy bool) []dto.ServiceInstance {
	snapshot := r.snapshot.Load().(map[string]*dto.ServiceEntry)
	var instances []dto.ServiceInstance

	for _, entry := range snapshot {
		if entry.ServiceName != name {
			continue
		}
		if !allDC && datacenter != "" && entry.Datacenter != datacenter {
			continue
		}
		if !r.matchTags(entry.Tags, filter) {
			continue
		}
		if onlyHealthy && entry.Status != dto.StatusPassing {
			continue
		}
		instances = append(instances, dto.ServiceInstance{
			ServiceID:     entry.ServiceID,
			ServiceName:   entry.ServiceName,
			Address:       entry.Address,
			Port:          entry.Port,
			Tags:          entry.Tags,
			Datacenter:    entry.Datacenter,
			Status:        entry.Status,
			LastHeartbeat: entry.LastHeartbeat,
		})
	}

	return instances
}

func (r *Registry) GetServiceHealth(name string, datacenter string, allDC bool) dto.ServiceHealthResponse {
	instances := r.ListServiceInstances(name, dto.TagFilter{}, datacenter, allDC, false)

	healthy := 0
	warning := 0
	critical := 0

	for _, inst := range instances {
		switch inst.Status {
		case dto.StatusPassing:
			healthy++
		case dto.StatusWarning:
			warning++
		case dto.StatusCritical:
			critical++
		}
	}

	return dto.ServiceHealthResponse{
		ServiceName: name,
		Instances:   instances,
		Total:       len(instances),
		Healthy:     healthy,
		Warning:     warning,
		Critical:    critical,
	}
}

func (r *Registry) matchTags(tags []string, filter dto.TagFilter) bool {
	for _, include := range filter.IncludeTags {
		if !r.matchTag(tags, include, filter.UseRegex) {
			return false
		}
	}

	for _, exclude := range filter.ExcludeTags {
		if r.matchTag(tags, exclude, filter.UseRegex) {
			return false
		}
	}

	return true
}

func (r *Registry) matchTag(tags []string, pattern string, useRegex bool) bool {
	if useRegex {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return false
		}
		for _, tag := range tags {
			if re.MatchString(tag) {
				return true
			}
		}
		return false
	}

	for _, tag := range tags {
		if tag == pattern {
			return true
		}
	}
	return false
}

func (r *Registry) UpdateHealthStatus(serviceID string, status dto.ServiceStatus) {
	r.mu.Lock()
	entry, exists := r.services[serviceID]
	if !exists {
		r.mu.Unlock()
		return
	}

	oldStatus := entry.Status
	if status == dto.StatusPassing {
		entry.FailCount = 0
	} else {
		entry.FailCount++
	}
	entry.Status = status

	shouldDeregister := false
	if entry.FailCount > r.maxFailures {
		shouldDeregister = true
	}
	r.mu.Unlock()

	if oldStatus != status {
		r.updateSnapshot()
		r.notifyWatchers(entry.ServiceName, "update")
	}

	if shouldDeregister {
		_ = r.Deregister(serviceID)
	}
}

func (r *Registry) CleanupExpired() []string {
	var expired []string
	var servicesToNotify []string

	r.mu.Lock()
	now := time.Now()
	for id, entry := range r.services {
		if entry.TTL > 0 && now.Sub(entry.LastHeartbeat) > entry.TTL*3 {
			expired = append(expired, id)
			servicesToNotify = append(servicesToNotify, entry.ServiceName)
			delete(r.services, id)
		}
	}
	r.mu.Unlock()

	if len(expired) > 0 {
		r.updateSnapshot()
		for _, svc := range servicesToNotify {
			r.notifyWatchers(svc, "deregister")
		}
	}

	return expired
}

func (r *Registry) GetAllEntries() []*dto.ServiceEntry {
	snapshot := r.snapshot.Load().(map[string]*dto.ServiceEntry)
	var entries []*dto.ServiceEntry
	for _, entry := range snapshot {
		entries = append(entries, entry)
	}
	return entries
}

func (r *Registry) updateSnapshot() {
	r.mu.RLock()
	snapshot := make(map[string]*dto.ServiceEntry, len(r.services))
	for k, v := range r.services {
		entryCopy := *v
		tagsCopy := make([]string, len(v.Tags))
		copy(tagsCopy, v.Tags)
		entryCopy.Tags = tagsCopy
		snapshot[k] = &entryCopy
	}
	r.mu.RUnlock()
	r.snapshot.Store(snapshot)
}

func (r *Registry) Watch(serviceName string) (chan dto.WatchEvent, func()) {
	ch := make(chan dto.WatchEvent, 10)

	r.watcherMu.Lock()
	if _, ok := r.watchers[serviceName]; !ok {
		r.watchers[serviceName] = make(map[chan dto.WatchEvent]struct{})
	}
	r.watchers[serviceName][ch] = struct{}{}
	r.watcherMu.Unlock()

	cleanup := func() {
		r.watcherMu.Lock()
		if watchers, ok := r.watchers[serviceName]; ok {
			delete(watchers, ch)
			if len(watchers) == 0 {
				delete(r.watchers, serviceName)
			}
		}
		r.watcherMu.Unlock()
		close(ch)
	}

	return ch, cleanup
}

func (r *Registry) notifyWatchers(serviceName string, eventType string) {
	instances := r.ListServiceInstances(serviceName, dto.TagFilter{}, "", true, false)
	event := dto.WatchEvent{
		Type:    eventType,
		Service: serviceName,
		Data:    instances,
	}

	r.watcherMu.RLock()
	watchers, ok := r.watchers[serviceName]
	if !ok {
		r.watcherMu.RUnlock()
		return
	}

	channels := make([]chan dto.WatchEvent, 0, len(watchers))
	for ch := range watchers {
		channels = append(channels, ch)
	}
	r.watcherMu.RUnlock()

	for _, ch := range channels {
		select {
		case ch <- event:
		default:
		}
	}
}

func ParseTagQuery(tagStr string, excludeTagStr string, useRegex bool) dto.TagFilter {
	filter := dto.TagFilter{
		UseRegex: useRegex,
	}

	if tagStr != "" {
		filter.IncludeTags = splitAndTrim(tagStr)
	}
	if excludeTagStr != "" {
		filter.ExcludeTags = splitAndTrim(excludeTagStr)
	}

	return filter
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
