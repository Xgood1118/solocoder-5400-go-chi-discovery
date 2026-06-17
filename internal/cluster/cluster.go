package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"discovery/internal/dto"
	"discovery/internal/registry"
)

type RemoteRegistry interface {
	ListServiceInstances(ctx context.Context, name string, filter dto.TagFilter, onlyHealthy bool) ([]dto.ServiceInstance, error)
	GetServiceHealth(ctx context.Context, name string) (dto.ServiceHealthResponse, error)
	ListServices(ctx context.Context, filter dto.TagFilter) (map[string][]string, error)
}

type LocalRegistry struct {
	reg *registry.Registry
}

func NewLocalRegistry(reg *registry.Registry) *LocalRegistry {
	return &LocalRegistry{reg: reg}
}

func (l *LocalRegistry) ListServiceInstances(ctx context.Context, name string, filter dto.TagFilter, onlyHealthy bool) ([]dto.ServiceInstance, error) {
	return l.reg.ListServiceInstances(name, filter, "", false, onlyHealthy), nil
}

func (l *LocalRegistry) GetServiceHealth(ctx context.Context, name string) (dto.ServiceHealthResponse, error) {
	return l.reg.GetServiceHealth(name, "", false), nil
}

func (l *LocalRegistry) ListServices(ctx context.Context, filter dto.TagFilter) (map[string][]string, error) {
	return l.reg.ListServices(filter, "", false), nil
}

type RemoteHTTPRegistry struct {
	baseURL string
	client  *http.Client
	token   string
}

func NewRemoteHTTPRegistry(baseURL string, token string) *RemoteHTTPRegistry {
	return &RemoteHTTPRegistry{
		baseURL: baseURL,
		client:  &http.Client{},
		token:   token,
	}
}

func (r *RemoteHTTPRegistry) ListServiceInstances(ctx context.Context, name string, filter dto.TagFilter, onlyHealthy bool) ([]dto.ServiceInstance, error) {
	url := fmt.Sprintf("%s/v1/catalog/service/%s", r.baseURL, name)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	if r.token != "" {
		req.Header.Set("X-Registry-Token", r.token)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote registry returned status %d", resp.StatusCode)
	}

	var instances []dto.ServiceInstance
	if err := json.NewDecoder(resp.Body).Decode(&instances); err != nil {
		return nil, err
	}

	return instances, nil
}

func (r *RemoteHTTPRegistry) GetServiceHealth(ctx context.Context, name string) (dto.ServiceHealthResponse, error) {
	url := fmt.Sprintf("%s/v1/health/service/%s", r.baseURL, name)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return dto.ServiceHealthResponse{}, err
	}

	if r.token != "" {
		req.Header.Set("X-Registry-Token", r.token)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return dto.ServiceHealthResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return dto.ServiceHealthResponse{}, fmt.Errorf("remote registry returned status %d", resp.StatusCode)
	}

	var health dto.ServiceHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return dto.ServiceHealthResponse{}, err
	}

	return health, nil
}

func (r *RemoteHTTPRegistry) ListServices(ctx context.Context, filter dto.TagFilter) (map[string][]string, error) {
	url := fmt.Sprintf("%s/v1/catalog/services", r.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	if r.token != "" {
		req.Header.Set("X-Registry-Token", r.token)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote registry returned status %d", resp.StatusCode)
	}

	var result dto.ServicesResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.Services, nil
}

type ClusterManager struct {
	datacenters map[string]RemoteRegistry
	localDC     string
	token       string
}

func NewClusterManager(localDC string, token string, dcInfo []dto.DatacenterInfo) *ClusterManager {
	cm := &ClusterManager{
		datacenters: make(map[string]RemoteRegistry),
		localDC:     localDC,
		token:       token,
	}

	for _, info := range dcInfo {
		if info.Name == localDC {
			continue
		}
		if len(info.SeedList) > 0 {
			cm.datacenters[info.Name] = NewRemoteHTTPRegistry(info.SeedList[0], token)
		}
	}

	return cm
}

func (cm *ClusterManager) SetLocalRegistry(reg *registry.Registry) {
	cm.datacenters[cm.localDC] = NewLocalRegistry(reg)
}

func (cm *ClusterManager) GetDatacenter(name string) (RemoteRegistry, bool) {
	dc, ok := cm.datacenters[name]
	return dc, ok
}

func (cm *ClusterManager) ListServiceInstances(ctx context.Context, name string, filter dto.TagFilter, datacenter string, allDC bool, onlyHealthy bool) []dto.ServiceInstance {
	var result []dto.ServiceInstance
	var mu sync.Mutex
	var wg sync.WaitGroup

	dcs := cm.getDatacentersToQuery(datacenter, allDC)

	for dcName, dc := range dcs {
		wg.Add(1)
		go func(dcName string, dc RemoteRegistry) {
			defer wg.Done()

			instances, err := dc.ListServiceInstances(ctx, name, filter, onlyHealthy)
			if err != nil {
				return
			}

			mu.Lock()
			for _, inst := range instances {
				if inst.Datacenter == "" {
					inst.Datacenter = dcName
				}
				result = append(result, inst)
			}
			mu.Unlock()
		}(dcName, dc)
	}

	wg.Wait()
	return result
}

func (cm *ClusterManager) GetServiceHealth(ctx context.Context, name string, datacenter string, allDC bool) dto.ServiceHealthResponse {
	health := dto.ServiceHealthResponse{
		ServiceName: name,
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	dcs := cm.getDatacentersToQuery(datacenter, allDC)

	for dcName, dc := range dcs {
		wg.Add(1)
		go func(dcName string, dc RemoteRegistry) {
			defer wg.Done()

			h, err := dc.GetServiceHealth(ctx, name)
			if err != nil {
				return
			}

			mu.Lock()
			health.Total += h.Total
			health.Healthy += h.Healthy
			health.Warning += h.Warning
			health.Critical += h.Critical
			for _, inst := range h.Instances {
				if inst.Datacenter == "" {
					inst.Datacenter = dcName
				}
				health.Instances = append(health.Instances, inst)
			}
			mu.Unlock()
		}(dcName, dc)
	}

	wg.Wait()
	return health
}

func (cm *ClusterManager) ListServices(ctx context.Context, filter dto.TagFilter, datacenter string, allDC bool) map[string][]string {
	result := make(map[string][]string)
	var mu sync.Mutex
	var wg sync.WaitGroup

	dcs := cm.getDatacentersToQuery(datacenter, allDC)

	for dcName, dc := range dcs {
		wg.Add(1)
		go func(dcName string, dc RemoteRegistry) {
			defer wg.Done()

			services, err := dc.ListServices(ctx, filter)
			if err != nil {
				return
			}

			mu.Lock()
			for svc, tags := range services {
				if _, ok := result[svc]; !ok {
					result[svc] = make([]string, 0)
				}
				for _, tag := range tags {
					found := false
					for _, existing := range result[svc] {
						if existing == tag {
							found = true
							break
						}
					}
					if !found {
						result[svc] = append(result[svc], tag)
					}
				}
			}
			mu.Unlock()
		}(dcName, dc)
	}

	wg.Wait()
	return result
}

func (cm *ClusterManager) getDatacentersToQuery(datacenter string, allDC bool) map[string]RemoteRegistry {
	dcs := make(map[string]RemoteRegistry)

	if allDC {
		for name, dc := range cm.datacenters {
			dcs[name] = dc
		}
	} else if datacenter != "" {
		if dc, ok := cm.datacenters[datacenter]; ok {
			dcs[datacenter] = dc
		}
	} else {
		if dc, ok := cm.datacenters[cm.localDC]; ok {
			dcs[cm.localDC] = dc
		}
	}

	return dcs
}
