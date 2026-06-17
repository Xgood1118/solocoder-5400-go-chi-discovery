package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/hashicorp/go-hclog"

	"discovery/internal/cluster"
	"discovery/internal/dto"
	"discovery/internal/health"
	"discovery/internal/registry"
)

type Handler struct {
	reg        *registry.Registry
	healthMgr  *health.HealthCheckManager
	clusterMgr *cluster.ClusterManager
	logger     hclog.Logger
}

func NewHandler(reg *registry.Registry, healthMgr *health.HealthCheckManager, clusterMgr *cluster.ClusterManager, logger hclog.Logger) *Handler {
	return &Handler{
		reg:        reg,
		healthMgr:  healthMgr,
		clusterMgr: clusterMgr,
		logger:     logger,
	}
}

func (h *Handler) RegisterService(w http.ResponseWriter, r *http.Request) {
	var req dto.ServiceRegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	entry, err := h.reg.Register(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.healthMgr.StartCheck(entry)

	h.logger.Info("service registered", "service", entry.ServiceName, "id", entry.ServiceID, "address", entry.Address, "port", entry.Port)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"service_id": entry.ServiceID,
	})
}

func (h *Handler) DeregisterService(w http.ResponseWriter, r *http.Request) {
	serviceID := chi.URLParam(r, "service_id")
	if serviceID == "" {
		http.Error(w, "service_id is required", http.StatusBadRequest)
		return
	}

	h.healthMgr.StopCheck(serviceID)

	if err := h.reg.Deregister(serviceID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	h.logger.Info("service deregistered", "id", serviceID)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	serviceID := chi.URLParam(r, "service_id")
	if serviceID == "" {
		http.Error(w, "service_id is required", http.StatusBadRequest)
		return
	}

	if err := h.reg.Heartbeat(serviceID); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) ListServices(w http.ResponseWriter, r *http.Request) {
	tagStr := r.URL.Query().Get("tag")
	excludeTagStr := r.URL.Query().Get("exclude_tag")
	useRegex, _ := strconv.ParseBool(r.URL.Query().Get("regex"))
	dc := r.URL.Query().Get("dc")
	allDC, _ := strconv.ParseBool(r.URL.Query().Get("all"))

	filter := registry.ParseTagQuery(tagStr, excludeTagStr, useRegex)

	var services map[string][]string

	if allDC || dc != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		services = h.clusterMgr.ListServices(ctx, filter, dc, allDC)
	} else {
		services = h.reg.ListServices(filter, dc, allDC)
	}

	resp := dto.ServicesResponse{Services: services}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) GetServiceInstances(w http.ResponseWriter, r *http.Request) {
	serviceName := chi.URLParam(r, "name")
	if serviceName == "" {
		http.Error(w, "service name is required", http.StatusBadRequest)
		return
	}

	watch, _ := strconv.ParseBool(r.URL.Query().Get("watch"))
	if watch {
		h.watchServiceInstances(w, r, serviceName)
		return
	}

	tagStr := r.URL.Query().Get("tag")
	excludeTagStr := r.URL.Query().Get("exclude_tag")
	useRegex, _ := strconv.ParseBool(r.URL.Query().Get("regex"))
	dc := r.URL.Query().Get("dc")
	allDC, _ := strconv.ParseBool(r.URL.Query().Get("all"))

	filter := registry.ParseTagQuery(tagStr, excludeTagStr, useRegex)

	var instances []dto.ServiceInstance

	if allDC || dc != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		instances = h.clusterMgr.ListServiceInstances(ctx, serviceName, filter, dc, allDC, true)
	} else {
		instances = h.reg.ListServiceInstances(serviceName, filter, dc, allDC, true)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(instances)
}

func (h *Handler) GetServiceHealth(w http.ResponseWriter, r *http.Request) {
	serviceName := chi.URLParam(r, "name")
	if serviceName == "" {
		http.Error(w, "service name is required", http.StatusBadRequest)
		return
	}

	dc := r.URL.Query().Get("dc")
	allDC, _ := strconv.ParseBool(r.URL.Query().Get("all"))

	var health dto.ServiceHealthResponse

	if allDC || dc != "" {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		health = h.clusterMgr.GetServiceHealth(ctx, serviceName, dc, allDC)
	} else {
		health = h.reg.GetServiceHealth(serviceName, dc, allDC)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}

func (h *Handler) watchServiceInstances(w http.ResponseWriter, r *http.Request, serviceName string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch, cleanup := h.reg.Watch(serviceName)
	defer cleanup()

	instances := h.reg.ListServiceInstances(serviceName, dto.TagFilter{}, "", true, false)
	initialEvent := dto.WatchEvent{
		Type:    "init",
		Service: serviceName,
		Data:    instances,
	}

	data, _ := json.Marshal(initialEvent)
	_, _ = w.Write([]byte("data: " + string(data) + "\n\n"))
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(event)
			_, _ = w.Write([]byte("data: " + string(data) + "\n\n"))
			flusher.Flush()
		}
	}
}

func (h *Handler) GetStatus(w http.ResponseWriter, r *http.Request) {
	entries := h.reg.GetAllEntries()
	total := len(entries)
	healthy := 0
	warning := 0
	critical := 0

	for _, entry := range entries {
		switch entry.Status {
		case dto.StatusPassing:
			healthy++
		case dto.StatusWarning:
			warning++
		case dto.StatusCritical:
			critical++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"total":    total,
		"healthy":  healthy,
		"warning":  warning,
		"critical": critical,
		"services": h.reg.ListServices(dto.TagFilter{}, "", true),
	})
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
