package dto

import (
	"time"
)

type HealthCheckType string

const (
	CheckHTTP   HealthCheckType = "http"
	CheckTCP    HealthCheckType = "tcp"
	CheckTTL    HealthCheckType = "ttl"
	CheckScript HealthCheckType = "script"
)

type ServiceStatus string

const (
	StatusPassing  ServiceStatus = "passing"
	StatusWarning  ServiceStatus = "warning"
	StatusCritical ServiceStatus = "critical"
)

type HealthCheckConfig struct {
	Type     HealthCheckType `json:"type"`
	HTTP     string          `json:"http,omitempty"`
	TCP      string          `json:"tcp,omitempty"`
	Script   string          `json:"script,omitempty"`
	Interval time.Duration   `json:"interval"`
	Timeout  time.Duration   `json:"timeout"`
	TTL      time.Duration   `json:"ttl,omitempty"`
}

type ServiceRegisterRequest struct {
	ServiceName  string            `json:"service_name" binding:"required"`
	ServiceID    string            `json:"service_id"`
	Address      string            `json:"address" binding:"required"`
	Port         int               `json:"port" binding:"required"`
	Tags         []string          `json:"tags"`
	Datacenter   string            `json:"datacenter"`
	HealthCheck  HealthCheckConfig `json:"health_check"`
	CheckInterval time.Duration    `json:"check_interval"`
	TTL          time.Duration     `json:"ttl"`
}

type ServiceEntry struct {
	ServiceID     string            `json:"service_id"`
	ServiceName   string            `json:"service_name"`
	Address       string            `json:"address"`
	Port          int               `json:"port"`
	Tags          []string          `json:"tags"`
	Datacenter    string            `json:"datacenter"`
	HealthCheck   HealthCheckConfig `json:"health_check"`
	Status        ServiceStatus     `json:"status"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	RegisteredAt  time.Time         `json:"registered_at"`
	TTL           time.Duration     `json:"ttl"`
	FailCount     int               `json:"-"`
}

type ServiceInstance struct {
	ServiceID     string        `json:"service_id"`
	ServiceName   string        `json:"service_name"`
	Address       string        `json:"address"`
	Port          int           `json:"port"`
	Tags          []string      `json:"tags"`
	Datacenter    string        `json:"datacenter"`
	Status        ServiceStatus `json:"status"`
	LastHeartbeat time.Time     `json:"last_heartbeat"`
}

type TagFilter struct {
	IncludeTags []string
	ExcludeTags []string
	UseRegex    bool
}

type ServiceHealthResponse struct {
	ServiceName string            `json:"service_name"`
	Instances   []ServiceInstance `json:"instances"`
	Total       int               `json:"total"`
	Healthy     int               `json:"healthy"`
	Warning     int               `json:"warning"`
	Critical    int               `json:"critical"`
}

type ServicesResponse struct {
	Services map[string][]string `json:"services"`
}

type WatchEvent struct {
	Type    string            `json:"type"`
	Service string            `json:"service"`
	Data    []ServiceInstance `json:"data"`
}

type DatacenterInfo struct {
	Name    string   `json:"name"`
	SeedList []string `json:"seed_list"`
}

type Config struct {
	NodeID      string           `mapstructure:"node_id"`
	Bind        string           `mapstructure:"bind"`
	DataDir     string           `mapstructure:"data_dir"`
	Datacenter  string           `mapstructure:"datacenter"`
	AuthToken   string           `mapstructure:"auth_token"`
	DefaultTTL  time.Duration    `mapstructure:"default_ttl"`
	MaxFailures int              `mapstructure:"max_failures"`
	Datacenters []DatacenterInfo `mapstructure:"datacenters"`
}
