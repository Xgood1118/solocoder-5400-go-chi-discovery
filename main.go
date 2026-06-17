package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"

	"discovery/internal/api"
	"discovery/internal/auth"
	"discovery/internal/cluster"
	"discovery/internal/dto"
	"discovery/internal/election"
	"discovery/internal/health"
	"discovery/internal/registry"
)

func main() {
	var (
		nodeID    = flag.String("node-id", "", "instance unique ID")
		bindAddr  = flag.String("bind", ":8109", "listen address")
		dataDir   = flag.String("data-dir", "./data", "data directory")
		dc        = flag.String("dc", "dc1", "datacenter name")
		config    = flag.String("config", "", "config file path")
		isPrimary = flag.Bool("primary", false, "participate in leader election")
	)
	flag.Parse()

	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "discovery",
		Level:  hclog.Info,
		Output: os.Stdout,
	})

	cfg := loadConfig(*config, logger)

	if *nodeID != "" {
		cfg.NodeID = *nodeID
	}
	if *bindAddr != "" {
		cfg.Bind = *bindAddr
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *dc != "" {
		cfg.Datacenter = *dc
	}
	if cfg.NodeID == "" {
		hostname, _ := os.Hostname()
		cfg.NodeID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}
	if cfg.DefaultTTL == 0 {
		cfg.DefaultTTL = 30 * time.Second
	}
	if cfg.MaxFailures == 0 {
		cfg.MaxFailures = 3
	}

	reg := registry.NewRegistry(cfg.Datacenter, cfg.MaxFailures)

	healthMgr := health.NewHealthCheckManager(reg, logger.Named("health"))

	clusterMgr := cluster.NewClusterManager(cfg.Datacenter, cfg.AuthToken, cfg.Datacenters)
	clusterMgr.SetLocalRegistry(reg)

	var leaderElection election.LeaderElection
	if *isPrimary {
		leaderElection = election.NewFileLockElection(cfg.NodeID, cfg.DataDir, 10, logger.Named("election"))
	} else {
		leaderElection = election.NewNoopElection()
	}

	if err := leaderElection.Start(context.Background()); err != nil {
		logger.Error("failed to start leader election", "error", err)
	}

	authenticator := auth.NewAuthenticator(cfg.AuthToken)

	handler := api.NewHandler(reg, healthMgr, clusterMgr, logger.Named("api"))

	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(chimw.Logger)
	r.Use(chimw.Recoverer)

	r.Get("/metrics", promhttp.Handler().ServeHTTP)
	r.Get("/status", handler.GetStatus)

	r.Route("/v1", func(r chi.Router) {
		r.Use(authenticator.WriteMiddleware)

		r.Route("/agent/service", func(r chi.Router) {
			r.Post("/register", handler.RegisterService)
			r.Post("/heartbeat/{service_id}", handler.Heartbeat)
			r.Post("/deregister/{service_id}", handler.DeregisterService)
		})

		r.Route("/catalog", func(r chi.Router) {
			r.Get("/services", handler.ListServices)
			r.Get("/service/{name}", handler.GetServiceInstances)
		})

		r.Route("/health", func(r chi.Router) {
			r.Get("/service/{name}", handler.GetServiceHealth)
		})
	})

	go runCleanupLoop(reg, logger)

	srv := &http.Server{
		Addr:         cfg.Bind,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("starting server", "bind", cfg.Bind, "dc", cfg.Datacenter, "node", cfg.NodeID)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	healthMgr.StopAll()
	leaderElection.Stop()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("server forced to shutdown", "error", err)
	}

	logger.Info("server exited")
}

func loadConfig(configPath string, logger hclog.Logger) dto.Config {
	v := viper.New()
	v.SetDefault("bind", ":8109")
	v.SetDefault("datacenter", "dc1")
	v.SetDefault("default_ttl", "30s")
	v.SetDefault("max_failures", 3)

	v.SetEnvPrefix("DISCOVERY")
	v.AutomaticEnv()

	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			logger.Warn("failed to read config file", "error", err)
		}
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/discovery")
		_ = v.ReadInConfig()
	}

	var cfg dto.Config
	if err := v.Unmarshal(&cfg); err != nil {
		logger.Error("failed to parse config", "error", err)
	}

	return cfg
}

func runCleanupLoop(reg *registry.Registry, logger hclog.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		expired := reg.CleanupExpired()
		if len(expired) > 0 {
			logger.Info("cleaned up expired services", "count", len(expired), "ids", expired)
		}
	}
}
