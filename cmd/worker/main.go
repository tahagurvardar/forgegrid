package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"forgegrid/internal/config"
	"forgegrid/internal/observability"
	"forgegrid/internal/worker"
	"net/http"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service.name", "forgegrid-worker"))
	shutdownTelemetry := observability.Init("forgegrid-worker")
	defer shutdownTelemetry()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	worker.Metrics = observability.NewWorkerMetrics()
	metricsServer := &http.Server{Addr: config.Env("METRICS_ADDR", ":9091"), Handler: worker.Metrics.Handler(), ReadHeaderTimeout: time.Second, WriteTimeout: 2 * time.Second}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("worker metrics listener unavailable; execution unaffected")
		}
	}()
	defer metricsServer.Close()
	if err := worker.Run(ctx, config.Env("CONTROL_PLANE_ADDR", "localhost:9090"), config.Env("WORKER_ID", "worker-a"), config.Duration("LEASE_RENEW_INTERVAL", 2*time.Second)); err != nil && ctx.Err() == nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
