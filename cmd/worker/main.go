package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"forgegrid/internal/config"
	"forgegrid/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := worker.Run(ctx, config.Env("CONTROL_PLANE_ADDR", "localhost:9090"), config.Env("WORKER_ID", "worker-a"), config.Duration("LEASE_RENEW_INTERVAL", 2*time.Second)); err != nil && ctx.Err() == nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
