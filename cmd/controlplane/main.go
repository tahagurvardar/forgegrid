package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/config"
	"forgegrid/internal/controlplane"
	"forgegrid/internal/store/postgres"
	"google.golang.org/grpc"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, stop := context.WithTimeout(ctx, 30*time.Second)
	store, err := postgres.Open(startup, config.Env("DATABASE_URL", "postgres://forgegrid:forgegrid@localhost:5432/forgegrid?sslmode=disable"), config.Duration("EXECUTION_LEASE", 10*time.Second), config.Duration("OFFLINE_THRESHOLD", 6*time.Second), config.Duration("RETRY_BASE", time.Second))
	if err != nil {
		slog.Error("database startup failed", "error", err)
		os.Exit(1)
	}
	defer store.Pool.Close()
	if err = store.Migrate(startup); err != nil {
		slog.Error("migration failed", "error", err)
		os.Exit(1)
	}
	if err = store.ResetConnections(startup); err != nil {
		slog.Error("connection reset failed", "error", err)
		os.Exit(1)
	}
	stop()
	server := controlplane.New(store, config.Duration("HEARTBEAT_INTERVAL", 2*time.Second))
	grpcServer := grpc.NewServer(grpc.MaxRecvMsgSize(256 * 1024))
	pb.RegisterWorkerControlServer(grpcServer, server)
	pb.RegisterWorkerLogsServer(grpcServer, server)
	listener, err := net.Listen("tcp", config.Env("GRPC_ADDR", ":9090"))
	if err != nil {
		slog.Error("gRPC listen failed", "error", err)
		os.Exit(1)
	}
	httpServer := &http.Server{Addr: config.Env("HTTP_ADDR", ":8080"), Handler: server.HTTP(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	errors := make(chan error, 2)
	go func() { errors <- grpcServer.Serve(listener) }()
	go func() { errors <- httpServer.ListenAndServe() }()
	go server.Run(ctx)
	slog.Info("control plane ready", "coordination", store.String())
	select {
	case <-ctx.Done():
	case err = <-errors:
		slog.Error("server stopped", "error", err)
		cancel()
	}
	grpcServer.Stop()
	shutdown, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	if err = httpServer.Shutdown(shutdown); err != nil {
		slog.Warn("HTTP shutdown", "error", err)
	}
}
