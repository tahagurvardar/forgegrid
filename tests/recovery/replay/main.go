package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	attempt := flag.String("attempt", "", "old attempt ID")
	session := flag.String("session", "", "old worker session ID")
	fence := flag.Int64("fence", 0, "old fencing token")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(config.Env("CONTROL_PLANE_ADDR", "localhost:9090"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	ack, err := pb.NewWorkerControlClient(conn).ReportResult(ctx, &pb.AttemptResult{Identity: &pb.AttemptIdentity{AttemptId: *attempt, WorkerSessionId: *session, FencingToken: *fence}, State: "SUCCEEDED", ExitCode: 0})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(ack); err != nil {
		os.Exit(1)
	}
	if ack.Accepted || ack.Error != "STALE_ATTEMPT" {
		fmt.Fprintln(os.Stderr, "expected STALE_ATTEMPT rejection")
		os.Exit(2)
	}
	fmt.Println("STALE_ATTEMPT_REJECTED")
}
