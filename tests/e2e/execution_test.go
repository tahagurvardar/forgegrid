//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/domain"
	"forgegrid/internal/store/postgres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func baseURL() string {
	if v := os.Getenv("TEST_HTTP_URL"); v != "" {
		return v
	}
	return "http://localhost:8080"
}
func get(t *testing.T, path string, out any) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	r, err := client.Get(baseURL() + path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("GET %s status %d", path, r.StatusCode)
	}
	if err = json.NewDecoder(r.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}
func submit(t *testing.T, argv []string, timeout int) string {
	t.Helper()
	payload, err := json.Marshal(domain.Spec{Image: "alpine:3.22", Command: argv, TimeoutSeconds: timeout, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	r, err := client.Post(baseURL()+"/api/v1/jobs", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 201 {
		b, _ := io.ReadAll(r.Body)
		t.Fatalf("submit status=%d body=%s", r.StatusCode, b)
	}
	var j struct {
		ID string `json:"id"`
	}
	if err = json.NewDecoder(r.Body).Decode(&j); err != nil {
		t.Fatal(err)
	}
	return j.ID
}
func await(t *testing.T, id string) domain.Job {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		var j domain.Job
		get(t, "/api/v1/jobs/"+id, &j)
		if j.State == "SUCCEEDED" || j.State == "FAILED" {
			return j
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("job never became terminal")
	return domain.Job{}
}
func grpcConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	addr := os.Getenv("TEST_GRPC_ADDR")
	if addr == "" {
		addr = "localhost:9090"
	}
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func TestDockerExecutionAndProtocolIdempotency(t *testing.T) {
	id := submit(t, []string{"/bin/sh", "-c", "printf 'stdout-proof'; printf 'stderr-proof' >&2"}, 30)
	j := await(t, id)
	if j.State != "SUCCEEDED" || len(j.Attempts) != 1 || j.Attempts[0].State != "SUCCEEDED" {
		t.Fatalf("unexpected job %+v", j)
	}
	a := j.Attempts[0]
	var logs []postgres.LogChunk
	get(t, "/api/v1/attempts/"+a.AttemptID+"/logs", &logs)
	stdout, stderr := "", ""
	last := int64(0)
	for _, c := range logs {
		if c.Sequence <= last || len(c.Payload) > 16384 {
			t.Fatal("invalid log sequencing/chunk size")
		}
		last = c.Sequence
		if c.Stream == "STDOUT" {
			stdout += string(c.Payload)
		} else if c.Stream == "STDERR" {
			stderr += string(c.Payload)
		}
	}
	if !strings.Contains(stdout, "stdout-proof") || !strings.Contains(stderr, "stderr-proof") {
		t.Fatalf("stdout=%q stderr=%q", stdout, stderr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn := grpcConn(t)
	control := pb.NewWorkerControlClient(conn)
	identity := &pb.AttemptIdentity{AttemptId: a.AttemptID, WorkerSessionId: a.SessionID, FencingToken: a.FencingToken}
	result := &pb.AttemptResult{Identity: identity, State: "SUCCEEDED", ExitCode: 0}
	ack, err := control.ReportResult(ctx, result)
	if err != nil || !ack.Accepted || !ack.Duplicate {
		t.Fatalf("duplicate ACK: %v %v", ack, err)
	}
	identity.FencingToken++
	ack, err = control.ReportResult(ctx, result)
	if err != nil || ack.Accepted || ack.Error != "STALE_ATTEMPT" {
		t.Fatalf("stale fencing ACK: %v %v", ack, err)
	}
	identity.FencingToken--
	stream, err := pb.NewWorkerLogsClient(conn).Stream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.CloseSend()
	// Deliberately resend an existing sequence with different bytes; original must remain.
	first := logs[0]
	if err = stream.Send(&pb.LogChunk{Identity: identity, Sequence: first.Sequence, Stream: first.Stream, Payload: []byte("duplicate")}); err != nil {
		t.Fatal(err)
	}
	logAck, err := stream.Recv()
	if err != nil || logAck.Sequence != first.Sequence {
		t.Fatalf("log duplicate ACK: %v %v", logAck, err)
	}
	var after []postgres.LogChunk
	get(t, "/api/v1/attempts/"+a.AttemptID+"/logs", &after)
	if len(after) != len(logs) || !bytes.Equal(after[0].Payload, first.Payload) {
		t.Fatal("duplicate log altered persisted stream")
	}
}
func TestDockerWorkloadFailuresAreNotRetried(t *testing.T) {
	for _, c := range []struct {
		name        string
		argv        []string
		timeout     int
		state, kind string
		exit        int32
	}{
		{"nonzero", []string{"/bin/sh", "-c", "exit 7"}, 30, "FAILED", "EXIT_NON_ZERO", 7},
		{"invalid-command", []string{"/forgegrid-command-does-not-exist"}, 30, "FAILED", "INVALID_COMMAND", 127},
		{"timeout", []string{"sleep", "30"}, 1, "TIMED_OUT", "JOB_TIMEOUT", -1},
	} {
		t.Run(c.name, func(t *testing.T) {
			j := await(t, submit(t, c.argv, c.timeout))
			if j.State != "FAILED" || j.AttemptCount != 1 || len(j.Attempts) != 1 {
				t.Fatalf("workload failure retried: %+v", j)
			}
			a := j.Attempts[0]
			if a.State != c.state || a.FailureKind != c.kind || a.ExitCode == nil || *a.ExitCode != c.exit {
				t.Fatal(fmt.Sprintf("bad result: %+v", a))
			}
		})
	}
}
