//go:build integration

package controlplane

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/domain"
	"forgegrid/internal/store/postgres"
	"forgegrid/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// Exercise Connect itself with a deterministic transport failure; all
// registration, scheduling, ownership, and recovery use real PostgreSQL.
type failedStream struct {
	grpc.ServerStream
	ctx     context.Context
	in      chan *pb.WorkerMessage
	out     chan *pb.ControlMessage
	failRun bool
	blocked chan struct{}
	failed  chan struct{}
}

func (f *failedStream) Context() context.Context { return f.ctx }
func (f *failedStream) Recv() (*pb.WorkerMessage, error) {
	select {
	case m := <-f.in:
		return m, nil
	case <-f.ctx.Done():
		return nil, io.EOF
	}
}
func (f *failedStream) Send(m *pb.ControlMessage) error {
	f.out <- m
	if m.GetRunAttempt() != nil && f.failRun {
		close(f.failed)
		return errors.New("injected transport send failure")
	}
	if f.blocked != nil {
		select {
		case <-f.blocked:
		case <-f.ctx.Done():
			return f.ctx.Err()
		}
	}
	return nil
}
func receive(t *testing.T, ctx context.Context, ch <-chan *pb.ControlMessage) *pb.ControlMessage {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return nil
}
func TestCommittedAssignmentSurvivesDispatchFailure(t *testing.T) {
	for _, queueFull := range []bool{false, true} {
		t.Run(map[bool]string{false: "send-error", true: "queue-full"}[queueFull], func(t *testing.T) {
			pool, ctx := testutil.Database(t)
			store := &postgres.Store{Pool: pool, Lease: 10 * time.Second, Offline: 6 * time.Second, RetryBase: time.Millisecond}
			if err := store.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			s := New(store, 2*time.Second)
			b := uuid.NewString()
			streamCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			f := &failedStream{ctx: streamCtx, in: make(chan *pb.WorkerMessage, 2), out: make(chan *pb.ControlMessage, 2), failRun: !queueFull, failed: make(chan struct{})}
			if queueFull {
				f.blocked = make(chan struct{})
			}
			f.in <- &pb.WorkerMessage{Body: &pb.WorkerMessage_Register{Register: &pb.Register{WorkerId: "worker-b", WorkerSessionId: b, CapacitySlots: 1}}}
			done := make(chan error, 1)
			go func() { done <- s.Connect(f) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("Connect cleanup timeout")
				}
			})
			if m := receive(t, ctx, f.out); m.GetRegistered() == nil {
				t.Fatalf("registration must be first: %v", m)
			}
			id, err := store.Submit(ctx, domain.Spec{Image: "alpine:3.22", Command: []string{"true"}, TimeoutSeconds: 30, MaxAttempts: 2})
			if err != nil {
				t.Fatal(err)
			}
			a, err := store.Schedule(ctx, s.connected())
			if err != nil || a == nil {
				t.Fatalf("schedule: %v %v", a, err)
			}
			if queueFull {
				for n := 0; n < 32; n++ {
					if !s.send(b, &pb.ControlMessage{}) {
						t.Fatal("queue unexpectedly full")
					}
				}
			}
			delivered := s.send(b, &pb.ControlMessage{Body: &pb.ControlMessage_RunAttempt{RunAttempt: &pb.RunAttempt{Identity: wire(a.Attempt.Identity)}}})
			if queueFull {
				if delivered {
					t.Fatal("assignment should fail queue admission")
				}
				cancel()
			} else {
				if !delivered {
					t.Fatal("expected enqueue before transport failure")
				}
				if m := receive(t, ctx, f.out); m.GetRunAttempt() == nil {
					t.Fatalf("expected assignment: %v", m)
				}
				select {
				case <-f.failed:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				f.in <- &pb.WorkerMessage{Body: &pb.WorkerMessage_Heartbeat{Heartbeat: &pb.Heartbeat{WorkerSessionId: b}}}
			}
			select {
			case <-done:
				done <- nil
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			c := uuid.NewString()
			if err := store.Register(ctx, "worker-c", c); err != nil {
				t.Fatal(err)
			}
			if err := store.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			j, err := store.GetJob(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if j.State != "DISPATCHED" || j.AttemptCount != 1 || j.Attempts[0].State != "ASSIGNED" || !j.Attempts[0].LeaseExpiresAt.Equal(a.Attempt.LeaseExpiresAt) {
				t.Fatalf("failed dispatch changed owner: %+v", j)
			}
			var slots int
			if err := pool.QueryRow(ctx, `SELECT active_slots FROM worker_sessions WHERE id=$1`, b).Scan(&slots); err != nil || slots != 1 {
				t.Fatalf("reservation=%d %v", slots, err)
			}
			if other, err := store.Schedule(ctx, []string{c}); err != nil || other != nil {
				t.Fatalf("early replacement: %v %v", other, err)
			}
			if _, err := pool.Exec(ctx, `UPDATE job_attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, a.Attempt.AttemptID); err != nil {
				t.Fatal(err)
			}
			if err := store.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `UPDATE jobs SET retry_available_at=clock_timestamp() WHERE id=$1`, id); err != nil {
				t.Fatal(err)
			}
			if err := store.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			retry, err := store.Schedule(ctx, []string{c})
			if err != nil || retry == nil || retry.Attempt.FencingToken != a.Attempt.FencingToken+1 {
				t.Fatalf("retry: %v %v", retry, err)
			}
			if _, err := store.Complete(ctx, retry.Attempt.Identity, domain.Result{State: "SUCCEEDED"}); err != nil {
				t.Fatal(err)
			}
			ack, err := s.ReportResult(ctx, &pb.AttemptResult{Identity: wire(a.Attempt.Identity), State: "SUCCEEDED"})
			if err != nil || ack.Accepted || ack.Error != domain.ErrStale.Error() {
				t.Fatalf("stale dispatch: %v %v", ack, err)
			}
			if err := pool.QueryRow(ctx, `SELECT sum(active_slots) FROM worker_sessions`).Scan(&slots); err != nil || slots != 0 {
				t.Fatalf("slots=%d %v", slots, err)
			}
		})
	}
}
