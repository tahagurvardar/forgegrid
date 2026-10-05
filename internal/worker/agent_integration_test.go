//go:build integration

package worker

import (
	"context"
	"fmt"
	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/controlplane"
	"forgegrid/internal/domain"
	"forgegrid/internal/store/postgres"
	"forgegrid/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"net"
	"testing"
	"time"
)

type orderingPeer struct {
	pb.UnimplementedWorkerControlServer
	store            *postgres.Store
	results          chan error
	cancel           bool
	cancelBeforeACK  bool
	timeoutBeforeACK bool
	budgetOnly       bool
}

func assignment(a *domain.Assignment) *pb.ControlMessage {
	return &pb.ControlMessage{Body: &pb.ControlMessage_RunAttempt{RunAttempt: &pb.RunAttempt{Identity: &pb.AttemptIdentity{AttemptId: a.Attempt.AttemptID, WorkerSessionId: a.Attempt.SessionID, FencingToken: a.Attempt.FencingToken}, JobId: a.Job.ID, Image: a.Job.Image, Command: a.Job.Command, TimeoutSeconds: int32(a.Job.TimeoutSeconds), AttemptNumber: int32(a.Attempt.Number)}}}
}
func peerIdentity(i *pb.AttemptIdentity) domain.Identity {
	return domain.Identity{AttemptID: i.AttemptId, SessionID: i.WorkerSessionId, FencingToken: i.FencingToken}
}

func (p *orderingPeer) Connect(stream pb.WorkerControl_ConnectServer) (err error) {
	defer func() {
		if err != nil {
			select {
			case p.results <- err:
			default:
			}
		}
	}()
	ctx := stream.Context()
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	r := msg.GetRegister()
	if r == nil {
		return fmt.Errorf("registration missing")
	}
	if err = p.store.Register(ctx, r.WorkerId, r.WorkerSessionId); err != nil {
		return err
	}
	if err = stream.Send(&pb.ControlMessage{Body: &pb.ControlMessage_Registered{Registered: &pb.Registered{WorkerSessionId: r.WorkerSessionId, HeartbeatIntervalMs: 100}}}); err != nil {
		return err
	}
	a, err := p.store.Schedule(ctx, []string{r.WorkerSessionId})
	if err != nil || a == nil {
		return fmt.Errorf("first claim: %v", err)
	}
	if err = stream.Send(assignment(a)); err != nil {
		return err
	}
	control := controlplane.New(p.store, 100*time.Millisecond)
	var delayed *pb.ResultAck
	count := 0
	for {
		msg, err = stream.Recv()
		if err != nil {
			return err
		}
		switch {
		case msg.GetHeartbeat() != nil:
			err = p.store.Heartbeat(ctx, r.WorkerSessionId)
		case msg.GetAssignmentRejected() != nil:
			return fmt.Errorf("spurious capacity rejection: %s", msg.GetAssignmentRejected().Reason)
		case msg.GetAssignmentAccepted() != nil:
			_, err = p.store.Advance(ctx, peerIdentity(msg.GetAssignmentAccepted()), "accept")
			if err == nil && delayed != nil {
				// ACK for attempt #1 arrives only after attempt #2 has been accepted.
				err = stream.Send(&pb.ControlMessage{Body: &pb.ControlMessage_ResultAck{ResultAck: delayed}})
				delayed = nil
			}
		case msg.GetLeaseRenewRequest() != nil:
			req := msg.GetLeaseRenewRequest()
			var ttl time.Duration
			ttl, err = p.store.Advance(ctx, peerIdentity(req.Identity), "renew")
			if err == nil && p.cancelBeforeACK {
				reason := domain.ErrCancelled.Error()
				if p.timeoutBeforeACK {
					_, err = p.store.Pool.Exec(ctx, `UPDATE job_attempts SET execution_deadline_at=clock_timestamp()-interval '1 second' WHERE id=$1`, req.Identity.AttemptId)
					reason = domain.ErrTimeout.Error()
				} else {
					_, err = p.store.Cancel(ctx, a.Job.ID)
				}
				if err == nil {
					if !p.budgetOnly {
						err = stream.Send(&pb.ControlMessage{Body: &pb.ControlMessage_CancelAttempt{CancelAttempt: &pb.CancelAttempt{Identity: req.Identity, Reason: reason}}})
					}
				}
			}
			if err == nil {
				var budget int64
				budget, err = p.store.ExecutionBudget(ctx, peerIdentity(req.Identity))
				if err == nil {
					err = stream.Send(&pb.ControlMessage{Body: &pb.ControlMessage_LeaseRenewed{LeaseRenewed: &pb.LeaseRenewed{Identity: req.Identity, RequestId: req.RequestId, TtlMs: ttl.Milliseconds(), ExecutionBudgetMs: budget}}})
				}
			}
		case msg.GetAttemptStarted() != nil:
			_, err = p.store.Advance(ctx, peerIdentity(msg.GetAttemptStarted()), "start")
			if err == nil && p.cancel {
				_, err = p.store.Cancel(ctx, a.Job.ID)
				if err == nil {
					err = stream.Send(&pb.ControlMessage{Body: &pb.ControlMessage_CancelAttempt{CancelAttempt: &pb.CancelAttempt{Identity: msg.GetAttemptStarted(), Reason: domain.ErrCancelled.Error()}}})
				}
			}
		case msg.GetAttemptCompleted() != nil || msg.GetAttemptFailed() != nil:
			result := msg.GetAttemptCompleted()
			if result == nil {
				result = msg.GetAttemptFailed()
			}
			var ack *pb.ResultAck
			ack, err = control.ReportResult(ctx, result)
			if err != nil {
				return err
			}
			if !ack.Accepted {
				return fmt.Errorf("result rejected: %+v", ack)
			}
			count++
			if p.cancel {
				want := "CANCELLED"
				if p.timeoutBeforeACK {
					want = "TIMED_OUT"
				}
				if result.State != want {
					return fmt.Errorf("cancel acknowledged as %s", result.State)
				}
				p.results <- nil
				return stream.Send(&pb.ControlMessage{Body: &pb.ControlMessage_ResultAck{ResultAck: ack}})
			}
			if count == 1 {
				delayed = ack
				next, e := p.store.Schedule(ctx, []string{r.WorkerSessionId})
				if e != nil || next == nil {
					return fmt.Errorf("next authoritative claim: %v", e)
				}
				// Real PostgreSQL has already released old capacity and reserved new.
				err = stream.Send(assignment(next))
			} else {
				err = stream.Send(&pb.ControlMessage{Body: &pb.ControlMessage_ResultAck{ResultAck: ack}})
				if err == nil {
					p.results <- nil
					return nil
				}
			}
		default:
			return fmt.Errorf("unexpected message")
		}
		if err != nil {
			return err
		}
	}
}

func TestWorkerMessageOrderingWithPostgres(t *testing.T) {
	for _, mode := range []string{"next-assignment-before-result-ack", "cancel-running", "cancel-before-initial-renewal-ack", "timeout-before-initial-renewal-ack", "expired-execution-budget-ack"} {
		t.Run(mode, func(t *testing.T) {
			cancelRequested := mode != "next-assignment-before-result-ack"
			cancelBeforeACK := mode == "cancel-before-initial-renewal-ack" || mode == "timeout-before-initial-renewal-ack" || mode == "expired-execution-budget-ack"
			timeoutBeforeACK := mode == "timeout-before-initial-renewal-ack" || mode == "expired-execution-budget-ack"
			pool, ctx := testutil.Database(t)
			store := &postgres.Store{Pool: pool, Lease: 2 * time.Second, Offline: 10 * time.Second}
			if err := store.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			jobs := []string{}
			n := 2
			if cancelRequested {
				n = 1
			}
			for i := 0; i < n; i++ {
				id, e := store.Submit(ctx, domain.Spec{Image: "test-executor", Command: []string{"echo"}, TimeoutSeconds: 10, MaxAttempts: 2})
				if e != nil {
					t.Fatal(e)
				}
				jobs = append(jobs, id)
			}
			peer := &orderingPeer{store: store, results: make(chan error, 2), cancel: cancelRequested, cancelBeforeACK: cancelBeforeACK, timeoutBeforeACK: timeoutBeforeACK, budgetOnly: mode == "expired-execution-budget-ack"}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			pb.RegisterWorkerControlServer(server, peer)
			go server.Serve(listener)
			t.Cleanup(server.Stop)
			workerCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			executed := make(chan struct{}, 2)
			executor := func(ctx context.Context, _ pb.WorkerLogsClient, _ string, _ *pb.RunAttempt, started func()) domain.Result {
				executed <- struct{}{}
				started()
				if cancelRequested {
					<-ctx.Done()
				}
				return domain.Result{State: "SUCCEEDED"}
			}
			go func() {
				done <- runSession(workerCtx, listener.Addr().String(), "ordering-worker", uuid.NewString(), 200*time.Millisecond, executor)
			}()
			t.Cleanup(func() {
				stop()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("worker did not stop")
				}
			})
			select {
			case err = <-peer.results:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if cancelBeforeACK {
				// Stop and join the agent before inspecting the execution signal.
				stop()
				select {
				case e := <-done:
					done <- e
				case <-time.After(5 * time.Second):
					t.Fatal("worker did not stop")
				}
				select {
				case <-executed:
					t.Fatal("cancelled execution started after delayed initial renewal ACK")
				default:
				}
			}
			for _, id := range jobs {
				j, e := store.GetJob(ctx, id)
				if e != nil {
					t.Fatal(e)
				}
				want := "SUCCEEDED"
				if cancelRequested {
					want = "CANCELLED"
				}
				if timeoutBeforeACK {
					want = "FAILED"
				}
				if j.State != want || j.AttemptCount != 1 {
					t.Fatalf("unexpected persisted result %+v", j)
				}
			}
			var slots int
			if err = pool.QueryRow(ctx, `SELECT sum(active_slots) FROM worker_sessions`).Scan(&slots); err != nil {
				t.Fatal(err)
			}
			if slots != 0 {
				t.Fatal("capacity leaked")
			}
		})
	}
}
