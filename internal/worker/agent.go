package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/domain"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type activeAttempt struct {
	assignment      *pb.RunAttempt
	deadline        time.Time
	initialDeadline time.Time
	pending         uint64
	sent            time.Time
	started         bool
	cancel          context.CancelFunc
	result          *pb.AttemptResult
	lastReport      time.Time
	lost            bool
	cancelRequested bool
	stopState       string
}

func same(a, b *pb.AttemptIdentity) bool {
	return a != nil && b != nil && a.AttemptId == b.AttemptId && a.FencingToken == b.FencingToken && a.WorkerSessionId == b.WorkerSessionId
}
func Run(ctx context.Context, address, worker string, renewInterval time.Duration) error {
	session := uuid.NewString()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	err := Reconcile(startup, worker, session)
	cancel()
	if err != nil {
		return err
	}
	return runSession(ctx, address, worker, session, renewInterval, runAttempt)
}

// Control-session behavior is independent of the Docker executor. Tests supply
// a deterministic executor here while retaining real gRPC and PostgreSQL state.
func runSession(ctx context.Context, address, worker, session string, renewInterval time.Duration, execute func(context.Context, pb.WorkerLogsClient, string, *pb.RunAttempt, func()) domain.Result) error {
	controlConn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer controlConn.Close()
	// Distinct HTTP/2 connections prevent large log traffic from blocking control flow.
	logConn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer logConn.Close()
	rpcctx, rpcCancel := context.WithCancel(ctx)
	defer rpcCancel()
	stream, err := pb.NewWorkerControlClient(controlConn).Connect(rpcctx)
	if err != nil {
		return err
	}
	if err = stream.Send(&pb.WorkerMessage{Body: &pb.WorkerMessage_Register{Register: &pb.Register{WorkerId: worker, WorkerSessionId: session, CapacitySlots: 1}}}); err != nil {
		return err
	}
	registered, err := stream.Recv()
	if err != nil {
		return err
	}
	reg := registered.GetRegistered()
	if reg == nil || reg.WorkerSessionId != session || reg.HeartbeatIntervalMs < 100 {
		return errors.New("invalid registration ACK")
	}
	slog.Info("registered", "worker_id", worker, "worker_session_id", session)
	outgoing := make(chan *pb.WorkerMessage, 32)
	incoming := make(chan *pb.ControlMessage, 32)
	transportErrors := make(chan error, 2)
	go func() {
		for {
			select {
			case <-rpcctx.Done():
				return
			case msg := <-outgoing:
				if e := stream.Send(msg); e != nil {
					transportErrors <- e
					return
				}
			}
		}
	}()
	go func() {
		for {
			msg, e := stream.Recv()
			if e != nil {
				transportErrors <- e
				return
			}
			select {
			case incoming <- msg:
			case <-rpcctx.Done():
				return
			}
		}
	}()
	send := func(msg *pb.WorkerMessage) bool {
		select {
		case outgoing <- msg:
			return true
		default:
			return false
		}
	}
	heartbeat := time.NewTicker(time.Duration(reg.HeartbeatIntervalMs) * time.Millisecond)
	defer heartbeat.Stop()
	renew := time.NewTicker(renewInterval)
	defer renew.Stop()
	guard := time.NewTicker(25 * time.Millisecond)
	defer guard.Stop()
	results := make(chan domain.Result, 1)
	var active *activeAttempt
	var request uint64
	var disconnected error
	requestRenew := func() {
		if active == nil || active.pending != 0 || active.lost || active.cancelRequested || disconnected != nil {
			return
		}
		request++
		active.pending = request
		active.sent = time.Now()
		if !send(&pb.WorkerMessage{Body: &pb.WorkerMessage_LeaseRenewRequest{LeaseRenewRequest: &pb.LeaseRenewRequest{Identity: active.assignment.Identity, RequestId: request}}}) {
			disconnected = errors.New("control queue full")
		}
	}
	defer func() {
		if active != nil && active.cancel != nil {
			active.cancel()
			select {
			case <-results:
			case <-time.After(7 * time.Second):
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-transportErrors:
			disconnected = err
			slog.Warn("control connection lost; lease guard remains active", "error", err)
			if active == nil {
				return err
			}
		case <-heartbeat.C:
			if disconnected == nil && !send(&pb.WorkerMessage{Body: &pb.WorkerMessage_Heartbeat{Heartbeat: &pb.Heartbeat{WorkerSessionId: session}}}) {
				disconnected = errors.New("control queue full")
			}
		case <-renew.C:
			requestRenew()
		case <-guard.C:
			if active == nil {
				if disconnected != nil {
					return disconnected
				}
				continue
			}
			now := time.Now()
			expired := !active.deadline.IsZero() && !LeaseValid(active.deadline, now)
			if active.deadline.IsZero() && !now.Before(active.initialDeadline) {
				expired = true
			}
			if expired && !active.lost {
				active.lost = true
				slog.Warn("local execution authority expired", "attempt_id", active.assignment.Identity.AttemptId)
				if active.cancel != nil {
					active.cancel()
				} else {
					return errors.New("initial lease ACK missing")
				}
			}
			if active.result != nil && !active.lost && disconnected == nil && now.Sub(active.lastReport) > time.Second {
				active.lastReport = now
				msg := &pb.WorkerMessage{}
				if active.result.State == "SUCCEEDED" {
					msg.Body = &pb.WorkerMessage_AttemptCompleted{AttemptCompleted: active.result}
				} else {
					msg.Body = &pb.WorkerMessage_AttemptFailed{AttemptFailed: active.result}
				}
				if !send(msg) {
					disconnected = errors.New("control queue full")
				}
			}
		case result := <-results:
			if active == nil {
				continue
			}
			active.cancel = nil
			if active.lost {
				return errors.New("lease authority lost")
			}
			if active.cancelRequested {
				result = stoppedResult(active.stopState)
			}
			active.result = &pb.AttemptResult{Identity: active.assignment.Identity, State: result.State, ExitCode: result.ExitCode, FailureKind: result.FailureKind, Detail: domain.BoundedDetail(result.Detail)}
			slog.Info("execution finished", "attempt_id", active.assignment.Identity.AttemptId, "state", result.State, "exit_code", result.ExitCode)
		case msg := <-incoming:
			switch {
			case msg.GetRunAttempt() != nil:
				a := msg.GetRunAttempt()
				if a.Identity == nil || a.Identity.WorkerSessionId != session || len(a.Command) == 0 || a.TimeoutSeconds < 1 {
					return errors.New("invalid assignment")
				}
				if active != nil {
					if same(active.assignment.Identity, a.Identity) {
						continue
					}
					if active.result == nil || active.cancel != nil {
						send(&pb.WorkerMessage{Body: &pb.WorkerMessage_AssignmentRejected{AssignmentRejected: &pb.AssignmentRejected{Identity: a.Identity, Reason: "worker capacity occupied"}}})
						continue
					}
					// A fresh assignment means PostgreSQL has released the old
					// reservation. Execution and log draining already finished;
					// a delayed old ACK must not consume the new attempt's retry.
				}
				active = &activeAttempt{assignment: a, initialDeadline: time.Now().Add(5 * time.Second)}
				send(&pb.WorkerMessage{Body: &pb.WorkerMessage_AssignmentAccepted{AssignmentAccepted: a.Identity}})
				requestRenew()
			case msg.GetLeaseRenewed() != nil:
				ack := msg.GetLeaseRenewed()
				if active == nil || active.lost || active.cancelRequested || !same(active.assignment.Identity, ack.Identity) || ack.RequestId != active.pending || active.pending == 0 {
					continue
				}
				active.pending = 0
				deadline := LeaseDeadline(active.sent, time.Duration(ack.TtlMs)*time.Millisecond)
				if !LeaseValid(deadline, time.Now()) {
					continue
				}
				active.deadline = deadline
				if !active.started {
					executionDeadline := active.sent.Add(time.Duration(ack.ExecutionBudgetMs) * time.Millisecond)
					if ack.ExecutionBudgetMs <= 0 || !time.Now().Before(executionDeadline) {
						active.cancelRequested = true
						active.stopState = "TIMED_OUT"
						r := stoppedResult("TIMED_OUT")
						active.result = &pb.AttemptResult{Identity: active.assignment.Identity, State: r.State, ExitCode: r.ExitCode, FailureKind: r.FailureKind}
						continue
					}
					active.started = true
					if payloadLimit := time.Now().Add(time.Duration(active.assignment.TimeoutSeconds) * time.Second); payloadLimit.Before(executionDeadline) {
						executionDeadline = payloadLimit
					}
					runctx, stop := context.WithDeadline(ctx, executionDeadline)
					active.cancel = stop
					a := active.assignment
					go func() {
						r := execute(runctx, pb.NewWorkerLogsClient(logConn), worker, a, func() { send(&pb.WorkerMessage{Body: &pb.WorkerMessage_AttemptStarted{AttemptStarted: a.Identity}}) })
						stop()
						results <- r
					}()
				}
			case msg.GetResultAck() != nil:
				ack := msg.GetResultAck()
				if active != nil && same(active.assignment.Identity, ack.Identity) {
					if !ack.Accepted {
						return fmt.Errorf("completion rejected: %s", ack.Error)
					}
					active = nil
				}
			case msg.GetCancelAttempt() != nil:
				c := msg.GetCancelAttempt()
				if active != nil && same(active.assignment.Identity, c.Identity) {
					if c.Reason == domain.ErrCancelled.Error() || c.Reason == domain.ErrTimeout.Error() {
						active.cancelRequested = true
						if c.Reason == domain.ErrCancelled.Error() {
							active.stopState = "CANCELLED"
						} else if active.stopState != "CANCELLED" {
							active.stopState = "TIMED_OUT"
						}
						if active.cancel != nil {
							active.cancel()
						}
						if active.result != nil {
							r := stoppedResult(active.stopState)
							active.result = &pb.AttemptResult{Identity: c.Identity, State: r.State, ExitCode: r.ExitCode, FailureKind: r.FailureKind}
							active.lastReport = time.Time{}
						}
						if !active.started {
							r := stoppedResult(active.stopState)
							active.result = &pb.AttemptResult{Identity: c.Identity, State: r.State, ExitCode: r.ExitCode, FailureKind: r.FailureKind}
						}
						continue
					}
					active.lost = true
					if active.cancel != nil {
						active.cancel()
					} else {
						return fmt.Errorf("assignment cancelled: %s", c.Reason)
					}
					if active.result != nil {
						return fmt.Errorf("authority rejected: %s", c.Reason)
					}
				}
			}
		}
	}
}
func stoppedResult(state string) domain.Result {
	if state == "TIMED_OUT" {
		return domain.Result{State: "TIMED_OUT", ExitCode: -1, FailureKind: "JOB_TIMEOUT"}
	}
	return domain.Result{State: "CANCELLED", ExitCode: -1, FailureKind: "JOB_CANCELLED"}
}
