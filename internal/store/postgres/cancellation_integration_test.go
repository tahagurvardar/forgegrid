//go:build integration

package postgres

import (
	"errors"
	"forgegrid/internal/domain"
	"testing"
)

func cancelled() domain.Result {
	return domain.Result{State: "CANCELLED", ExitCode: -1, FailureKind: "JOB_CANCELLED"}
}

func TestCancellationCompletionLockOrders(t *testing.T) {
	for _, first := range []string{"cancel", "complete"} {
		t.Run(first+"-first", func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			job := submit(t, s, ctx, 2)
			a := claim(t, s, ctx, b)
			table, condition := "jobs", `WHEN (NEW.state='CANCELLING')`
			if first == "complete" {
				table = "job_attempts"
				condition = `WHEN (NEW.state='SUCCEEDED')`
			}
			release := pauseWrite(t, s, ctx, table, "UPDATE", condition)
			cancel := func() error { _, e := s.Cancel(ctx, job); return e }
			complete := func() error { _, e := s.Complete(ctx, a.Attempt.Identity, success()); return e }
			firstOp, secondOp := cancel, complete
			if first == "complete" {
				firstOp, secondOp = complete, cancel
			}
			one, two := make(chan error, 1), make(chan error, 1)
			go func() { one <- firstOp() }()
			waitDatabase(t, s, ctx, advisoryWait)
			go func() { two <- secondOp() }()
			waitDatabase(t, s, ctx, rowWait)
			release()
			if err := <-one; err != nil {
				t.Fatal(err)
			}
			want := domain.ErrCancelled
			if first == "complete" {
				want = domain.ErrTerminal
			}
			if err := <-two; !errors.Is(err, want) {
				t.Fatalf("loser=%v want=%v", err, want)
			}
			if first == "cancel" {
				j, e := s.GetJob(ctx, job)
				if e != nil {
					t.Fatal(e)
				}
				if j.State != "CANCELLING" || !domain.Active(j.Attempts[0].State) {
					t.Fatal("cancel released ownership before stop ACK")
				}
				assertSlots(t, s, ctx)
				if _, err := s.Advance(ctx, a.Attempt.Identity, "renew"); !errors.Is(err, domain.ErrCancelled) {
					t.Fatalf("cancelled execution renewed: %v", err)
				}
				if _, err := s.Complete(ctx, a.Attempt.Identity, cancelled()); err != nil {
					t.Fatal(err)
				}
				if duplicate, err := s.Complete(ctx, a.Attempt.Identity, cancelled()); err != nil || !duplicate {
					t.Fatal("cancel ACK not idempotent")
				}
			}
			j, e := s.GetJob(ctx, job)
			if e != nil {
				t.Fatal(e)
			}
			wantState := "SUCCEEDED"
			if first == "cancel" {
				wantState = "CANCELLED"
			}
			if j.State != wantState || j.AttemptCount != 1 {
				t.Fatalf("winner changed: %+v", j)
			}
			assertSlots(t, s, ctx)
		})
	}
}

func TestCancellationExpiryAndQueuedJobs(t *testing.T) {
	for _, state := range []string{"QUEUED", "DISPATCHED", "RETRY_WAIT"} {
		t.Run(state, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := submit(t, s, ctx, 2)
			if state != "QUEUED" {
				a := claim(t, s, ctx, b)
				if state == "RETRY_WAIT" {
					if _, e := s.Complete(ctx, a.Attempt.Identity, domain.Result{State: "FAILED", ExitCode: -1, FailureKind: "EXECUTOR_INFRA_ERROR"}); e != nil {
						t.Fatal(e)
					}
				}
			}
			active, e := s.Cancel(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			if active != nil {
				before := snapshot(t, s, ctx)
				if _, e = s.Cancel(ctx, id); e != nil {
					t.Fatal(e)
				}
				if snapshot(t, s, ctx) != before {
					t.Fatal("duplicate cancel mutated reservation")
				}
				if e = s.Recover(ctx); e != nil {
					t.Fatal(e)
				}
				j, e := s.GetJob(ctx, id)
				if e != nil {
					t.Fatal(e)
				}
				if j.State != "CANCELLING" {
					t.Fatal("scanner released unexpired cancellation")
				}
				assertSlots(t, s, ctx)
				expire(t, s, ctx, active.AttemptID)
				if _, e = s.Complete(ctx, *active, cancelled()); !errors.Is(e, domain.ErrStale) {
					t.Fatal("expired cancellation ACK accepted")
				}
				if e = s.Recover(ctx); e != nil {
					t.Fatal(e)
				}
			}
			j, e := s.GetJob(ctx, id)
			if e != nil {
				t.Fatal(e)
			}
			if j.State != "CANCELLED" {
				t.Fatalf("cancel did not converge: %+v", j)
			}
			if a, e := s.Schedule(ctx, []string{b}); e != nil || a != nil {
				t.Fatal("cancelled job retried")
			}
			assertSlots(t, s, ctx)
		})
	}
}

func TestUnrequestedCancellationRejected(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	a := claim(t, s, ctx, b)
	before := snapshot(t, s, ctx)
	if _, err := s.Complete(ctx, a.Attempt.Identity, cancelled()); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("worker cancelled logical job without request: %v", err)
	}
	if snapshot(t, s, ctx) != before {
		t.Fatal("unrequested cancellation changed state")
	}
}

func TestCancellationRollback(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, deferred := range []bool{false, true} {
			t.Run(fmtBool(active)+"/deferred="+fmtBool(deferred), func(t *testing.T) {
				s, ctx := fixture(t)
				b := register(t, s, ctx, "worker-b")
				id := submit(t, s, ctx, 2)
				if active {
					claim(t, s, ctx, b)
				}
				before := snapshot(t, s, ctx)
				injectFailure(t, s, ctx, "jobs", "UPDATE", deferred)
				if identity, e := s.Cancel(ctx, id); e == nil || identity != nil {
					t.Fatal("returned rolled-back cancellation")
				}
				if snapshot(t, s, ctx) != before {
					t.Fatal("cancel rollback changed ownership")
				}
				assertSlots(t, s, ctx)
			})
		}
	}
	for _, op := range []string{"ack", "expire"} {
		for _, table := range []string{"job_attempts", "jobs", "worker_sessions"} {
			for _, deferred := range []bool{false, true} {
				t.Run(op+"/"+table+"/deferred="+fmtBool(deferred), func(t *testing.T) {
					s, ctx := fixture(t)
					b := register(t, s, ctx, "worker-b")
					id := submit(t, s, ctx, 2)
					a := claim(t, s, ctx, b)
					if _, e := s.Cancel(ctx, id); e != nil {
						t.Fatal(e)
					}
					if op == "expire" {
						expire(t, s, ctx, a.Attempt.AttemptID)
					}
					before := snapshot(t, s, ctx)
					injectFailure(t, s, ctx, table, "UPDATE", deferred)
					var e error
					if op == "ack" {
						_, e = s.Complete(ctx, a.Attempt.Identity, cancelled())
					} else {
						_, e = s.recoverOne(ctx)
					}
					if e == nil {
						t.Fatal("cancellation failure injection missed")
					}
					if snapshot(t, s, ctx) != before {
						t.Fatal("partial cancellation terminal transition")
					}
					assertSlots(t, s, ctx)
				})
			}
		}
	}
}

func fmtBool(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
