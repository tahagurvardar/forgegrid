//go:build integration

package postgres

import (
	"context"
	"errors"
	"forgegrid/internal/domain"
	"testing"
)

func node(key string, deps ...string) domain.PipelineJobSpec {
	return domain.PipelineJobSpec{Key: key, Spec: domain.Spec{Image: "alpine:3.22", Command: []string{"true"}, TimeoutSeconds: 30, MaxAttempts: 2}, Dependencies: deps}
}
func pipeline(t *testing.T, s *Store, ctx context.Context, nodes ...domain.PipelineJobSpec) string {
	t.Helper()
	id, err := s.SubmitPipeline(ctx, domain.PipelineSpec{Jobs: nodes})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func pipelineView(t *testing.T, s *Store, ctx context.Context, id string) (domain.Pipeline, map[string]domain.Job) {
	t.Helper()
	p, err := s.GetPipeline(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	jobs := map[string]domain.Job{}
	for _, j := range p.Jobs {
		jobs[*j.Key] = j
	}
	return p, jobs
}
func claimKey(t *testing.T, s *Store, ctx context.Context, id, key, session string) *domain.Assignment {
	t.Helper()
	_, jobs := pipelineView(t, s, ctx, id)
	a, err := s.scheduleJob(ctx, jobs[key].ID, []string{session})
	if err != nil || a == nil {
		t.Fatalf("claim %s: %v %v", key, a, err)
	}
	return a
}
func finishKey(t *testing.T, s *Store, ctx context.Context, id, key, session string, r domain.Result) {
	t.Helper()
	a := claimKey(t, s, ctx, id, key, session)
	if _, err := s.Complete(ctx, a.Attempt.Identity, r); err != nil {
		t.Fatal(err)
	}
	assertSlots(t, s, ctx)
}
func workloadFailure() domain.Result {
	return domain.Result{State: "FAILED", ExitCode: 7, FailureKind: "EXIT_NON_ZERO"}
}
func infraFailure() domain.Result {
	return domain.Result{State: "FAILED", ExitCode: -1, FailureKind: "EXECUTOR_INFRA_ERROR"}
}

func TestPipelineSuccessShapes(t *testing.T) {
	for _, c := range []struct {
		name  string
		nodes []domain.PipelineJobSpec
		order []string
	}{
		{"single", []domain.PipelineJobSpec{node("a")}, []string{"a"}},
		{"linear", []domain.PipelineJobSpec{node("a"), node("b", "a"), node("c", "b")}, []string{"a", "b", "c"}},
		{"fan-out", []domain.PipelineJobSpec{node("a"), node("b", "a"), node("c", "a")}, []string{"a", "b", "c"}},
		{"fan-in", []domain.PipelineJobSpec{node("a"), node("b"), node("c", "a", "b")}, []string{"a", "b", "c"}},
		{"diamond", []domain.PipelineJobSpec{node("build"), node("test", "build"), node("lint", "build"), node("package", "test", "lint")}, []string{"build", "test", "lint", "package"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, c.nodes...)
			_, jobs := pipelineView(t, s, ctx, id)
			for _, n := range c.nodes {
				want := "QUEUED"
				if len(n.Dependencies) > 0 {
					want = "BLOCKED"
				}
				if jobs[n.Key].State != want {
					t.Fatalf("initial %s: %s", n.Key, jobs[n.Key].State)
				}
				if want == "BLOCKED" {
					if a, err := s.scheduleJob(ctx, jobs[n.Key].ID, []string{b}); err != nil || a != nil {
						t.Fatalf("blocked job scheduled: %v %v", a, err)
					}
				}
			}
			for index, key := range c.order {
				finishKey(t, s, ctx, id, key, b, success())
				p, jobs := pipelineView(t, s, ctx, id)
				want := "RUNNING"
				if index == len(c.order)-1 {
					want = "SUCCEEDED"
				}
				if p.State != want {
					t.Fatalf("pipeline state after %s: %s", key, p.State)
				}
				for _, j := range jobs {
					if j.State == "QUEUED" {
						for _, parent := range j.Dependencies {
							if jobs[parent].State != "SUCCEEDED" {
								t.Fatal("released before all parents succeeded")
							}
						}
					}
				}
			}
		})
	}
}
func TestPipelineInvalidDAGDoesNotPersist(t *testing.T) {
	for _, c := range []struct {
		name  string
		nodes []domain.PipelineJobSpec
	}{
		{"cycle", []domain.PipelineJobSpec{node("a", "b"), node("b", "a")}},
		{"missing", []domain.PipelineJobSpec{node("a", "missing")}},
		{"self", []domain.PipelineJobSpec{node("a", "a")}},
		{"duplicate-key", []domain.PipelineJobSpec{node("a"), node("a")}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, ctx := fixture(t)
			before := snapshot(t, s, ctx)
			if id, err := s.SubmitPipeline(ctx, domain.PipelineSpec{Jobs: c.nodes}); err == nil || id != "" {
				t.Fatalf("invalid submission %q %v", id, err)
			}
			if snapshot(t, s, ctx) != before {
				t.Fatal("invalid DAG persisted runnable work")
			}
		})
	}
}
func TestPipelineFailureSkipsTransitivelyWithoutFailFast(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	c := register(t, s, ctx, "worker-c")
	id := pipeline(t, s, ctx, node("parent"), node("child", "parent"), node("grandchild", "child"), node("independent"))
	other := claimKey(t, s, ctx, id, "independent", c)
	if _, err := s.Advance(ctx, other.Attempt.Identity, "start"); err != nil {
		t.Fatal(err)
	}
	finishKey(t, s, ctx, id, "parent", b, workloadFailure())
	p, jobs := pipelineView(t, s, ctx, id)
	if p.State != "RUNNING" || jobs["child"].State != "SKIPPED" || jobs["grandchild"].State != "SKIPPED" || jobs["independent"].State != "RUNNING" {
		t.Fatalf("premature finalization or skip: %+v", p)
	}
	for _, key := range []string{"child", "grandchild"} {
		if jobs[key].AttemptCount != 0 || jobs[key].CurrentAttemptID != nil || jobs[key].FencingToken != 0 {
			t.Fatal("skipped job executed")
		}
	}
	if _, err := s.Complete(ctx, other.Attempt.Identity, success()); err != nil {
		t.Fatal(err)
	}
	p, _ = pipelineView(t, s, ctx, id)
	if p.State != "FAILED" {
		t.Fatal(p.State)
	}
	assertSlots(t, s, ctx)
}
func TestPipelineRetryReleaseAndExhaustion(t *testing.T) {
	for _, exhaust := range []bool{false, true} {
		t.Run(fmtBool(exhaust), func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, node("parent"), node("child", "parent"), node("grandchild", "child"))
			old := claimKey(t, s, ctx, id, "parent", b)
			if _, err := s.Complete(ctx, old.Attempt.Identity, infraFailure()); err != nil {
				t.Fatal(err)
			}
			p, jobs := pipelineView(t, s, ctx, id)
			if p.State != "RUNNING" || jobs["parent"].State != "RETRY_WAIT" || jobs["child"].State != "BLOCKED" {
				t.Fatalf("premature skip/release %+v", p)
			}
			if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			next := claimKey(t, s, ctx, id, "parent", b)
			if next.Attempt.FencingToken != old.Attempt.FencingToken+1 || next.Attempt.AttemptID == old.Attempt.AttemptID {
				t.Fatal("retry identity reused")
			}
			r := success()
			if exhaust {
				r = infraFailure()
			}
			if _, err := s.Complete(ctx, next.Attempt.Identity, r); err != nil {
				t.Fatal(err)
			}
			if duplicate, err := s.Complete(ctx, next.Attempt.Identity, r); err != nil || !duplicate {
				t.Fatal("retry completion not idempotent")
			}
			p, jobs = pipelineView(t, s, ctx, id)
			if exhaust {
				if p.State != "FAILED" || jobs["child"].State != "SKIPPED" || jobs["grandchild"].State != "SKIPPED" {
					t.Fatalf("exhaustion %+v", p)
				}
			} else {
				if jobs["child"].State != "QUEUED" || jobs["grandchild"].State != "BLOCKED" {
					t.Fatalf("retry release %+v", p)
				}
				finishKey(t, s, ctx, id, "child", b, success())
				finishKey(t, s, ctx, id, "grandchild", b, success())
				p, _ = pipelineView(t, s, ctx, id)
				if p.State != "SUCCEEDED" {
					t.Fatal(p.State)
				}
			}
			assertSlots(t, s, ctx)
		})
	}
}

func trackRelease(t *testing.T, s *Store, ctx context.Context) {
	t.Helper()
	_, err := s.Pool.Exec(ctx, `CREATE TABLE test_releases(job_id uuid); CREATE FUNCTION test_release() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO test_releases VALUES(NEW.id); RETURN NEW; END $$; CREATE TRIGGER test_release AFTER UPDATE ON jobs FOR EACH ROW WHEN(OLD.state='BLOCKED' AND NEW.state='QUEUED') EXECUTE FUNCTION test_release()`)
	if err != nil {
		t.Fatal(err)
	}
}
func TestPipelineConcurrentParentsReleaseOnce(t *testing.T) {
	for _, failure := range []string{"none", "retry", "exhausted"} {
		for _, first := range []string{"a", "b"} {
			t.Run(failure+"/"+first, func(t *testing.T) {
				s, ctx := fixture(t)
				b := register(t, s, ctx, "worker-b")
				c := register(t, s, ctx, "worker-c")
				parent := node("a")
				if failure == "exhausted" {
					parent.MaxAttempts = 1
				}
				id := pipeline(t, s, ctx, parent, node("b"), node("child", "a", "b"))
				a := claimKey(t, s, ctx, id, "a", b)
				other := claimKey(t, s, ctx, id, "b", c)
				trackRelease(t, s, ctx)
				release := pauseWrite(t, s, ctx, "job_attempts", "UPDATE", `WHEN(NEW.state IN ('SUCCEEDED','FAILED'))`)
				one, two := make(chan error, 1), make(chan error, 1)
				r := success()
				if failure != "none" {
					r = infraFailure()
				}
				aOp := func() error { _, err := s.Complete(ctx, a.Attempt.Identity, r); return err }
				bOp := func() error { _, err := s.Complete(ctx, other.Attempt.Identity, success()); return err }
				firstOp, secondOp := aOp, bOp
				if first == "b" {
					firstOp, secondOp = bOp, aOp
				}
				go func() { one <- firstOp() }()
				waitDatabase(t, s, ctx, advisoryWait)
				go func() { two <- secondOp() }()
				waitDatabase(t, s, ctx, rowWait)
				release()
				if err := <-one; err != nil {
					t.Fatal(err)
				}
				if err := <-two; err != nil {
					t.Fatal(err)
				}
				p, jobs := pipelineView(t, s, ctx, id)
				want := "QUEUED"
				count := 1
				if failure == "retry" {
					want = "BLOCKED"
					count = 0
				}
				if failure == "exhausted" {
					want = "SKIPPED"
					count = 0
					if p.State != "FAILED" {
						t.Fatal(p.State)
					}
				}
				if jobs["child"].State != want {
					t.Fatalf("child %s want %s", jobs["child"].State, want)
				}
				var releases int
				if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM test_releases WHERE job_id=$1`, jobs["child"].ID).Scan(&releases); err != nil || releases != count {
					t.Fatalf("release count=%d %v", releases, err)
				}
				assertSlots(t, s, ctx)
			})
		}
	}
}
func TestPipelineCancellationDependencyReleaseLockOrders(t *testing.T) {
	for _, first := range []string{"cancel", "complete"} {
		t.Run(first, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, node("parent"), node("child", "parent"))
			a := claimKey(t, s, ctx, id, "parent", b)
			_, jobs := pipelineView(t, s, ctx, id)
			childID := jobs["child"].ID
			table, condition := "jobs", `WHEN(NEW.state='CANCELLED')`
			if first == "complete" {
				table = "job_attempts"
				condition = `WHEN(NEW.state='SUCCEEDED')`
			}
			release := pauseWrite(t, s, ctx, table, "UPDATE", condition)
			cancel := func() error { _, err := s.Cancel(ctx, childID); return err }
			complete := func() error { _, err := s.Complete(ctx, a.Attempt.Identity, success()); return err }
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
			if err := <-two; err != nil {
				t.Fatal(err)
			}
			p, jobs := pipelineView(t, s, ctx, id)
			if p.State != "CANCELLED" || jobs["child"].State != "CANCELLED" || jobs["child"].AttemptCount != 0 {
				t.Fatalf("cancelled child released %+v", p)
			}
			assertSlots(t, s, ctx)
		})
	}
}
func TestPipelineCancelEveryJobStateAndWorkerLoss(t *testing.T) {
	for _, state := range []string{"BLOCKED", "QUEUED", "DISPATCHED", "RUNNING", "RETRY_WAIT"} {
		t.Run(state, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			nodes := []domain.PipelineJobSpec{node("target"), node("downstream", "target")}
			if state == "BLOCKED" {
				nodes = append(nodes, node("root"))
				nodes[0].Dependencies = []string{"root"}
			}
			id := pipeline(t, s, ctx, nodes...)
			_, jobs := pipelineView(t, s, ctx, id)
			var a *domain.Assignment
			if state != "BLOCKED" && state != "QUEUED" {
				a = claimKey(t, s, ctx, id, "target", b)
				if state == "RUNNING" {
					if _, err := s.Advance(ctx, a.Attempt.Identity, "start"); err != nil {
						t.Fatal(err)
					}
				}
				if state == "RETRY_WAIT" {
					if _, err := s.Complete(ctx, a.Attempt.Identity, infraFailure()); err != nil {
						t.Fatal(err)
					}
				}
			}
			active, err := s.Cancel(ctx, jobs["target"].ID)
			if err != nil {
				t.Fatal(err)
			}
			if active != nil {
				if err := s.Disconnect(ctx, b); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Pool.Exec(ctx, `UPDATE worker_sessions SET last_seen_at=clock_timestamp()-interval '20 seconds' WHERE id=$1`, b); err != nil {
					t.Fatal(err)
				}
				if err := s.Recover(ctx); err != nil {
					t.Fatal(err)
				}
				_, view := pipelineView(t, s, ctx, id)
				if view["target"].State != "CANCELLING" {
					t.Fatal("worker loss released unexpired cancelled owner")
				}
				assertSlots(t, s, ctx)
				expire(t, s, ctx, a.Attempt.AttemptID)
				if err := s.Recover(ctx); err != nil {
					t.Fatal(err)
				}
			}
			p, view := pipelineView(t, s, ctx, id)
			if view["target"].State != "CANCELLED" || view["downstream"].State != "SKIPPED" {
				t.Fatalf("cancel propagation %+v", p)
			}
			if state == "BLOCKED" {
				if p.State != "RUNNING" {
					t.Fatal("active root forgotten")
				}
				finishKey(t, s, ctx, id, "root", register(t, s, ctx, "worker-c"), success())
				p, _ = pipelineView(t, s, ctx, id)
			}
			if p.State != "CANCELLED" {
				t.Fatal(p.State)
			}
			assertSlots(t, s, ctx)
		})
	}
}
func TestWholePipelineCancellation(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	c := register(t, s, ctx, "worker-c")
	id := pipeline(t, s, ctx, node("running"), node("dispatched"), node("queued"), node("blocked", "running"))
	a := claimKey(t, s, ctx, id, "running", b)
	if _, err := s.Advance(ctx, a.Attempt.Identity, "start"); err != nil {
		t.Fatal(err)
	}
	other := claimKey(t, s, ctx, id, "dispatched", c)
	active, err := s.CancelPipeline(ctx, id)
	if err != nil || len(active) != 2 {
		t.Fatalf("cancel %v %v", active, err)
	}
	p, jobs := pipelineView(t, s, ctx, id)
	if p.State != "CANCELLING" || jobs["queued"].State != "CANCELLED" || jobs["blocked"].State != "CANCELLED" {
		t.Fatalf("cancelled graph %+v", p)
	}
	if _, err := s.Complete(ctx, a.Attempt.Identity, success()); !errors.Is(err, domain.ErrCancelled) {
		t.Fatal("cancelled result accepted")
	}
	if _, err := s.Complete(ctx, a.Attempt.Identity, cancelled()); err != nil {
		t.Fatal(err)
	}
	p, _ = pipelineView(t, s, ctx, id)
	if p.State != "CANCELLING" {
		t.Fatal("pipeline finalized with another active reservation")
	}
	expire(t, s, ctx, other.Attempt.AttemptID)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	p, jobs = pipelineView(t, s, ctx, id)
	if p.State != "CANCELLED" || jobs["dispatched"].State != "CANCELLED" || jobs["dispatched"].AttemptCount != 1 {
		t.Fatalf("cancel retry/finalization %+v", p)
	}
	assertSlots(t, s, ctx)
}
func TestPipelineCancelCompletionLockOrders(t *testing.T) {
	for _, first := range []string{"cancel", "complete"} {
		t.Run(first, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, node("one"))
			a := claimKey(t, s, ctx, id, "one", b)
			table, condition := "pipelines", `WHEN(NEW.state='CANCELLING')`
			if first == "complete" {
				table = "job_attempts"
				condition = `WHEN(NEW.state='SUCCEEDED')`
			}
			release := pauseWrite(t, s, ctx, table, "UPDATE", condition)
			cancel := func() error { _, err := s.CancelPipeline(ctx, id); return err }
			complete := func() error { _, err := s.Complete(ctx, a.Attempt.Identity, success()); return err }
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
				t.Fatalf("loser %v", err)
			}
			if first == "cancel" {
				if _, err := s.Complete(ctx, a.Attempt.Identity, cancelled()); err != nil {
					t.Fatal(err)
				}
			}
			p, _ := pipelineView(t, s, ctx, id)
			wantState := "SUCCEEDED"
			if first == "cancel" {
				wantState = "CANCELLED"
			}
			if p.State != wantState {
				t.Fatal(p.State)
			}
			assertSlots(t, s, ctx)
		})
	}
}
func TestPipelineTimeoutCompletionAndExpiry(t *testing.T) {
	for _, mode := range []string{"expired-before-success", "success-holds-lock", "timeout-first", "lease-also-expired"} {
		t.Run(mode, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, node("parent"), node("child", "parent"))
			a := claimKey(t, s, ctx, id, "parent", b)
			if _, err := s.Advance(ctx, a.Attempt.Identity, "renew"); err != nil {
				t.Fatal(err)
			}
			timeout := domain.Result{State: "TIMED_OUT", ExitCode: -1, FailureKind: "JOB_TIMEOUT"}
			if mode == "success-holds-lock" {
				if _, err := s.Pool.Exec(ctx, `UPDATE job_attempts SET execution_deadline_at=clock_timestamp()+interval '2 seconds' WHERE id=$1`, a.Attempt.AttemptID); err != nil {
					t.Fatal(err)
				}
				release := pauseWrite(t, s, ctx, "job_attempts", "UPDATE", `WHEN(NEW.state='SUCCEEDED')`)
				done := make(chan error, 1)
				go func() { _, err := s.Complete(ctx, a.Attempt.Identity, success()); done <- err }()
				waitDatabase(t, s, ctx, advisoryWait)
				waitDatabase(t, s, ctx, `SELECT clock_timestamp()>execution_deadline_at FROM job_attempts`)
				late := make(chan error, 1)
				go func() { _, err := s.Complete(ctx, a.Attempt.Identity, timeout); late <- err }()
				waitDatabase(t, s, ctx, rowWait)
				release()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if err := <-late; !errors.Is(err, domain.ErrConflict) {
					t.Fatalf("timeout overwrote success: %v", err)
				}
			} else {
				if _, err := s.Pool.Exec(ctx, `UPDATE job_attempts SET execution_deadline_at=clock_timestamp()-interval '1 second' WHERE id=$1`, a.Attempt.AttemptID); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Complete(ctx, a.Attempt.Identity, success()); !errors.Is(err, domain.ErrTimeout) {
					t.Fatalf("late success %v", err)
				}
				if _, err := s.Advance(ctx, a.Attempt.Identity, "renew"); !errors.Is(err, domain.ErrTimeout) {
					t.Fatal("timed-out authority renewed")
				}
				assertSlots(t, s, ctx)
				if mode == "lease-also-expired" {
					expire(t, s, ctx, a.Attempt.AttemptID)
					if _, err := s.Complete(ctx, a.Attempt.Identity, timeout); !errors.Is(err, domain.ErrStale) {
						t.Fatal("timeout bypassed lease")
					}
					if err := s.Recover(ctx); err != nil {
						t.Fatal(err)
					}
				} else {
					if mode == "timeout-first" {
						release := pauseWrite(t, s, ctx, "job_attempts", "UPDATE", `WHEN(NEW.state='TIMED_OUT')`)
						one, two := make(chan error, 1), make(chan error, 1)
						go func() { _, err := s.Complete(ctx, a.Attempt.Identity, timeout); one <- err }()
						waitDatabase(t, s, ctx, advisoryWait)
						go func() { _, err := s.Complete(ctx, a.Attempt.Identity, success()); two <- err }()
						waitDatabase(t, s, ctx, rowWait)
						release()
						if err := <-one; err != nil {
							t.Fatal(err)
						}
						if err := <-two; !errors.Is(err, domain.ErrConflict) {
							t.Fatalf("late success after timeout: %v", err)
						}
					} else {
						if _, err := s.Complete(ctx, a.Attempt.Identity, timeout); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			p, jobs := pipelineView(t, s, ctx, id)
			if mode == "success-holds-lock" {
				if p.State != "RUNNING" || jobs["child"].State != "QUEUED" {
					t.Fatal("valid success lost")
				}
			} else {
				if p.State != "FAILED" || jobs["child"].State != "SKIPPED" || jobs["parent"].Attempts[0].State != "TIMED_OUT" {
					t.Fatalf("timeout retried or failed to skip %+v", p)
				}
			}
			assertSlots(t, s, ctx)
		})
	}
}

func TestPipelineOwnershipAndPropagationRollback(t *testing.T) {
	for _, op := range []string{"submit", "release", "skip", "retry-exhaust", "cancel"} {
		for _, table := range []string{"pipelines", "jobs", "job_dependencies", "job_attempts", "worker_sessions"} {
			if op == "submit" && (table == "job_attempts" || table == "worker_sessions") {
				continue
			}
			if op != "submit" && table == "job_dependencies" {
				continue
			}
			if op == "cancel" && (table == "job_attempts" || table == "worker_sessions") {
				continue
			}
			for _, deferred := range []bool{false, true} {
				t.Run(op+"/"+table+"/"+fmtBool(deferred), func(t *testing.T) {
					s, ctx := fixture(t)
					b := register(t, s, ctx, "worker-b")
					spec := domain.PipelineSpec{Jobs: []domain.PipelineJobSpec{node("parent"), node("child", "parent")}}
					var id string
					var a *domain.Assignment
					if op != "submit" {
						if op == "retry-exhaust" {
							spec.Jobs[0].MaxAttempts = 1
						}
						id = pipeline(t, s, ctx, spec.Jobs...)
						a = claimKey(t, s, ctx, id, "parent", b)
					}
					before := snapshot(t, s, ctx)
					event := "UPDATE"
					if op == "submit" {
						event = "INSERT"
					}
					injectFailure(t, s, ctx, table, event, deferred)
					var err error
					switch op {
					case "submit":
						_, err = s.SubmitPipeline(ctx, spec)
					case "cancel":
						_, err = s.CancelPipeline(ctx, id)
					case "skip":
						_, err = s.Complete(ctx, a.Attempt.Identity, workloadFailure())
					case "retry-exhaust":
						_, err = s.Complete(ctx, a.Attempt.Identity, infraFailure())
					default:
						_, err = s.Complete(ctx, a.Attempt.Identity, success())
					}
					if err == nil {
						t.Fatal("fault injection missed")
					}
					if snapshot(t, s, ctx) != before {
						t.Fatal("partial ownership or graph state after rollback")
					}
					assertSlots(t, s, ctx)
				})
			}
		}
	}
}

func TestPipelineRecoveryReleasesDependencyOnlyAfterValidRetrySuccess(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	c := register(t, s, ctx, "worker-c")
	id := pipeline(t, s, ctx, node("build"), node("test", "build"), node("package", "test"))
	finishKey(t, s, ctx, id, "build", b, success())
	old := claimKey(t, s, ctx, id, "test", b)
	if _, err := s.Advance(ctx, old.Attempt.Identity, "renew"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE worker_sessions SET last_seen_at=clock_timestamp()-interval '20 seconds' WHERE id=$1`, b); err != nil {
		t.Fatal(err)
	}
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	_, jobs := pipelineView(t, s, ctx, id)
	if jobs["package"].State != "BLOCKED" || jobs["test"].AttemptCount != 1 {
		t.Fatal("worker loss prematurely released DAG")
	}
	expire(t, s, ctx, old.Attempt.AttemptID)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	next := claimKey(t, s, ctx, id, "test", c)
	if next.Attempt.FencingToken != 2 {
		t.Fatal("missing retry fence")
	}
	if _, err := s.Complete(ctx, old.Attempt.Identity, success()); !errors.Is(err, domain.ErrStale) {
		t.Fatal("stale parent released child")
	}
	if _, err := s.Complete(ctx, next.Attempt.Identity, success()); err != nil {
		t.Fatal(err)
	}
	finishKey(t, s, ctx, id, "package", c, success())
	p, _ := pipelineView(t, s, ctx, id)
	if p.State != "SUCCEEDED" {
		t.Fatal(p.State)
	}
	assertSlots(t, s, ctx)
}

func TestPipelineTimeoutLeaseRecoveryLockOrders(t *testing.T) {
	for _, first := range []string{"timeout", "recovery"} {
		t.Run(first, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, node("parent"), node("child", "parent"))
			a := claimKey(t, s, ctx, id, "parent", b)
			if _, err := s.Pool.Exec(ctx, `UPDATE job_attempts SET execution_deadline_at=clock_timestamp()-interval '1 second',lease_expires_at=clock_timestamp()+interval '2 seconds' WHERE id=$1`, a.Attempt.AttemptID); err != nil {
				t.Fatal(err)
			}
			r := domain.Result{State: "TIMED_OUT", ExitCode: -1, FailureKind: "JOB_TIMEOUT", Detail: "execution deadline exceeded before lease recovery"}
			if first == "recovery" {
				expire(t, s, ctx, a.Attempt.AttemptID)
			}
			release := pauseWrite(t, s, ctx, "job_attempts", "UPDATE", `WHEN(NEW.state='TIMED_OUT')`)
			one := make(chan error, 1)
			if first == "timeout" {
				go func() { _, err := s.Complete(ctx, a.Attempt.Identity, r); one <- err }()
			} else {
				go func() { _, err := s.recoverOne(ctx); one <- err }()
			}
			waitDatabase(t, s, ctx, advisoryWait)
			if first == "timeout" {
				waitDatabase(t, s, ctx, `SELECT clock_timestamp()>lease_expires_at FROM job_attempts`)
				if recovered, err := s.recoverOne(ctx); err != nil || recovered {
					t.Fatalf("scanner replaced held timeout: %v %v", recovered, err)
				}
				release()
				if err := <-one; err != nil {
					t.Fatal(err)
				}
			} else {
				two := make(chan error, 1)
				go func() {
					duplicate, err := s.Complete(ctx, a.Attempt.Identity, r)
					if err == nil && !duplicate {
						err = errors.New("expired result performed second finalization")
					}
					two <- err
				}()
				waitDatabase(t, s, ctx, rowWait)
				release()
				if err := <-one; err != nil {
					t.Fatal(err)
				}
				if err := <-two; err != nil {
					t.Fatal(err)
				}
			}
			p, jobs := pipelineView(t, s, ctx, id)
			if p.State != "FAILED" || jobs["parent"].AttemptCount != 1 || jobs["child"].State != "SKIPPED" {
				t.Fatalf("timeout/recovery winner %+v", p)
			}
			assertSlots(t, s, ctx)
		})
	}
}
func TestPipelineSchedulerCancellationLockOrders(t *testing.T) {
	for _, first := range []string{"cancel", "schedule"} {
		t.Run(first, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, node("root"), node("child", "root"))
			table, event, when := "pipelines", "UPDATE", `WHEN(NEW.state='CANCELLING')`
			if first == "schedule" {
				table = "job_attempts"
				event = "INSERT"
				when = ""
			}
			release := pauseWrite(t, s, ctx, table, event, when)
			one := make(chan error, 1)
			if first == "cancel" {
				go func() { _, err := s.CancelPipeline(ctx, id); one <- err }()
				waitDatabase(t, s, ctx, advisoryWait)
				if a, err := s.Schedule(ctx, []string{b}); err != nil || a != nil {
					t.Fatalf("scheduler took cancelling gate %v %v", a, err)
				}
				release()
				if err := <-one; err != nil {
					t.Fatal(err)
				}
			} else {
				var a *domain.Assignment
				go func() { var err error; a, err = s.Schedule(ctx, []string{b}); one <- err }()
				waitDatabase(t, s, ctx, advisoryWait)
				two := make(chan error, 1)
				go func() { _, err := s.CancelPipeline(ctx, id); two <- err }()
				waitDatabase(t, s, ctx, rowWait)
				release()
				if err := <-one; err != nil {
					t.Fatal(err)
				}
				if err := <-two; err != nil {
					t.Fatal(err)
				}
				if a == nil {
					t.Fatal("committed assignment missing")
				}
				assertSlots(t, s, ctx)
				if _, err := s.Advance(ctx, a.Attempt.Identity, "renew"); !errors.Is(err, domain.ErrCancelled) {
					t.Fatal("cancelled dispatch renewed")
				}
				expire(t, s, ctx, a.Attempt.AttemptID)
				if err := s.Recover(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if a, err := s.Schedule(ctx, []string{b}); err != nil || a != nil {
				t.Fatalf("cancelled graph scheduled %v %v", a, err)
			}
			p, _ := pipelineView(t, s, ctx, id)
			if p.State != "CANCELLED" {
				t.Fatal(p.State)
			}
			assertSlots(t, s, ctx)
		})
	}
}
func TestPipelineRenewalsNeverResetExecutionDeadline(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	id := pipeline(t, s, ctx, node("one"))
	a := claimKey(t, s, ctx, id, "one", b)
	if _, err := s.Advance(ctx, a.Attempt.Identity, "renew"); err != nil {
		t.Fatal(err)
	}
	_, jobs := pipelineView(t, s, ctx, id)
	deadline := jobs["one"].Attempts[0].ExecutionDeadline
	budget, err := s.ExecutionBudget(ctx, a.Attempt.Identity)
	if err != nil || budget <= 0 || budget > 30000 {
		t.Fatalf("initial budget %d %v", budget, err)
	}
	if _, err := s.Advance(ctx, a.Attempt.Identity, "renew"); err != nil {
		t.Fatal(err)
	}
	_, jobs = pipelineView(t, s, ctx, id)
	later, err := s.ExecutionBudget(ctx, a.Attempt.Identity)
	if err != nil || later > budget || deadline == nil || !jobs["one"].Attempts[0].ExecutionDeadline.Equal(*deadline) {
		t.Fatal("renewal reset timeout")
	}
}

func TestPipelineDependencyPropagationRollbackAfterParentMutation(t *testing.T) {
	for _, mode := range []string{"release-child", "skip-grandchild"} {
		t.Run(mode, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, node("parent"), node("child", "parent"), node("grandchild", "child"))
			a := claimKey(t, s, ctx, id, "parent", b)
			_, jobs := pipelineView(t, s, ctx, id)
			key, state := "child", "QUEUED"
			result := success()
			if mode == "skip-grandchild" {
				key = "grandchild"
				state = "SKIPPED"
				result = workloadFailure()
			}
			// Failure occurs only after the parent/attempt/slot writes, and for
			// transitive skip after the first downstream skip has already executed.
			ddl := `CREATE FUNCTION fail_child() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'dependency propagation fault'; END $$; CREATE TRIGGER fail_child AFTER UPDATE ON jobs FOR EACH ROW WHEN(NEW.id='` + jobs[key].ID + `'::uuid AND NEW.state='` + state + `') EXECUTE FUNCTION fail_child()`
			if _, err := s.Pool.Exec(ctx, ddl); err != nil {
				t.Fatal(err)
			}
			before := snapshot(t, s, ctx)
			if _, err := s.Complete(ctx, a.Attempt.Identity, result); err == nil {
				t.Fatal("child fault not reached")
			}
			if snapshot(t, s, ctx) != before {
				t.Fatal("parent or previous child persisted across graph rollback")
			}
			assertSlots(t, s, ctx)
		})
	}
}

func TestPipelineConcurrentTerminalAggregation(t *testing.T) {
	for _, want := range []string{"SUCCEEDED", "FAILED", "CANCELLED"} {
		t.Run(want, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			c := register(t, s, ctx, "worker-c")
			id := pipeline(t, s, ctx, node("a"), node("b"))
			a := claimKey(t, s, ctx, id, "a", b)
			other := claimKey(t, s, ctx, id, "b", c)
			r := success()
			if want == "FAILED" {
				r = workloadFailure()
			}
			release := pauseWrite(t, s, ctx, "job_attempts", "UPDATE", `WHEN(NEW.state IN ('SUCCEEDED','FAILED'))`)
			one, two := make(chan error, 1), make(chan error, 1)
			go func() { _, err := s.Complete(ctx, a.Attempt.Identity, r); one <- err }()
			waitDatabase(t, s, ctx, advisoryWait)
			go func() {
				result := success()
				if want == "CANCELLED" {
					if _, err := s.Cancel(ctx, other.Job.ID); err != nil {
						two <- err
						return
					}
					result = cancelled()
				}
				_, err := s.Complete(ctx, other.Attempt.Identity, result)
				two <- err
			}()
			waitDatabase(t, s, ctx, rowWait)
			release()
			if err := <-one; err != nil {
				t.Fatal(err)
			}
			if err := <-two; err != nil {
				t.Fatal(err)
			}
			p, _ := pipelineView(t, s, ctx, id)
			if p.State != want {
				t.Fatalf("aggregate %s want %s", p.State, want)
			}
			before := snapshot(t, s, ctx)
			if dup, err := s.Complete(ctx, a.Attempt.Identity, r); err != nil || !dup {
				t.Fatal("terminal result not idempotent")
			}
			if snapshot(t, s, ctx) != before {
				t.Fatal("duplicate recalculated terminal pipeline")
			}
			assertSlots(t, s, ctx)
		})
	}
}

func TestPipelineRecoveryClassifiesFirstExpiredAuthority(t *testing.T) {
	for _, first := range []string{"lease", "timeout"} {
		t.Run(first, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := pipeline(t, s, ctx, node("parent"), node("child", "parent"))
			a := claimKey(t, s, ctx, id, "parent", b)
			leaseAgo, timeoutAgo := 20, 10
			if first == "timeout" {
				leaseAgo, timeoutAgo = 10, 20
			}
			if _, err := s.Pool.Exec(ctx, `UPDATE job_attempts SET lease_expires_at=clock_timestamp()-$2*interval '1 second',execution_deadline_at=clock_timestamp()-$3*interval '1 second' WHERE id=$1`, a.Attempt.AttemptID, leaseAgo, timeoutAgo); err != nil {
				t.Fatal(err)
			}
			// Both deadlines have expired before recovery, as after a long outage.
			if err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			p, jobs := pipelineView(t, s, ctx, id)
			if first == "lease" {
				if p.State != "RUNNING" || jobs["parent"].Attempts[0].State != "LOST" || jobs["parent"].State != "QUEUED" || jobs["child"].State != "BLOCKED" {
					t.Fatalf("infra loss reclassified %+v", p)
				}
				next := claimKey(t, s, ctx, id, "parent", b)
				if next.Attempt.FencingToken != 2 {
					t.Fatal("retry fence missing")
				}
				if _, err := s.Complete(ctx, next.Attempt.Identity, success()); err != nil {
					t.Fatal(err)
				}
				finishKey(t, s, ctx, id, "child", b, success())
			} else {
				if p.State != "FAILED" || jobs["parent"].Attempts[0].State != "TIMED_OUT" || jobs["child"].State != "SKIPPED" {
					t.Fatalf("timeout retried %+v", p)
				}
			}
			assertSlots(t, s, ctx)
		})
	}
}
