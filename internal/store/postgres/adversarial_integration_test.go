//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"forgegrid/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func snapshot(t *testing.T, s *Store, ctx context.Context) string {
	t.Helper()
	var value string
	err := s.Pool.QueryRow(ctx, `SELECT jsonb_build_object(
 'pipelines',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]') FROM pipelines t),
 'dependencies',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY job_id,depends_on_job_id),'[]') FROM job_dependencies t),
 'workers',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]') FROM workers t),
 'sessions',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]') FROM worker_sessions t),
 'jobs',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]') FROM jobs t),
 'attempts',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]') FROM job_attempts t),
 'logs',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY attempt_id,sequence),'[]') FROM job_log_chunks t))::text`).Scan(&value)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func waitDatabase(t *testing.T, s *Store, ctx context.Context, condition string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var ready bool
		if err := s.Pool.QueryRow(ctx, condition).Scan(&ready); err != nil {
			t.Fatal(err)
		}
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("database barrier not reached: %s", condition)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

const advisoryWait = `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock' AND wait_event='advisory')`
const rowWait = `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock' AND wait_event<>'advisory')`

// The first transition holds its real ownership locks and waits inside a real
// PostgreSQL trigger. The second transition can then be observed waiting on it.
// No production hooks or mocked transaction behavior are needed.
func pauseWrite(t *testing.T, s *Store, ctx context.Context, table, event, when string) func() {
	t.Helper()
	const key = 8817291
	blocker, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { blocker.Rollback(context.Background()) })
	ddl := fmt.Sprintf(`CREATE FUNCTION test_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(%d); RETURN NEW; END $$;
CREATE TRIGGER test_pause AFTER %s ON %s FOR EACH ROW %s EXECUTE FUNCTION test_pause()`, key, event, pgx.Identifier{table}.Sanitize(), when)
	if _, err = s.Pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Pool.Exec(context.Background(), `DROP FUNCTION test_pause() CASCADE`) })
	return func() {
		t.Helper()
		if err := blocker.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRenewalCompletionLockOrders(t *testing.T) {
	for _, first := range []string{"renew", "complete"} {
		t.Run(first+"-first", func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			submit(t, s, ctx, 2)
			a := claim(t, s, ctx, b)
			condition := `WHEN (NEW.lease_expires_at<>OLD.lease_expires_at)`
			if first == "complete" {
				condition = `WHEN (NEW.state='SUCCEEDED')`
			}
			release := pauseWrite(t, s, ctx, "job_attempts", "UPDATE", condition)
			renew := func() error { _, err := s.Advance(ctx, a.Attempt.Identity, "renew"); return err }
			complete := func() error { _, err := s.Complete(ctx, a.Attempt.Identity, success()); return err }
			firstOp, secondOp := renew, complete
			if first == "complete" {
				firstOp, secondOp = complete, renew
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
			err := <-two
			if first == "complete" {
				if !errors.Is(err, domain.ErrStale) {
					t.Fatalf("terminal attempt renewed: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			j, err := s.GetJob(ctx, a.Job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if j.State != "SUCCEEDED" || j.AttemptCount != 1 {
				t.Fatalf("unexpected result %+v", j)
			}
			if first == "complete" && !j.Attempts[0].LeaseExpiresAt.Equal(a.Attempt.LeaseExpiresAt) {
				t.Fatal("renewal changed terminal lease")
			}
			assertSlots(t, s, ctx)
		})
	}
}

func TestRecoveryCompletionLockOrders(t *testing.T) {
	t.Run("recovery-first", func(t *testing.T) {
		s, ctx := fixture(t)
		b := register(t, s, ctx, "worker-b")
		submit(t, s, ctx, 2)
		a := claim(t, s, ctx, b)
		expire(t, s, ctx, a.Attempt.AttemptID)
		release := pauseWrite(t, s, ctx, "job_attempts", "UPDATE", `WHEN (NEW.state='LOST')`)
		one, two := make(chan error, 1), make(chan error, 1)
		go func() { _, e := s.recoverOne(ctx); one <- e }()
		waitDatabase(t, s, ctx, advisoryWait)
		go func() { _, e := s.Complete(ctx, a.Attempt.Identity, success()); two <- e }()
		waitDatabase(t, s, ctx, rowWait)
		release()
		if err := <-one; err != nil {
			t.Fatal(err)
		}
		if err := <-two; !errors.Is(err, domain.ErrStale) {
			t.Fatalf("lost attempt completed: %v", err)
		}
		j, err := s.GetJob(ctx, a.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if j.Attempts[0].State != "LOST" || j.State != "RETRY_WAIT" {
			t.Fatalf("unexpected job %+v", j)
		}
		assertSlots(t, s, ctx)
	})
	t.Run("valid-completion-first", func(t *testing.T) {
		s, ctx := fixture(t)
		b := register(t, s, ctx, "worker-b")
		submit(t, s, ctx, 2)
		a := claim(t, s, ctx, b)
		if _, err := s.Pool.Exec(ctx, `UPDATE job_attempts SET lease_expires_at=clock_timestamp()+interval '2 seconds' WHERE id=$1`, a.Attempt.AttemptID); err != nil {
			t.Fatal(err)
		}
		release := pauseWrite(t, s, ctx, "job_attempts", "UPDATE", `WHEN (NEW.state='SUCCEEDED')`)
		done := make(chan error, 1)
		go func() { _, e := s.Complete(ctx, a.Attempt.Identity, success()); done <- e }()
		waitDatabase(t, s, ctx, advisoryWait)
		waitDatabase(t, s, ctx, `SELECT clock_timestamp()>lease_expires_at FROM job_attempts`)
		// Scanner skips the job already owned by the valid terminal transition.
		if recovered, err := s.recoverOne(ctx); err != nil || recovered {
			t.Fatalf("scanner took a locked completion: %v %v", recovered, err)
		}
		release()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if recovered, err := s.recoverOne(ctx); err != nil || recovered {
			t.Fatal("scanner overwrote winner")
		}
		j, err := s.GetJob(ctx, a.Job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if j.State != "SUCCEEDED" {
			t.Fatal("valid completion lost")
		}
		assertSlots(t, s, ctx)
	})
}

func TestDeterministicSchedulerAndRecoveryClaims(t *testing.T) {
	for _, operation := range []string{"schedule", "recover"} {
		t.Run(operation, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			submit(t, s, ctx, 2)
			table, event, condition := "job_attempts", "INSERT", ""
			if operation == "recover" {
				a := claim(t, s, ctx, b)
				expire(t, s, ctx, a.Attempt.AttemptID)
				event = "UPDATE"
				condition = `WHEN (NEW.state='LOST')`
			}
			release := pauseWrite(t, s, ctx, table, event, condition)
			op := func() (bool, error) {
				if operation == "recover" {
					return s.recoverOne(ctx)
				}
				a, e := s.Schedule(ctx, []string{b})
				return a != nil, e
			}
			done := make(chan error, 1)
			go func() {
				claimed, e := op()
				if e == nil && !claimed {
					e = errors.New("first loop did not claim")
				}
				done <- e
			}()
			waitDatabase(t, s, ctx, advisoryWait)
			if claimed, e := op(); e != nil || claimed {
				t.Fatalf("second loop claimed held job: %v %v", claimed, e)
			}
			release()
			if e := <-done; e != nil {
				t.Fatal(e)
			}
			if claimed, e := op(); e != nil || claimed {
				t.Fatalf("second loop reclaimed committed transition: %v %v", claimed, e)
			}
			assertSlots(t, s, ctx)
		})
	}
}

func TestOldSessionCannotReregister(t *testing.T) {
	s, ctx := fixture(t)
	old := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	claim(t, s, ctx, old)
	register(t, s, ctx, "worker-b")
	before := snapshot(t, s, ctx)
	for _, worker := range []string{"worker-b", "worker-c"} {
		err := s.Register(ctx, worker, old)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("old session revived: %v", err)
		}
		if snapshot(t, s, ctx) != before {
			t.Fatal("rejected reconnect changed sessions or worker identity")
		}
	}
	assertSlots(t, s, ctx)
}

func TestQueuedOldSessionMessagesAfterSupersession(t *testing.T) {
	for _, message := range []string{"heartbeat", "renew"} {
		t.Run(message, func(t *testing.T) {
			s, ctx := fixture(t)
			old := register(t, s, ctx, "worker-b")
			submit(t, s, ctx, 2)
			a := claim(t, s, ctx, old)
			next := uuid.NewString()
			release := pauseWrite(t, s, ctx, "worker_sessions", "INSERT", "")
			registered, delayed := make(chan error, 1), make(chan error, 1)
			go func() { registered <- s.Register(ctx, "worker-b", next) }()
			waitDatabase(t, s, ctx, advisoryWait)
			go func() {
				if message == "heartbeat" {
					delayed <- s.Heartbeat(ctx, old)
				} else {
					_, e := s.Advance(ctx, a.Attempt.Identity, "renew")
					delayed <- e
				}
			}()
			waitDatabase(t, s, ctx, rowWait)
			release()
			if err := <-registered; err != nil {
				t.Fatal(err)
			}
			want := domain.ErrStale
			if message == "heartbeat" {
				want = domain.ErrSession
			}
			if err := <-delayed; !errors.Is(err, want) {
				t.Fatalf("delayed %s accepted: %v", message, err)
			}
			j, err := s.GetJob(ctx, a.Job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if j.AttemptCount != 1 || !j.Attempts[0].LeaseExpiresAt.Equal(a.Attempt.LeaseExpiresAt) {
				t.Fatal("supersession or delayed message transferred/renewed ownership")
			}
			assertSlots(t, s, ctx)
		})
	}
}

func TestDelayedRenewalAfterNewFence(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	old := claim(t, s, ctx, b)
	expire(t, s, ctx, old.Attempt.AttemptID)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	next := claim(t, s, ctx, b)
	before := snapshot(t, s, ctx)
	if _, err := s.Advance(ctx, old.Attempt.Identity, "renew"); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("old attempt renewed: %v", err)
	}
	if snapshot(t, s, ctx) != before || next.Attempt.FencingToken != old.Attempt.FencingToken+1 {
		t.Fatal("stale renewal changed authority")
	}
	assertSlots(t, s, ctx)
}

func TestConcurrentDuplicateLogs(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	a := claim(t, s, ctx, b)
	c := LogChunk{Sequence: 1, Stream: "STDOUT", Payload: []byte("persist-once")}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.AppendLog(ctx, a.Attempt.Identity, c) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	chunks, err := s.Logs(ctx, a.Attempt.AttemptID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatal("duplicate log inserts")
	}
	assertSlots(t, s, ctx)
}

func injectFailure(t *testing.T, s *Store, ctx context.Context, table, event string, deferred bool) {
	t.Helper()
	trigger := "CREATE TRIGGER test_fail AFTER "
	timing := ""
	if deferred {
		trigger = "CREATE CONSTRAINT TRIGGER test_fail AFTER "
		timing = " DEFERRABLE INITIALLY DEFERRED "
	}
	ddl := `CREATE FUNCTION test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected rollback' USING ERRCODE='P0001'; END $$;` + trigger + event + " ON " + pgx.Identifier{table}.Sanitize() + timing + " FOR EACH ROW EXECUTE FUNCTION test_fail()"
	if _, err := s.Pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Pool.Exec(context.Background(), `DROP FUNCTION test_fail() CASCADE`) })
}

func TestOwnershipWriteAndCommitRollback(t *testing.T) {
	type point struct {
		table, event string
		deferred     bool
	}
	for _, op := range []string{"register-new", "register-supersede", "schedule", "accept", "start", "renew", "complete", "recover", "requeue", "heartbeat", "disconnect", "log"} {
		points := []point{}
		switch op {
		case "register-new":
			points = []point{{"workers", "INSERT", false}, {"worker_sessions", "INSERT", false}}
		case "register-supersede":
			points = []point{{"worker_sessions", "UPDATE", false}, {"worker_sessions", "INSERT", false}}
		case "schedule":
			points = []point{{"job_attempts", "INSERT", false}, {"jobs", "UPDATE", false}, {"worker_sessions", "UPDATE", false}}
		case "accept", "renew":
			points = []point{{"job_attempts", "UPDATE", false}}
		case "start":
			points = []point{{"job_attempts", "UPDATE", false}, {"jobs", "UPDATE", false}}
		case "complete", "recover":
			points = []point{{"job_attempts", "UPDATE", false}, {"jobs", "UPDATE", false}, {"worker_sessions", "UPDATE", false}}
		case "requeue":
			points = []point{{"jobs", "UPDATE", false}}
		case "heartbeat", "disconnect":
			points = []point{{"worker_sessions", "UPDATE", false}}
		case "log":
			points = []point{{"job_log_chunks", "INSERT", false}}
		}
		last := points[len(points)-1]
		last.deferred = true
		points = append(points, last)
		for _, p := range points {
			t.Run(fmt.Sprintf("%s/%s/%s/deferred=%v", op, p.table, p.event, p.deferred), func(t *testing.T) {
				s, ctx := fixture(t)
				b := register(t, s, ctx, "worker-b")
				submit(t, s, ctx, 2)
				var a *domain.Assignment
				if op != "schedule" {
					a = claim(t, s, ctx, b)
				}
				if op == "recover" {
					expire(t, s, ctx, a.Attempt.AttemptID)
				}
				if op == "requeue" {
					if _, err := s.Complete(ctx, a.Attempt.Identity, domain.Result{State: "FAILED", ExitCode: -1, FailureKind: "EXECUTOR_INFRA_ERROR"}); err != nil {
						t.Fatal(err)
					}
				}
				before := snapshot(t, s, ctx)
				injectFailure(t, s, ctx, p.table, p.event, p.deferred)
				var err error
				switch op {
				case "register-new":
					err = s.Register(ctx, "worker-c", uuid.NewString())
				case "register-supersede":
					err = s.Register(ctx, "worker-b", uuid.NewString())
				case "schedule":
					var assigned *domain.Assignment
					assigned, err = s.Schedule(ctx, []string{b})
					if assigned != nil {
						t.Fatal("returned rolled-back assignment")
					}
				case "accept", "start", "renew":
					_, err = s.Advance(ctx, a.Attempt.Identity, op)
				case "complete":
					_, err = s.Complete(ctx, a.Attempt.Identity, success())
				case "recover":
					_, err = s.recoverOne(ctx)
				case "requeue":
					err = s.Recover(ctx)
				case "heartbeat":
					err = s.Heartbeat(ctx, b)
				case "disconnect":
					err = s.Disconnect(ctx, b)
				case "log":
					err = s.AppendLog(ctx, a.Attempt.Identity, LogChunk{Sequence: 1, Stream: "STDOUT", Payload: []byte("rollback")})
				}
				var pgErr *pgconn.PgError
				if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
					t.Fatalf("injected failure not returned: %v", err)
				}
				if after := snapshot(t, s, ctx); after != before {
					t.Fatalf("rollback left partial state\nbefore=%s\nafter=%s", before, after)
				}
				assertSlots(t, s, ctx)
			})
		}
	}
}
