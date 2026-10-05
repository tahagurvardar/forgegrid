//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"forgegrid/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func fixture(t *testing.T) (*Store, context.Context) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["application_name"] = schema
	cfg.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		clean, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if _, e := admin.Exec(clean, `DROP SCHEMA `+schema+` CASCADE`); e != nil {
			t.Error(e)
		}
		admin.Close()
	})
	s := &Store{Pool: pool, Lease: 10 * time.Second, Offline: 6 * time.Second, RetryBase: 0}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s, ctx
}
func register(t *testing.T, s *Store, ctx context.Context, worker string) string {
	t.Helper()
	id := uuid.NewString()
	if err := s.Register(ctx, worker, id); err != nil {
		t.Fatal(err)
	}
	return id
}
func submit(t *testing.T, s *Store, ctx context.Context, max int) string {
	t.Helper()
	id, err := s.Submit(ctx, domain.Spec{Image: "alpine:3.22", Command: []string{"echo", "hello"}, TimeoutSeconds: 30, MaxAttempts: max})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func claim(t *testing.T, s *Store, ctx context.Context, ids ...string) *domain.Assignment {
	t.Helper()
	a, err := s.Schedule(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	if a == nil {
		t.Fatal("no assignment")
	}
	return a
}
func expire(t *testing.T, s *Store, ctx context.Context, id string) {
	t.Helper()
	if _, err := s.Pool.Exec(ctx, `UPDATE job_attempts SET lease_expires_at=clock_timestamp()-interval '1 millisecond' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}
func success() domain.Result { return domain.Result{State: "SUCCEEDED", ExitCode: 0} }
func assertSlots(t *testing.T, s *Store, ctx context.Context) {
	t.Helper()
	var bad int
	err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM worker_sessions w WHERE active_slots<>(SELECT count(*) FROM job_attempts a WHERE a.worker_session_id=w.id AND a.state IN ('ASSIGNED','RUNNING'))`).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatal("slot accounting differs from active attempts")
	}
}
func TestCompletionDuplicatesAndFencing(t *testing.T) {
	s, ctx := fixture(t)
	session := register(t, s, ctx, "worker-b")
	job := submit(t, s, ctx, 2)
	a := claim(t, s, ctx, session)
	bad := a.Attempt.Identity
	bad.FencingToken++
	if _, err := s.Complete(ctx, bad, success()); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("fence accepted: %v", err)
	}
	bad = a.Attempt.Identity
	bad.SessionID = uuid.NewString()
	if _, err := s.Complete(ctx, bad, success()); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("session accepted: %v", err)
	}
	if _, err := s.Advance(ctx, a.Attempt.Identity, "accept"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(ctx, a.Attempt.Identity, "start"); err != nil {
		t.Fatal(err)
	}
	if dup, err := s.Complete(ctx, a.Attempt.Identity, success()); err != nil || dup {
		t.Fatalf("first completion: %v %v", dup, err)
	}
	expire(t, s, ctx, a.Attempt.AttemptID)
	if dup, err := s.Complete(ctx, a.Attempt.Identity, success()); err != nil || !dup {
		t.Fatalf("duplicate: %v %v", dup, err)
	}
	if _, err := s.Complete(ctx, a.Attempt.Identity, domain.Result{State: "FAILED", ExitCode: 1, FailureKind: "EXIT_NON_ZERO"}); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("conflicting result: %v", err)
	}
	j, err := s.GetJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != "SUCCEEDED" || j.AttemptCount != 1 {
		t.Fatalf("unexpected job %+v", j)
	}
	assertSlots(t, s, ctx)
}
func TestConcurrentDuplicateCompletion(t *testing.T) {
	s, ctx := fixture(t)
	session := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	a := claim(t, s, ctx, session)
	var wg sync.WaitGroup
	out := make(chan bool, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dup, err := s.Complete(ctx, a.Attempt.Identity, success())
			out <- dup
			errs <- err
		}()
	}
	wg.Wait()
	close(out)
	close(errs)
	first := 0
	for dup := range out {
		if !dup {
			first++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if first != 1 {
		t.Fatalf("%d authoritative completions", first)
	}
	assertSlots(t, s, ctx)
}
func TestSchedulerConcurrency(t *testing.T) {
	for _, jobs := range []int{1, 40} {
		t.Run(string(rune('A'+jobs)), func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			c := register(t, s, ctx, "worker-c")
			for i := 0; i < jobs; i++ {
				submit(t, s, ctx, 2)
			}
			var wg sync.WaitGroup
			out := make(chan *domain.Assignment, 32)
			errs := make(chan error, 32)
			for i := 0; i < 32; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); a, err := s.Schedule(ctx, []string{b, c}); out <- a; errs <- err }()
			}
			wg.Wait()
			close(out)
			close(errs)
			count := 0
			seen := map[string]bool{}
			for a := range out {
				if a != nil {
					count++
					if seen[a.Job.ID] {
						t.Fatal("duplicate claim")
					}
					seen[a.Job.ID] = true
				}
			}
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			want := 2
			if jobs == 1 {
				want = 1
			}
			if count != want {
				t.Fatalf("claims=%d want=%d", count, want)
			}
			assertSlots(t, s, ctx)
		})
	}
}
func TestWorkerBToWorkerCRecovery(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	c := register(t, s, ctx, "worker-c")
	job := submit(t, s, ctx, 2)
	a := claim(t, s, ctx, b, c)
	if a.Attempt.WorkerID != "worker-b" {
		t.Fatal("expected deterministic worker-b")
	}
	if _, err := s.Advance(ctx, a.Attempt.Identity, "start"); err != nil {
		t.Fatal(err)
	}
	if err := s.Disconnect(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE worker_sessions SET last_seen_at=clock_timestamp()-interval '7 seconds' WHERE id=$1`, b); err != nil {
		t.Fatal(err)
	}
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	j, err := s.GetJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != "RUNNING" || j.Attempts[0].State != "RUNNING" {
		t.Fatal("heartbeat loss transferred ownership")
	}
	if assigned, err := s.Schedule(ctx, []string{c}); err != nil || assigned != nil {
		t.Fatal("reassigned before lease expiry")
	}
	expire(t, s, ctx, a.Attempt.AttemptID)
	// Reject expiration independently of the recovery scan.
	if _, err := s.Complete(ctx, a.Attempt.Identity, success()); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("expired completion: %v", err)
	}
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	second := claim(t, s, ctx, c)
	if second.Attempt.Number != 2 || second.Attempt.FencingToken != 2 || second.Attempt.WorkerID != "worker-c" || second.Attempt.AttemptID == a.Attempt.AttemptID {
		t.Fatalf("bad retry %+v", second)
	}
	if _, err := s.Complete(ctx, second.Attempt.Identity, success()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Complete(ctx, a.Attempt.Identity, success()); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("old attempt accepted: %v", err)
	}
	j, err = s.GetJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if j.State != "SUCCEEDED" || j.Attempts[0].State != "LOST" || j.Attempts[1].State != "SUCCEEDED" {
		t.Fatalf("bad recovery %+v", j)
	}
	assertSlots(t, s, ctx)
}
func TestLeaseExpirationCompletionRace(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	a := claim(t, s, ctx, b)
	expire(t, s, ctx, a.Attempt.AttemptID)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		if _, err := s.Complete(ctx, a.Attempt.Identity, success()); !errors.Is(err, domain.ErrStale) {
			t.Errorf("expired result accepted: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		if err := s.Recover(ctx); err != nil {
			t.Error(err)
		}
	}()
	close(start)
	wg.Wait()
	j, err := s.GetJob(ctx, a.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j.Attempts[0].State != "LOST" {
		t.Fatal("expired result won")
	}
	assertSlots(t, s, ctx)
}
func TestCompletionBlockedPastLeaseDeadline(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	a := claim(t, s, ctx, b)
	blocker, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(ctx)
	if _, err = blocker.Exec(ctx, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, a.Job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = blocker.Exec(ctx, `UPDATE job_attempts SET lease_expires_at=clock_timestamp()+interval '200 milliseconds' WHERE id=$1`, a.Attempt.AttemptID); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := s.Complete(ctx, a.Attempt.Identity, success()); done <- e }()
	// Observe the actual PostgreSQL waiter before releasing the job lock after expiry.
	deadline := time.Now().Add(3 * time.Second)
	for {
		var waiting bool
		err = s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("completion did not block")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err = blocker.Exec(ctx, `SELECT pg_sleep(0.25)`); err != nil {
		t.Fatal(err)
	}
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, domain.ErrStale) {
		t.Fatalf("transaction-start time allowed expired completion: %v", err)
	}
	if _, err = s.Advance(ctx, a.Attempt.Identity, "renew"); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("expired renewal accepted: %v", err)
	}
}
func TestSessionSupersession(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	a := claim(t, s, ctx, b)
	next := register(t, s, ctx, "worker-b")
	if err := s.Heartbeat(ctx, b); !errors.Is(err, domain.ErrSession) {
		t.Fatalf("old heartbeat: %v", err)
	}
	if _, err := s.Advance(ctx, a.Attempt.Identity, "renew"); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("old renewal: %v", err)
	}
	if _, err := s.Complete(ctx, a.Attempt.Identity, success()); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("superseded completion: %v", err)
	}
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if retry, err := s.Schedule(ctx, []string{next}); err != nil || retry != nil {
		t.Fatal("supersession transferred unexpired ownership")
	}
	expire(t, s, ctx, a.Attempt.AttemptID)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	claim(t, s, ctx, next)
	assertSlots(t, s, ctx)
}
func TestRetryBoundsAndWorkloadFailures(t *testing.T) {
	for _, kind := range []string{"EXIT_NON_ZERO", "JOB_TIMEOUT", "EXECUTOR_INFRA_ERROR", "LEASE_EXPIRED"} {
		t.Run(kind, func(t *testing.T) {
			s, ctx := fixture(t)
			b := register(t, s, ctx, "worker-b")
			id := submit(t, s, ctx, 2)
			for n := 1; n <= 2; n++ {
				a := claim(t, s, ctx, b)
				if kind == "LEASE_EXPIRED" {
					expire(t, s, ctx, a.Attempt.AttemptID)
					if err := s.Recover(ctx); err != nil {
						t.Fatal(err)
					}
				} else {
					state := "FAILED"
					if kind == "JOB_TIMEOUT" {
						state = "TIMED_OUT"
					}
					if _, err := s.Complete(ctx, a.Attempt.Identity, domain.Result{State: state, ExitCode: 1, FailureKind: kind}); err != nil {
						t.Fatal(err)
					}
					if err := s.Recover(ctx); err != nil {
						t.Fatal(err)
					}
				}
				j, err := s.GetJob(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if !domain.Retryable(kind) {
					if j.State != "FAILED" || j.AttemptCount != 1 {
						t.Fatal("workload failure retried")
					}
					break
				}
				if n == 2 && j.State != "FAILED" {
					t.Fatal("retry limit exceeded")
				}
			}
			if a, err := s.Schedule(ctx, []string{b}); err != nil || a != nil {
				t.Fatal("terminal job rescheduled")
			}
			assertSlots(t, s, ctx)
		})
	}
}
func TestLogIdempotency(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	submit(t, s, ctx, 2)
	a := claim(t, s, ctx, b)
	c := LogChunk{Sequence: 1, Stream: "STDOUT", Payload: []byte("hello")}
	if err := s.AppendLog(ctx, a.Attempt.Identity, c); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLog(ctx, a.Attempt.Identity, c); err != nil {
		t.Fatal(err)
	}
	bad := a.Attempt.Identity
	bad.FencingToken++
	if err := s.AppendLog(ctx, bad, c); !errors.Is(err, domain.ErrStale) {
		t.Fatal("incorrect log identity accepted")
	}
	chunks, err := s.Logs(ctx, a.Attempt.AttemptID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || string(chunks[0].Payload) != "hello" {
		t.Fatal("duplicate logs persisted")
	}
}
