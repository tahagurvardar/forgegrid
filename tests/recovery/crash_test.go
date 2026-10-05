//go:build recovery

package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/controlplane"
	"forgegrid/internal/domain"
	"forgegrid/internal/store/postgres"
	"forgegrid/internal/testutil"
	"forgegrid/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ready struct {
	Address    string
	Assignment *domain.Assignment
}

// A real OS process hosts production components. No simulated process death,
// transaction mocks, or production fault switches are involved.
func TestCrashHelperProcess(t *testing.T) {
	mode := os.Getenv("FORGEGRID_TEST_PROCESS")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = os.Getenv("FORGEGRID_TEST_SCHEMA")
	cfg.ConnConfig.RuntimeParams["application_name"] = os.Getenv("FORGEGRID_TEST_SCHEMA")
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := &postgres.Store{Pool: pool, Lease: 10 * time.Second, Offline: 6 * time.Second, RetryBase: 100 * time.Millisecond}
	announce := func(r ready) {
		if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	switch mode {
	case "commit":
		a, err := s.Schedule(ctx, []string{os.Getenv("FORGEGRID_TEST_SESSION")})
		if err != nil || a == nil {
			t.Fatalf("schedule: %v %v", a, err)
		}
		announce(ready{Assignment: a})
		select {}
	case "serve":
		if err := s.ResetConnections(ctx); err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		server := controlplane.New(s, 2*time.Second)
		transport := grpc.NewServer()
		pb.RegisterWorkerControlServer(transport, server)
		pb.RegisterWorkerLogsServer(transport, server)
		go server.Run(ctx)
		announce(ready{Address: listener.Addr().String()})
		if err := transport.Serve(listener); err != nil {
			t.Fatal(err)
		}
	case "worker":
		announce(ready{})
		if err := worker.Run(ctx, os.Getenv("FORGEGRID_TEST_ADDRESS"), os.Getenv("FORGEGRID_TEST_WORKER"), 2*time.Second); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
}

type process struct {
	cmd    *exec.Cmd
	done   chan error
	stderr bytes.Buffer
	once   sync.Once
}

func (p *process) kill(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		_ = p.cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("killed child did not exit")
		}
	})
}
func start(t *testing.T, ctx context.Context, s *postgres.Store, mode string, vars map[string]string) (*process, ready) {
	t.Helper()
	p := &process{done: make(chan error, 1)}
	p.cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashHelperProcess$")
	p.cmd.Env = append(os.Environ(), "FORGEGRID_TEST_PROCESS="+mode, "FORGEGRID_TEST_SCHEMA="+s.Pool.Config().ConnConfig.RuntimeParams["search_path"])
	for k, v := range vars {
		p.cmd.Env = append(p.cmd.Env, k+"="+v)
	}
	out, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd.Stderr = &p.stderr
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.done <- p.cmd.Wait() }()
	t.Cleanup(func() { p.kill(t) })
	result := make(chan struct {
		r   ready
		err error
	}, 1)
	go func() {
		var r ready
		err := json.NewDecoder(out).Decode(&r)
		result <- struct {
			r   ready
			err error
		}{r, err}
	}()
	select {
	case v := <-result:
		if v.err != nil {
			p.kill(t)
			t.Fatalf("%s startup: %v\n%s", mode, v.err, p.stderr.String())
		}
		return p, v.r
	case <-time.After(10 * time.Second):
		p.kill(t)
		t.Fatalf("%s startup timeout\n%s", mode, p.stderr.String())
	}
	return nil, ready{}
}
func fixture(t *testing.T) (*postgres.Store, context.Context) {
	pool, ctx := testutil.DatabaseWithTimeout(t, 90*time.Second)
	s := &postgres.Store{Pool: pool, Lease: 10 * time.Second, Offline: 6 * time.Second, RetryBase: 100 * time.Millisecond}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s, ctx
}
func wait(t *testing.T, ctx context.Context, description string, check func() bool) {
	t.Helper()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", description, ctx.Err())
		case <-tick.C:
		}
	}
}
func job(t *testing.T, ctx context.Context, s *postgres.Store, id string) domain.Job {
	t.Helper()
	j, err := s.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func submit(t *testing.T, ctx context.Context, s *postgres.Store, seconds string) string {
	t.Helper()
	id, err := s.Submit(ctx, domain.Spec{Image: "alpine:3.22", Command: []string{"sleep", seconds}, TimeoutSeconds: 60, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func dockerRunning(ctx context.Context, attempt string) bool {
	out, err := exec.CommandContext(ctx, "docker", "ps", "--filter", "label=forgegrid.attempt_id="+attempt, "--format", "{{.ID}}").Output()
	return err == nil && strings.TrimSpace(string(out)) != ""
}
func cleanupContainers(t *testing.T, workerID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", "ps", "-aq", "--filter", "label=forgegrid.worker_id="+workerID).Output()
		if err != nil {
			t.Error(err)
			return
		}
		for _, id := range strings.Fields(string(out)) {
			if out, err := exec.CommandContext(ctx, "docker", "rm", "-f", id).CombinedOutput(); err != nil {
				t.Errorf("remove own container: %v %s", err, out)
			}
		}
	})
}
func startWorker(t *testing.T, ctx context.Context, s *postgres.Store, address, workerID string) *process {
	t.Helper()
	cleanupContainers(t, workerID)
	p, _ := start(t, ctx, s, "worker", map[string]string{"FORGEGRID_TEST_ADDRESS": address, "FORGEGRID_TEST_WORKER": workerID})
	return p
}
func assertReserved(t *testing.T, ctx context.Context, s *postgres.Store, id string, old domain.Attempt) {
	t.Helper()
	j := job(t, ctx, s, id)
	if j.AttemptCount != 1 || j.FencingToken != old.FencingToken || j.CurrentAttemptID == nil || *j.CurrentAttemptID != old.AttemptID || !domain.Active(j.Attempts[0].State) {
		t.Fatalf("ownership transferred early: %+v", j)
	}
	var slots int
	if err := s.Pool.QueryRow(ctx, `SELECT active_slots FROM worker_sessions WHERE id=$1`, old.SessionID).Scan(&slots); err != nil || slots != 1 {
		t.Fatalf("reserved slots=%d err=%v", slots, err)
	}
}
func assertRecovered(t *testing.T, ctx context.Context, s *postgres.Store, address, id, workerC string, old domain.Attempt) {
	t.Helper()
	wait(t, ctx, "replacement succeeded", func() bool { return job(t, ctx, s, id).State == "SUCCEEDED" })
	j := job(t, ctx, s, id)
	if len(j.Attempts) != 2 || j.Attempts[0].State != "LOST" || j.Attempts[1].State != "SUCCEEDED" || j.Attempts[1].WorkerID != workerC || j.Attempts[1].AttemptID == old.AttemptID || j.FencingToken != old.FencingToken+1 || *j.CurrentAttemptID != j.Attempts[1].AttemptID {
		t.Fatalf("recovery: %+v", j)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ack, err := pb.NewWorkerControlClient(conn).ReportResult(ctx, &pb.AttemptResult{Identity: &pb.AttemptIdentity{AttemptId: old.AttemptID, FencingToken: old.FencingToken, WorkerSessionId: old.SessionID}, State: "SUCCEEDED"})
	if err != nil || ack.Accepted || ack.Error != domain.ErrStale.Error() {
		t.Fatalf("delayed old completion: %v %v", ack, err)
	}
	if _, err := s.Advance(ctx, old.Identity, "renew"); !errors.Is(err, domain.ErrStale) {
		t.Fatalf("delayed old renewal: %v", err)
	}
	var slots int
	if err := s.Pool.QueryRow(ctx, `SELECT COALESCE(sum(active_slots),0) FROM worker_sessions`).Scan(&slots); err != nil || slots != 0 {
		t.Fatalf("final slots=%d err=%v", slots, err)
	}
}

func TestControlPlaneCrashImmediatelyAfterAssignmentCommit(t *testing.T) {
	s, ctx := fixture(t)
	b := uuid.NewString()
	if err := s.Register(ctx, "worker-b", b); err != nil {
		t.Fatal(err)
	}
	id := submit(t, ctx, s, "1")
	child, r := start(t, ctx, s, "commit", map[string]string{"FORGEGRID_TEST_SESSION": b})
	old := r.Assignment.Attempt
	child.kill(t)
	assertReserved(t, ctx, s, id, old)
	_, cp := start(t, ctx, s, "serve", nil)
	workerC := "worker-c-" + uuid.NewString()
	startWorker(t, ctx, s, cp.Address, workerC)
	wait(t, ctx, "worker-c registration", func() bool {
		sessions, err := s.Sessions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range sessions {
			if v.WorkerID == workerC {
				return true
			}
		}
		return false
	})
	assertReserved(t, ctx, s, id, old)
	assertRecovered(t, ctx, s, cp.Address, id, workerC, old)
}

func TestControlPlaneCrashAfterDockerStartsBeforeStartedCommit(t *testing.T) {
	s, ctx := fixture(t)
	gate, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(context.Background())
	if _, err = gate.Exec(ctx, `SELECT pg_advisory_xact_lock(982317)`); err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool.Exec(ctx, `CREATE FUNCTION block_started() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(982317); RETURN NEW; END $$; CREATE TRIGGER block_started AFTER UPDATE ON job_attempts FOR EACH ROW WHEN (NEW.state='RUNNING') EXECUTE FUNCTION block_started()`)
	if err != nil {
		t.Fatal(err)
	}
	cpProcess, cp := start(t, ctx, s, "serve", nil)
	workerB := "worker-b-" + uuid.NewString()
	startWorker(t, ctx, s, cp.Address, workerB)
	id := submit(t, ctx, s, "15")
	wait(t, ctx, "blocked AttemptStarted transaction", func() bool {
		var blocked bool
		err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event='advisory')`, s.Pool.Config().ConnConfig.RuntimeParams["search_path"]).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		return blocked
	})
	j := job(t, ctx, s, id)
	if j.State != "DISPATCHED" || len(j.Attempts) != 1 || j.Attempts[0].State != "ASSIGNED" {
		t.Fatalf("start persisted before crash: %+v", j)
	}
	old := j.Attempts[0]
	if !dockerRunning(ctx, old.AttemptID) {
		t.Fatal("Docker must be physically running before crash")
	}
	cpProcess.kill(t)
	// PostgreSQL need not poll a dead client's socket during an advisory wait.
	// Release the test gate so it can observe EOF and abort the explicit tx;
	// the killed process cannot issue the subsequent statements or COMMIT.
	if err := gate.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, "crashed transaction rolled back", func() bool {
		var blocked bool
		err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event='advisory')`, s.Pool.Config().ConnConfig.RuntimeParams["search_path"]).Scan(&blocked)
		if err != nil {
			t.Fatal(err)
		}
		return !blocked
	})
	if j := job(t, ctx, s, id); j.State != "DISPATCHED" || j.Attempts[0].State != "ASSIGNED" {
		t.Fatalf("crashed start transaction committed: %+v", j)
	}
	if _, err := s.Pool.Exec(ctx, `DROP TRIGGER block_started ON job_attempts; DROP FUNCTION block_started()`); err != nil {
		t.Fatal(err)
	}
	assertReserved(t, ctx, s, id, old)
	_, restarted := start(t, ctx, s, "serve", nil)
	workerC := "worker-c-" + uuid.NewString()
	startWorker(t, ctx, s, restarted.Address, workerC)
	assertRecovered(t, ctx, s, restarted.Address, id, workerC, old)
	if dockerRunning(ctx, old.AttemptID) {
		t.Fatal("surviving worker failed to kill execution after renewal ACKs stopped")
	}
}

func TestWorkerCrashLeavesDockerAliveUntilRecovery(t *testing.T) {
	s, ctx := fixture(t)
	_, cp := start(t, ctx, s, "serve", nil)
	workerB := "worker-b-" + uuid.NewString()
	b := startWorker(t, ctx, s, cp.Address, workerB)
	id := submit(t, ctx, s, "20")
	wait(t, ctx, "running worker-b attempt", func() bool { return job(t, ctx, s, id).State == "RUNNING" })
	old := job(t, ctx, s, id).Attempts[0]
	b.kill(t)
	if !dockerRunning(ctx, old.AttemptID) {
		t.Fatal("agent crash should leave independent Docker container alive")
	}
	assertReserved(t, ctx, s, id, old)
	workerC := "worker-c-" + uuid.NewString()
	startWorker(t, ctx, s, cp.Address, workerC)
	wait(t, ctx, "offline worker-b before lease expiry", func() bool {
		var state string
		err := s.Pool.QueryRow(ctx, `SELECT state FROM worker_sessions WHERE id=$1`, old.SessionID).Scan(&state)
		if err != nil {
			t.Fatal(err)
		}
		return state == "OFFLINE"
	})
	assertReserved(t, ctx, s, id, old)
	wait(t, ctx, "replacement physically started", func() bool { return len(job(t, ctx, s, id).Attempts) == 2 && job(t, ctx, s, id).State == "RUNNING" })
	if !dockerRunning(ctx, old.AttemptID) {
		t.Fatal("expected overlapping physical executions after expiry")
	}
	// A fresh incarnation of worker-b reconciles its old labelled container.
	startWorker(t, ctx, s, cp.Address, workerB)
	wait(t, ctx, "new worker-b reconciled orphan", func() bool { return !dockerRunning(ctx, old.AttemptID) })
	j := job(t, ctx, s, id)
	if len(j.Attempts) != 2 || j.Attempts[1].WorkerID != workerC {
		t.Fatalf("reconciliation changed authority: %+v", j)
	}
	assertRecovered(t, ctx, s, cp.Address, id, workerC, old)
}

func TestInternalCancellationStopsRealDockerBeforeReleasingSlot(t *testing.T) {
	s, ctx := fixture(t)
	_, cp := start(t, ctx, s, "serve", nil)
	startWorker(t, ctx, s, cp.Address, "worker-b-"+uuid.NewString())
	id := submit(t, ctx, s, "20")
	wait(t, ctx, "running execution", func() bool { return job(t, ctx, s, id).State == "RUNNING" })
	old := job(t, ctx, s, id).Attempts[0]
	if !dockerRunning(ctx, old.AttemptID) {
		t.Fatal("missing live container")
	}
	if _, err := s.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	assertReserved(t, ctx, s, id, old)
	// The next renewal discovers the durable cancel intent; no public cancel API.
	wait(t, ctx, "stopped execution acknowledgement", func() bool { return job(t, ctx, s, id).State == "CANCELLED" })
	j := job(t, ctx, s, id)
	if len(j.Attempts) != 1 || j.Attempts[0].State != "CANCELLED" || dockerRunning(ctx, old.AttemptID) {
		t.Fatalf("cancelled before physical stop: %+v", j)
	}
	var slots int
	err := s.Pool.QueryRow(ctx, `SELECT active_slots FROM worker_sessions WHERE id=$1`, old.SessionID).Scan(&slots)
	if err != nil || slots != 0 {
		t.Fatal(fmt.Sprintf("slots=%d err=%v", slots, err))
	}
}
