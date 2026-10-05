package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"forgegrid/db/migrations"
	"forgegrid/internal/domain"
	"forgegrid/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Store struct {
	Pool                      *pgxpool.Pool
	Lease, Offline, RetryBase time.Duration
	Metrics                   *observability.Metrics
}

func Open(ctx context.Context, url string, lease, offline, retry time.Duration) (*Store, error) {
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Store{Pool: p, Lease: lease, Offline: offline, RetryBase: retry}, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(734128)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, migrations.Initial); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, migrations.InternalCancellation); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, migrations.PipelineSemantics); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, migrations.Observability); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Submit(ctx context.Context, spec domain.Spec) (string, error) {
	ctx, span := observability.Start(ctx, "job.submit")
	defer span.End()
	if err := spec.Validate(); err != nil {
		return "", err
	}
	id := uuid.NewString()
	argv, err := json.Marshal(spec.Command)
	if err != nil {
		return "", err
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO jobs(id,state,image,command,timeout_seconds,max_attempts,trace_parent,last_queued_at) VALUES($1,'QUEUED',$2,$3,$4,$5,$6,clock_timestamp())`, id, spec.Image, argv, spec.TimeoutSeconds, spec.MaxAttempts, observability.Carrier(ctx))
	return id, err
}
func (s *Store) Register(ctx context.Context, worker, session string) error {
	if len(worker) < 1 || len(worker) > 128 {
		return errors.New("worker_id must be 1..128 bytes")
	}
	if _, err := uuid.Parse(session); err != nil {
		return err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO workers(id) VALUES($1) ON CONFLICT DO NOTHING`, worker); err != nil {
		return err
	}
	var locked string
	if err = tx.QueryRow(ctx, `SELECT id FROM workers WHERE id=$1 FOR UPDATE`, worker).Scan(&locked); err != nil {
		return err
	}
	// Incarnations cannot be revived or reused. Registration only locks worker then sessions.
	if _, err = tx.Exec(ctx, `UPDATE worker_sessions SET state='SUPERSEDED',connected=false WHERE worker_id=$1 AND state='ONLINE'`, worker); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO worker_sessions(id,worker_id,state,capacity_slots) VALUES($1,$2,'ONLINE',1)`, session, worker); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Heartbeat(ctx context.Context, session string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE worker_sessions SET last_seen_at=clock_timestamp() WHERE id=$1 AND state='ONLINE' AND connected`, session)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrSession
	}
	return nil
}
func (s *Store) Disconnect(ctx context.Context, session string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE worker_sessions SET connected=false,disconnected_at=clock_timestamp() WHERE id=$1`, session)
	return err
}

const jobColumns = `id::text,state,image,command,timeout_seconds,max_attempts,current_attempt_id::text,fencing_token,attempt_count,pipeline_id::text,job_key,trace_parent,created_at,finished_at,retry_available_at`

func scanJob(row pgx.Row) (domain.Job, error) {
	var j domain.Job
	var argv []byte
	err := row.Scan(&j.ID, &j.State, &j.Image, &argv, &j.TimeoutSeconds, &j.MaxAttempts, &j.CurrentAttemptID, &j.FencingToken, &j.AttemptCount, &j.PipelineID, &j.Key, &j.TraceParent, &j.CreatedAt, &j.FinishedAt, &j.RetryAvailableAt)
	if err == nil {
		err = json.Unmarshal(argv, &j.Command)
	}
	return j, err
}

const attemptColumns = `a.id::text,a.job_id::text,a.attempt_number,a.fencing_token,a.worker_session_id::text,a.state,a.lease_expires_at,a.exit_code,a.failure_kind,a.failure_detail,w.worker_id,a.execution_deadline_at,a.trace_parent,a.assigned_at,a.started_at,a.finished_at`

func scanAttempt(row pgx.Row) (domain.Attempt, error) {
	var a domain.Attempt
	err := row.Scan(&a.AttemptID, &a.JobID, &a.Number, &a.FencingToken, &a.SessionID, &a.State, &a.LeaseExpiresAt, &a.ExitCode, &a.FailureKind, &a.FailureDetail, &a.WorkerID, &a.ExecutionDeadline, &a.TraceParent, &a.AssignedAt, &a.StartedAt, &a.FinishedAt)
	if sc := trace.SpanContextFromContext(observability.Parent(context.Background(), a.TraceParent)); sc.IsValid() {
		a.TraceID = sc.TraceID().String()
	}
	return a, err
}
func (s *Store) GetJob(ctx context.Context, id string) (domain.Job, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx)
	j, err := getJob(ctx, tx, id)
	if err != nil {
		return j, err
	}
	return j, tx.Commit(ctx)
}
func getJob(ctx context.Context, tx pgx.Tx, id string) (domain.Job, error) {
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1`, id))
	if err != nil {
		return j, err
	}
	rows, err := tx.Query(ctx, `SELECT `+attemptColumns+` FROM job_attempts a JOIN worker_sessions w ON w.id=a.worker_session_id WHERE a.job_id=$1 ORDER BY a.attempt_number`, id)
	if err != nil {
		return j, err
	}
	j.Attempts = []domain.Attempt{}
	for rows.Next() {
		a, e := scanAttempt(rows)
		if e != nil {
			rows.Close()
			return j, e
		}
		j.Attempts = append(j.Attempts, a)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return j, err
	}
	if j.PipelineID != nil {
		if err = tx.QueryRow(ctx, `SELECT COALESCE(array_agg(parent.job_key ORDER BY parent.job_key),'{}'::text[]) FROM job_dependencies d JOIN jobs parent ON parent.id=d.depends_on_job_id WHERE d.job_id=$1`, id).Scan(&j.Dependencies); err != nil {
			return j, err
		}
	}
	return j, nil
}
func (s *Store) Schedule(ctx context.Context, connected []string) (*domain.Assignment, error) {
	if len(connected) == 0 {
		return nil, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT id::text FROM jobs WHERE state='QUEUED' AND attempt_count<max_attempts ORDER BY created_at,id LIMIT 128`)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		a, err := s.scheduleJob(ctx, id, connected)
		if err != nil || a != nil {
			return a, err
		}
	}
	return nil, nil
}
func (s *Store) scheduleJob(ctx context.Context, id string, connected []string) (*domain.Assignment, error) {
	started := time.Now()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	locked, err := lockPipelineForJob(ctx, tx, id, true)
	if err != nil || !locked {
		return nil, err
	}
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1 AND state='QUEUED' AND attempt_count<max_attempts FOR UPDATE SKIP LOCKED`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var session, worker string
	ctx, claimSpan := observability.Start(observability.Parent(ctx, j.TraceParent), "scheduler.claim_job", attribute.String("job_id", j.ID))
	defer claimSpan.End()
	claimSpan.SetAttributes(attribute.Bool("committed", false))
	_, selectSpan := observability.Start(ctx, "scheduler.select_worker")
	err = tx.QueryRow(ctx, `SELECT id::text,worker_id FROM worker_sessions WHERE state='ONLINE' AND connected AND id::text=ANY($1::text[]) AND last_seen_at>clock_timestamp()-$2*interval '1 millisecond' AND active_slots<capacity_slots ORDER BY active_slots,last_assignment_at NULLS FIRST,worker_id,id FOR UPDATE SKIP LOCKED LIMIT 1`, connected, s.Offline.Milliseconds()).Scan(&session, &worker)
	observability.End(selectSpan, err)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a := domain.Attempt{Identity: domain.Identity{AttemptID: uuid.NewString(), FencingToken: j.FencingToken + 1, SessionID: session}, JobID: j.ID, Number: j.AttemptCount + 1, WorkerID: worker, State: "ASSIGNED"}
	claimSpan.SetAttributes(observability.Attrs(j, a)...)
	ctx, attemptSpan := observability.Start(ctx, "scheduler.create_attempt", observability.Attrs(j, a)...)
	defer attemptSpan.End()
	attemptSpan.SetAttributes(attribute.Bool("committed", false))
	a.TraceParent = observability.Carrier(ctx)
	err = tx.QueryRow(ctx, `INSERT INTO job_attempts(id,job_id,attempt_number,fencing_token,worker_session_id,state,lease_expires_at,trace_parent,queue_wait_seconds) VALUES($1,$2,$3,$4,$5,'ASSIGNED',clock_timestamp()+$6*interval '1 millisecond',$7,(SELECT greatest(0,extract(epoch FROM (clock_timestamp()-coalesce(last_queued_at,created_at)))) FROM jobs WHERE id=$2)) RETURNING lease_expires_at`, a.AttemptID, j.ID, a.Number, a.FencingToken, session, s.Lease.Milliseconds(), a.TraceParent).Scan(&a.LeaseExpiresAt)
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `UPDATE jobs SET state='DISPATCHED',current_attempt_id=$2,fencing_token=$3,attempt_count=$4,retry_available_at=NULL WHERE id=$1`, j.ID, a.AttemptID, a.FencingToken, a.Number); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `UPDATE worker_sessions SET active_slots=active_slots+1,last_assignment_at=clock_timestamp() WHERE id=$1`, session); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	j.State = "DISPATCHED"
	j.CurrentAttemptID = &a.AttemptID
	j.FencingToken = a.FencingToken
	j.AttemptCount = a.Number
	claimSpan.SetAttributes(attribute.Bool("committed", true))
	attemptSpan.SetAttributes(attribute.Bool("committed", true))
	if s.Metrics != nil {
		s.Metrics.Assignment.Observe(time.Since(started).Seconds())
	}
	slog.InfoContext(ctx, "attempt committed", observability.Fields(ctx, j, a)...)
	return &domain.Assignment{Job: j, Attempt: a}, nil
}

// All ownership operations lock job -> attempt -> session, then obtain database time.
// The first unlocked lookup only reads the immutable attempt-to-job identity.
func lockOwner(ctx context.Context, tx pgx.Tx, id domain.Identity) (domain.Job, domain.Attempt, string, time.Time, error) {
	var jobID string
	var j domain.Job
	var a domain.Attempt
	var state string
	var now time.Time
	err := tx.QueryRow(ctx, `SELECT job_id::text FROM job_attempts WHERE id=$1`, id.AttemptID).Scan(&jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = domain.ErrStale
	}
	if err != nil {
		return j, a, state, now, err
	}
	if _, err = lockPipelineForJob(ctx, tx, jobID, false); err != nil {
		return j, a, state, now, err
	}
	j, err = scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1 FOR UPDATE`, jobID))
	if err != nil {
		return j, a, state, now, err
	}
	a, err = scanAttempt(tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM job_attempts a JOIN worker_sessions w ON w.id=a.worker_session_id WHERE a.id=$1 FOR UPDATE OF a`, id.AttemptID))
	if err != nil {
		return j, a, state, now, err
	}
	err = tx.QueryRow(ctx, `SELECT state FROM worker_sessions WHERE id=$1 FOR UPDATE`, a.SessionID).Scan(&state)
	if err != nil {
		return j, a, state, now, err
	}
	err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return j, a, state, now, err
}
func (s *Store) Advance(ctx context.Context, id domain.Identity, action string) (time.Duration, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	j, a, session, now, err := lockOwner(ctx, tx, id)
	if err != nil {
		return 0, err
	}
	name := "controlplane." + action
	if action == "renew" {
		name = "lease.renew"
	}
	ctx, span := observability.Start(observability.Continue(ctx, a.TraceParent), name, observability.Attrs(j, a)...)
	defer span.End()
	span.SetAttributes(attribute.Bool("committed", false))
	if !domain.Owns(j, a, id, now) || session != "ONLINE" {
		return 0, domain.ErrStale
	}
	if j.State == "CANCELLING" {
		return 0, domain.ErrCancelled
	}
	if timedOut(a, now) {
		return 0, domain.ErrTimeout
	}
	var connected bool
	if err = tx.QueryRow(ctx, `SELECT connected FROM worker_sessions WHERE id=$1`, id.SessionID).Scan(&connected); err != nil {
		return 0, err
	}
	if !connected {
		return 0, domain.ErrSession
	}
	switch action {
	case "accept":
		_, err = tx.Exec(ctx, `UPDATE job_attempts SET accepted_at=COALESCE(accepted_at,clock_timestamp()) WHERE id=$1`, id.AttemptID)
	case "start":
		if _, err = tx.Exec(ctx, `UPDATE job_attempts SET state='RUNNING',started_at=COALESCE(started_at,clock_timestamp()),execution_deadline_at=COALESCE(execution_deadline_at,clock_timestamp()+$2*interval '1 second') WHERE id=$1`, id.AttemptID, j.TimeoutSeconds); err == nil {
			_, err = tx.Exec(ctx, `UPDATE jobs SET state='RUNNING' WHERE id=$1`, j.ID)
		}
	case "renew":
		_, err = tx.Exec(ctx, `UPDATE job_attempts SET lease_expires_at=clock_timestamp()+$2*interval '1 millisecond',execution_deadline_at=COALESCE(execution_deadline_at,clock_timestamp()+$3*interval '1 second') WHERE id=$1`, id.AttemptID, s.Lease.Milliseconds(), j.TimeoutSeconds)
	default:
		return 0, errors.New("invalid action")
	}
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	span.SetAttributes(attribute.Bool("committed", true))
	return s.Lease, nil
}
func (s *Store) Complete(ctx context.Context, id domain.Identity, r domain.Result) (duplicate bool, completionErr error) {
	defer func() {
		if s.Metrics != nil && errors.Is(completionErr, domain.ErrStale) {
			s.Metrics.Stale.Inc()
		}
	}()
	if err := r.Validate(); err != nil {
		return false, err
	}
	r.Detail = domain.BoundedDetail(r.Detail)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	j, a, session, now, err := lockOwner(ctx, tx, id)
	if err != nil {
		return false, err
	}
	ctx, span := observability.Start(observability.Continue(ctx, a.TraceParent), "controlplane.complete_attempt", observability.Attrs(j, a)...)
	defer func() {
		if completionErr != nil {
			reason := "DATABASE_ERROR"
			for _, category := range []error{domain.ErrStale, domain.ErrCancelled, domain.ErrConflict, domain.ErrTimeout, domain.ErrSession} {
				if errors.Is(completionErr, category) {
					reason = category.Error()
					break
				}
			}
			fields := observability.Fields(ctx, j, a)
			fields = append(fields, "rejection", reason, "requested_fencing_token", id.FencingToken)
			slog.WarnContext(ctx, "attempt result rejected", fields...)
		}
		observability.End(span, completionErr)
	}()
	span.SetAttributes(attribute.Bool("committed", false), attribute.String("result", r.State), attribute.String("failure_kind", r.FailureKind))
	// An exact duplicate of the current winner is acknowledged without mutating anything.
	if j.CurrentAttemptID == nil || *j.CurrentAttemptID != id.AttemptID || j.FencingToken != id.FencingToken || a.FencingToken != id.FencingToken || a.SessionID != id.SessionID {
		return false, domain.ErrStale
	}
	if !domain.Active(a.State) {
		if a.State == "LOST" {
			return false, domain.ErrStale
		}
		if a.State == r.State && a.ExitCode != nil && *a.ExitCode == r.ExitCode && a.FailureKind == r.FailureKind && a.FailureDetail == r.Detail {
			span.SetAttributes(attribute.Bool("duplicate", true))
			return true, nil
		}
		return false, domain.ErrConflict
	}
	if !domain.Owns(j, a, id, now) || session != "ONLINE" {
		return false, domain.ErrStale
	}
	if j.State == "CANCELLING" && r.State != "CANCELLED" {
		return false, domain.ErrCancelled
	}
	if j.State != "CANCELLING" && r.State == "CANCELLED" {
		return false, domain.ErrConflict
	}
	if j.State != "CANCELLING" && timedOut(a, now) && r.State != "TIMED_OUT" {
		return false, domain.ErrTimeout
	}
	if err = s.finish(ctx, tx, j, a, r); err != nil {
		return false, err
	}
	err = tx.Commit(ctx)
	span.SetAttributes(attribute.Bool("committed", err == nil))
	if err == nil {
		fields := observability.Fields(ctx, j, a)
		fields = append(fields, "result", r.State, "failure_kind", r.FailureKind)
		slog.InfoContext(ctx, "attempt finalized", fields...)
	}
	return false, err
}
func (s *Store) finish(ctx context.Context, tx pgx.Tx, j domain.Job, a domain.Attempt, r domain.Result) error {
	if _, err := tx.Exec(ctx, `UPDATE job_attempts SET state=$2,exit_code=$3,failure_kind=$4,failure_detail=$5,finished_at=clock_timestamp() WHERE id=$1`, a.AttemptID, r.State, r.ExitCode, r.FailureKind, domain.BoundedDetail(r.Detail)); err != nil {
		return err
	}
	next := domain.NextJobState(j.AttemptCount, j.MaxAttempts, r)
	if j.State == "CANCELLING" {
		next = "CANCELLED"
	}
	if next == "RETRY_WAIT" {
		var spanCtx context.Context
		spanCtx, retrySpan := observability.Start(ctx, "recovery.retry_job", attribute.String("reason", r.FailureKind), attribute.Int("next_attempt_number", a.Number+1))
		ctx = spanCtx
		defer retrySpan.End()
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET state=$2,retry_available_at=CASE WHEN $2='RETRY_WAIT' THEN clock_timestamp()+$3*interval '1 millisecond' ELSE NULL END,finished_at=CASE WHEN $2 IN ('SUCCEEDED','FAILED','CANCELLED') THEN clock_timestamp() ELSE NULL END,trace_parent=$4 WHERE id=$1`, j.ID, next, domain.RetryDelay(a.Number, s.RetryBase).Milliseconds(), observability.Carrier(ctx)); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE worker_sessions SET active_slots=active_slots-1 WHERE id=$1`, a.SessionID)
	if err != nil {
		return err
	}
	return refreshPipeline(ctx, tx, j.PipelineID)
}

// Cancel is an internal coordination primitive. It does not release active
// ownership or capacity: that waits for a valid stopped-execution ACK or expiry.
// A committed cancellation request prevents a racing successful completion.
func (s *Store) Cancel(ctx context.Context, jobID string) (*domain.Identity, error) {
	ctx, span := observability.Start(ctx, "job.cancel", attribute.String("job_id", jobID))
	defer span.End()
	span.SetAttributes(attribute.Bool("committed", false))
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = lockPipelineForJob(ctx, tx, jobID, false); err != nil {
		return nil, err
	}
	id, err := cancelJob(ctx, tx, jobID)
	if err != nil {
		return nil, err
	}
	var pipelineID *string
	if err = tx.QueryRow(ctx, `SELECT pipeline_id::text FROM jobs WHERE id=$1`, jobID).Scan(&pipelineID); err != nil {
		return nil, err
	}
	if err = refreshPipeline(ctx, tx, pipelineID); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Bool("committed", true))
	fields := []any{"job_id", jobID}
	if pipelineID != nil {
		fields = append(fields, "pipeline_id", *pipelineID)
	}
	fields = append(fields, observability.TraceFields(ctx)...)
	slog.InfoContext(ctx, "job cancellation committed", fields...)
	return id, nil
}
func cancelJob(ctx context.Context, tx pgx.Tx, jobID string) (*domain.Identity, error) {
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id=$1 FOR UPDATE`, jobID))
	if err != nil {
		return nil, err
	}
	if j.State == "SUCCEEDED" || j.State == "FAILED" || j.State == "CANCELLED" {
		return nil, domain.ErrTerminal
	}
	var identity *domain.Identity
	next := "CANCELLED"
	if j.State == "DISPATCHED" || j.State == "RUNNING" || j.State == "CANCELLING" {
		if j.CurrentAttemptID == nil {
			return nil, domain.ErrConflict
		}
		_, a, _, _, err := lockOwner(ctx, tx, domain.Identity{AttemptID: *j.CurrentAttemptID})
		if err != nil {
			return nil, err
		}
		if !domain.Active(a.State) {
			return nil, domain.ErrConflict
		}
		i := a.Identity
		identity = &i
		next = "CANCELLING"
		if j.State == "CANCELLING" {
			return identity, nil
		}
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET state=$2,cancel_requested_at=clock_timestamp(),retry_available_at=NULL,finished_at=CASE WHEN $2='CANCELLED' THEN clock_timestamp() ELSE NULL END WHERE id=$1`, jobID, next)
	if err != nil {
		return nil, err
	}
	return identity, nil
}
func (s *Store) Recover(ctx context.Context) error {
	// Liveness updates never mutate job ownership or release slots.
	if _, err := s.Pool.Exec(ctx, `UPDATE worker_sessions SET state='OFFLINE',connected=false WHERE state='ONLINE' AND last_seen_at<=clock_timestamp()-$1*interval '1 millisecond'`, s.Offline.Milliseconds()); err != nil {
		return err
	}
	for {
		recovered, err := s.recoverOne(ctx)
		if err != nil {
			return err
		}
		if !recovered {
			break
		}
	}
	return s.requeue(ctx)
}
func (s *Store) recoverOne(ctx context.Context) (bool, error) {
	rows, err := s.Pool.Query(ctx, `SELECT j.id::text FROM jobs j JOIN job_attempts a ON a.id=j.current_attempt_id WHERE a.state IN ('ASSIGNED','RUNNING') AND a.lease_expires_at<=clock_timestamp() ORDER BY a.lease_expires_at,j.id LIMIT 128`)
	if err != nil {
		return false, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return false, err
	}
	for _, id := range ids {
		done, err := s.recoverJob(ctx, id)
		if err != nil || done {
			return done, err
		}
	}
	return false, nil
}
func (s *Store) recoverJob(ctx context.Context, id string) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	locked, err := lockPipelineForJob(ctx, tx, id, true)
	if err != nil || !locked {
		return false, err
	}
	var jobID, attemptID string
	err = tx.QueryRow(ctx, `SELECT j.id::text,a.id::text FROM jobs j JOIN job_attempts a ON a.id=j.current_attempt_id WHERE j.id=$1 AND a.state IN ('ASSIGNED','RUNNING') AND a.lease_expires_at<=clock_timestamp() FOR UPDATE OF j SKIP LOCKED`, id).Scan(&jobID, &attemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	j, a, _, now, err := lockOwner(ctx, tx, domain.Identity{AttemptID: attemptID})
	if err != nil {
		return false, err
	}
	if !domain.Active(a.State) || now.Before(a.LeaseExpiresAt) {
		return false, nil
	}
	ctx, span := observability.Start(observability.Parent(ctx, a.TraceParent), "recovery.expire_attempt", observability.Attrs(j, a)...)
	defer span.End()
	span.SetAttributes(attribute.Bool("committed", false))
	r := domain.Result{State: "LOST", ExitCode: -1, FailureKind: "LEASE_EXPIRED", Detail: "execution lease expired"}
	// A delayed scanner must classify the first expired authority, not turn
	// an earlier infrastructure lease loss into workload failure just because
	// the execution deadline also elapsed during Control Plane downtime.
	if j.State != "CANCELLING" && a.ExecutionDeadline != nil && !a.ExecutionDeadline.After(a.LeaseExpiresAt) {
		r = domain.Result{State: "TIMED_OUT", ExitCode: -1, FailureKind: "JOB_TIMEOUT", Detail: "execution deadline exceeded before lease recovery"}
	}
	if err = s.finish(ctx, tx, j, a, r); err != nil {
		return false, err
	}
	err = tx.Commit(ctx)
	span.SetAttributes(attribute.Bool("committed", err == nil), attribute.String("result", r.State), attribute.String("failure_kind", r.FailureKind))
	if err == nil {
		fields := observability.Fields(ctx, j, a)
		fields = append(fields, "result", r.State, "failure_kind", r.FailureKind)
		slog.InfoContext(ctx, "expired attempt recovered", fields...)
	}
	return true, err
}
func timedOut(a domain.Attempt, now time.Time) bool {
	return a.ExecutionDeadline != nil && !now.Before(*a.ExecutionDeadline)
}

// Read-only ACK allowance. The deadline is write-once for each attempt. A
// delayed read/ACK can only reduce authority, never reset the timeout budget.
func (s *Store) ExecutionBudget(ctx context.Context, id domain.Identity) (int64, error) {
	var budget int64
	err := s.Pool.QueryRow(ctx, `SELECT GREATEST(0,floor(extract(epoch FROM (execution_deadline_at-clock_timestamp()))*1000))::bigint FROM job_attempts WHERE id=$1 AND fencing_token=$2 AND worker_session_id=$3`, id.AttemptID, id.FencingToken, id.SessionID).Scan(&budget)
	return budget, err
}

func (s *Store) requeue(ctx context.Context) error {
	rows, err := s.Pool.Query(ctx, `SELECT id::text FROM jobs WHERE state='RETRY_WAIT' AND retry_available_at<=clock_timestamp() ORDER BY retry_available_at,id`)
	if err != nil {
		return err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.requeueJob(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) requeueJob(ctx context.Context, id string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	locked, err := lockPipelineForJob(ctx, tx, id, true)
	if err != nil || !locked {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE jobs SET state='QUEUED',last_queued_at=clock_timestamp() WHERE id=$1 AND state='RETRY_WAIT' AND retry_available_at<=clock_timestamp()`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Notification only: deadlines reject renewal/completion independently. No
// capacity is released here while a lease is still valid.
func (s *Store) ExpiredExecutions(ctx context.Context) ([]domain.Identity, error) {
	rows, err := s.Pool.Query(ctx, `SELECT a.id::text,a.fencing_token,a.worker_session_id::text FROM job_attempts a JOIN jobs j ON j.current_attempt_id=a.id WHERE a.state IN ('ASSIGNED','RUNNING') AND j.state<>'CANCELLING' AND a.execution_deadline_at<=clock_timestamp() AND a.lease_expires_at>clock_timestamp()`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []domain.Identity{}
	for rows.Next() {
		var id domain.Identity
		if err = rows.Scan(&id.AttemptID, &id.FencingToken, &id.SessionID); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

type Session struct {
	StartedAt      time.Time  `json:"started_at"`
	DisconnectedAt *time.Time `json:"disconnected_at"`
	Current        bool       `json:"current"`
	ID             string     `json:"worker_session_id"`
	WorkerID       string     `json:"worker_id"`
	State          string     `json:"state"`
	Connected      bool       `json:"connected"`
	ActiveSlots    int        `json:"active_slots"`
	Capacity       int        `json:"capacity_slots"`
	LastSeen       time.Time  `json:"last_seen_at"`
}

func (s *Store) Sessions(ctx context.Context) ([]Session, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id::text,worker_id,state,connected,active_slots,capacity_slots,last_seen_at,started_at,disconnected_at,(row_number() OVER(PARTITION BY worker_id ORDER BY started_at DESC,id DESC)=1) FROM worker_sessions ORDER BY worker_id,started_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Session{}
	for rows.Next() {
		var v Session
		if err = rows.Scan(&v.ID, &v.WorkerID, &v.State, &v.Connected, &v.ActiveSlots, &v.Capacity, &v.LastSeen, &v.StartedAt, &v.DisconnectedAt, &v.Current); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type LogChunk struct {
	Sequence int64  `json:"sequence"`
	Stream   string `json:"stream"`
	Payload  []byte `json:"payload"`
}

func (s *Store) AppendLog(ctx context.Context, id domain.Identity, c LogChunk) error {
	if c.Sequence < 1 || len(c.Payload) < 1 || len(c.Payload) > 16384 || (c.Stream != "STDOUT" && c.Stream != "STDERR") {
		return errors.New("invalid log chunk")
	}
	tag, err := s.Pool.Exec(ctx, `INSERT INTO job_log_chunks(attempt_id,sequence,stream,payload) SELECT id,$4,$5,$6 FROM job_attempts WHERE id=$1 AND fencing_token=$2 AND worker_session_id=$3 ON CONFLICT DO NOTHING`, id.AttemptID, id.FencingToken, id.SessionID, c.Sequence, c.Stream, c.Payload)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var valid bool
		err = s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM job_attempts WHERE id=$1 AND fencing_token=$2 AND worker_session_id=$3)`, id.AttemptID, id.FencingToken, id.SessionID).Scan(&valid)
		if err != nil {
			return err
		}
		if !valid {
			return domain.ErrStale
		}
	}
	return nil
}
func (s *Store) Logs(ctx context.Context, attempt string, after int64) ([]LogChunk, error) {
	rows, err := s.Pool.Query(ctx, `SELECT sequence,stream,payload FROM job_log_chunks WHERE attempt_id=$1 AND sequence>$2 ORDER BY sequence LIMIT 1000`, attempt, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []LogChunk{}
	for rows.Next() {
		var c LogChunk
		if err = rows.Scan(&c.Sequence, &c.Stream, &c.Payload); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func (s *Store) ResetConnections(ctx context.Context) error {
	// Single Control Plane only: persisted connection flags from an earlier process are invalid.
	_, err := s.Pool.Exec(ctx, `UPDATE worker_sessions SET connected=false WHERE connected`)
	return err
}
func (s *Store) String() string { return fmt.Sprintf("lease=%s offline=%s", s.Lease, s.Offline) }
