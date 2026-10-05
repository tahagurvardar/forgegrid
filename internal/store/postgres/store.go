package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"forgegrid/db/migrations"
	"forgegrid/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	Pool                      *pgxpool.Pool
	Lease, Offline, RetryBase time.Duration
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
	return &Store{p, lease, offline, retry}, nil
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
	return tx.Commit(ctx)
}
func (s *Store) Submit(ctx context.Context, spec domain.Spec) (string, error) {
	if err := spec.Validate(); err != nil {
		return "", err
	}
	id := uuid.NewString()
	argv, err := json.Marshal(spec.Command)
	if err != nil {
		return "", err
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO jobs(id,state,image,command,timeout_seconds,max_attempts) VALUES($1,'QUEUED',$2,$3,$4,$5)`, id, spec.Image, argv, spec.TimeoutSeconds, spec.MaxAttempts)
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

const jobColumns = `id::text,state,image,command,timeout_seconds,max_attempts,current_attempt_id::text,fencing_token,attempt_count`

func scanJob(row pgx.Row) (domain.Job, error) {
	var j domain.Job
	var argv []byte
	err := row.Scan(&j.ID, &j.State, &j.Image, &argv, &j.TimeoutSeconds, &j.MaxAttempts, &j.CurrentAttemptID, &j.FencingToken, &j.AttemptCount)
	if err == nil {
		err = json.Unmarshal(argv, &j.Command)
	}
	return j, err
}

const attemptColumns = `a.id::text,a.job_id::text,a.attempt_number,a.fencing_token,a.worker_session_id::text,a.state,a.lease_expires_at,a.exit_code,a.failure_kind,a.failure_detail,w.worker_id`

func scanAttempt(row pgx.Row) (domain.Attempt, error) {
	var a domain.Attempt
	err := row.Scan(&a.AttemptID, &a.JobID, &a.Number, &a.FencingToken, &a.SessionID, &a.State, &a.LeaseExpiresAt, &a.ExitCode, &a.FailureKind, &a.FailureDetail, &a.WorkerID)
	return a, err
}
func (s *Store) GetJob(ctx context.Context, id string) (domain.Job, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx)
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
	return j, tx.Commit(ctx)
}
func (s *Store) Schedule(ctx context.Context, connected []string) (*domain.Assignment, error) {
	if len(connected) == 0 {
		return nil, nil
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE state='QUEUED' AND attempt_count<max_attempts ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var session, worker string
	err = tx.QueryRow(ctx, `SELECT id::text,worker_id FROM worker_sessions WHERE state='ONLINE' AND connected AND id::text=ANY($1::text[]) AND last_seen_at>clock_timestamp()-$2*interval '1 millisecond' AND active_slots<capacity_slots ORDER BY active_slots,last_assignment_at NULLS FIRST,worker_id,id FOR UPDATE SKIP LOCKED LIMIT 1`, connected, s.Offline.Milliseconds()).Scan(&session, &worker)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	a := domain.Attempt{Identity: domain.Identity{AttemptID: uuid.NewString(), FencingToken: j.FencingToken + 1, SessionID: session}, JobID: j.ID, Number: j.AttemptCount + 1, WorkerID: worker, State: "ASSIGNED"}
	err = tx.QueryRow(ctx, `INSERT INTO job_attempts(id,job_id,attempt_number,fencing_token,worker_session_id,state,lease_expires_at) VALUES($1,$2,$3,$4,$5,'ASSIGNED',clock_timestamp()+$6*interval '1 millisecond') RETURNING lease_expires_at`, a.AttemptID, j.ID, a.Number, a.FencingToken, session, s.Lease.Milliseconds()).Scan(&a.LeaseExpiresAt)
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
	if !domain.Owns(j, a, id, now) || session != "ONLINE" {
		return 0, domain.ErrStale
	}
	if j.State == "CANCELLING" {
		return 0, domain.ErrCancelled
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
		if _, err = tx.Exec(ctx, `UPDATE job_attempts SET state='RUNNING',started_at=COALESCE(started_at,clock_timestamp()) WHERE id=$1`, id.AttemptID); err == nil {
			_, err = tx.Exec(ctx, `UPDATE jobs SET state='RUNNING' WHERE id=$1`, j.ID)
		}
	case "renew":
		_, err = tx.Exec(ctx, `UPDATE job_attempts SET lease_expires_at=clock_timestamp()+$2*interval '1 millisecond' WHERE id=$1`, id.AttemptID, s.Lease.Milliseconds())
	default:
		return 0, errors.New("invalid action")
	}
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return s.Lease, nil
}
func (s *Store) Complete(ctx context.Context, id domain.Identity, r domain.Result) (bool, error) {
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
	// An exact duplicate of the current winner is acknowledged without mutating anything.
	if j.CurrentAttemptID == nil || *j.CurrentAttemptID != id.AttemptID || j.FencingToken != id.FencingToken || a.FencingToken != id.FencingToken || a.SessionID != id.SessionID {
		return false, domain.ErrStale
	}
	if !domain.Active(a.State) {
		if a.State == "LOST" {
			return false, domain.ErrStale
		}
		if a.State == r.State && a.ExitCode != nil && *a.ExitCode == r.ExitCode && a.FailureKind == r.FailureKind && a.FailureDetail == r.Detail {
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
	if err = s.finish(ctx, tx, j, a, r); err != nil {
		return false, err
	}
	return false, tx.Commit(ctx)
}
func (s *Store) finish(ctx context.Context, tx pgx.Tx, j domain.Job, a domain.Attempt, r domain.Result) error {
	if _, err := tx.Exec(ctx, `UPDATE job_attempts SET state=$2,exit_code=$3,failure_kind=$4,failure_detail=$5,finished_at=clock_timestamp() WHERE id=$1`, a.AttemptID, r.State, r.ExitCode, r.FailureKind, domain.BoundedDetail(r.Detail)); err != nil {
		return err
	}
	next := domain.NextJobState(j.AttemptCount, j.MaxAttempts, r)
	if j.State == "CANCELLING" {
		next = "CANCELLED"
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET state=$2,retry_available_at=CASE WHEN $2='RETRY_WAIT' THEN clock_timestamp()+$3*interval '1 millisecond' ELSE NULL END,finished_at=CASE WHEN $2 IN ('SUCCEEDED','FAILED','CANCELLED') THEN clock_timestamp() ELSE NULL END WHERE id=$1`, j.ID, next, domain.RetryDelay(a.Number, s.RetryBase).Milliseconds()); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE worker_sessions SET active_slots=active_slots-1 WHERE id=$1`, a.SessionID)
	return err
}

// Cancel is an internal coordination primitive. It does not release active
// ownership or capacity: that waits for a valid stopped-execution ACK or expiry.
// A committed cancellation request prevents a racing successful completion.
func (s *Store) Cancel(ctx context.Context, jobID string) (*domain.Identity, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
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
			return identity, tx.Commit(ctx)
		}
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET state=$2,cancel_requested_at=clock_timestamp(),retry_available_at=NULL,finished_at=CASE WHEN $2='CANCELLED' THEN clock_timestamp() ELSE NULL END WHERE id=$1`, jobID, next)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
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
	_, err := s.Pool.Exec(ctx, `UPDATE jobs SET state='QUEUED' WHERE state='RETRY_WAIT' AND retry_available_at<=clock_timestamp()`)
	return err
}
func (s *Store) recoverOne(ctx context.Context) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var jobID, attemptID string
	err = tx.QueryRow(ctx, `SELECT j.id::text,a.id::text FROM jobs j JOIN job_attempts a ON a.id=j.current_attempt_id WHERE a.state IN ('ASSIGNED','RUNNING') AND a.lease_expires_at<=clock_timestamp() ORDER BY a.lease_expires_at,j.id FOR UPDATE OF j SKIP LOCKED LIMIT 1`).Scan(&jobID, &attemptID)
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
	if err = s.finish(ctx, tx, j, a, domain.Result{State: "LOST", ExitCode: -1, FailureKind: "LEASE_EXPIRED", Detail: "execution lease expired"}); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

type Session struct {
	ID          string    `json:"worker_session_id"`
	WorkerID    string    `json:"worker_id"`
	State       string    `json:"state"`
	Connected   bool      `json:"connected"`
	ActiveSlots int       `json:"active_slots"`
	Capacity    int       `json:"capacity_slots"`
	LastSeen    time.Time `json:"last_seen_at"`
}

func (s *Store) Sessions(ctx context.Context) ([]Session, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id::text,worker_id,state,connected,active_slots,capacity_slots,last_seen_at FROM worker_sessions ORDER BY worker_id,started_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Session{}
	for rows.Next() {
		var v Session
		if err = rows.Scan(&v.ID, &v.WorkerID, &v.State, &v.Connected, &v.ActiveSlots, &v.Capacity, &v.LastSeen); err != nil {
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
