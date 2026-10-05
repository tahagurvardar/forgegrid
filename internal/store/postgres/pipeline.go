package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"forgegrid/internal/domain"
	"forgegrid/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"log/slog"
)

// The pipeline row is the outer gate. All pipeline jobs are locked, in ID
// order, before any attempt/session lock. Standalone jobs keep the old order.
func lockPipeline(ctx context.Context, tx pgx.Tx, id string, skip bool) (bool, error) {
	query := `SELECT id FROM pipelines WHERE id=$1 FOR UPDATE`
	if skip {
		query += ` SKIP LOCKED`
	}
	var locked string
	err := tx.QueryRow(ctx, query, id).Scan(&locked)
	if skip && errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `SELECT id FROM jobs WHERE pipeline_id=$1 ORDER BY id FOR UPDATE`, id)
	return err == nil, err
}
func lockPipelineForJob(ctx context.Context, tx pgx.Tx, jobID string, skip bool) (bool, error) {
	var id *string
	if err := tx.QueryRow(ctx, `SELECT pipeline_id::text FROM jobs WHERE id=$1`, jobID).Scan(&id); err != nil {
		return false, err
	}
	if id == nil {
		return true, nil
	}
	return lockPipeline(ctx, tx, *id, skip)
}

func (s *Store) SubmitPipeline(ctx context.Context, spec domain.PipelineSpec) (string, error) {
	ctx, span := observability.Start(ctx, "pipeline.submit")
	defer span.End()
	span.SetAttributes(attribute.Bool("committed", false))
	if err := spec.Validate(); err != nil {
		return "", err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	id := uuid.NewString()
	span.SetAttributes(attribute.String("pipeline_id", id), attribute.Int("job_count", len(spec.Jobs)))
	if _, err = tx.Exec(ctx, `INSERT INTO pipelines(id,state) VALUES($1,'RUNNING')`, id); err != nil {
		return "", err
	}
	ids := map[string]string{}
	for _, j := range spec.Jobs {
		jobID := uuid.NewString()
		ids[j.Key] = jobID
		state := "QUEUED"
		if len(j.Dependencies) > 0 {
			state = "BLOCKED"
		}
		argv, err := json.Marshal(j.Command)
		if err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO jobs(id,pipeline_id,job_key,state,image,command,timeout_seconds,max_attempts,trace_parent,last_queued_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,CASE WHEN $4='QUEUED' THEN clock_timestamp() ELSE NULL END)`, jobID, id, j.Key, state, j.Image, argv, j.TimeoutSeconds, j.MaxAttempts, observability.Carrier(ctx)); err != nil {
			return "", err
		}
	}
	for _, j := range spec.Jobs {
		for _, p := range j.Dependencies {
			if _, err = tx.Exec(ctx, `INSERT INTO job_dependencies(pipeline_id,job_id,depends_on_job_id) VALUES($1,$2,$3)`, id, ids[j.Key], ids[p]); err != nil {
				return "", err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	span.SetAttributes(attribute.Bool("committed", true))
	fields := []any{"pipeline_id", id, "job_count", len(spec.Jobs)}
	fields = append(fields, observability.TraceFields(ctx)...)
	slog.InfoContext(ctx, "pipeline submitted", fields...)
	return id, nil
}

// Called only while the pipeline gate and its job locks are held. Terminal
// parent changes, dependency propagation, and pipeline aggregation share a tx.
func refreshPipeline(ctx context.Context, tx pgx.Tx, id *string) error {
	if id == nil {
		return nil
	}
	_, skipSpan := observability.Start(ctx, "dag.propagate_skip", attribute.String("pipeline_id", *id))
	defer skipSpan.End()
	skipped := int64(0)
	for {
		tag, err := tx.Exec(ctx, `UPDATE jobs child SET state='SKIPPED',finished_at=clock_timestamp()
 WHERE child.pipeline_id=$1 AND child.state='BLOCKED' AND EXISTS
 (SELECT 1 FROM job_dependencies d JOIN jobs parent ON parent.id=d.depends_on_job_id
 WHERE d.job_id=child.id AND parent.state IN ('FAILED','CANCELLED','SKIPPED'))`, *id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			break
		}
		skipped += tag.RowsAffected()
	}
	skipSpan.SetAttributes(attribute.Int64("affected_jobs", skipped))
	releaseCtx, releaseSpan := observability.Start(ctx, "dag.release_dependencies", attribute.String("pipeline_id", *id))
	tag, err := tx.Exec(ctx, `UPDATE jobs child SET state='QUEUED',trace_parent=$2,last_queued_at=clock_timestamp() WHERE child.pipeline_id=$1 AND child.state='BLOCKED'
 AND NOT EXISTS(SELECT 1 FROM job_dependencies d JOIN jobs parent ON parent.id=d.depends_on_job_id WHERE d.job_id=child.id AND parent.state<>'SUCCEEDED')`, *id, observability.Carrier(releaseCtx))
	releaseSpan.SetAttributes(attribute.Int64("affected_jobs", tag.RowsAffected()))
	observability.End(releaseSpan, err)
	if err != nil {
		return err
	}
	_, finalizeSpan := observability.Start(ctx, "pipeline.finalize", attribute.String("pipeline_id", *id))
	defer finalizeSpan.End()
	var pipelineState string
	err = tx.QueryRow(ctx, `UPDATE pipelines p SET state=CASE
 WHEN EXISTS(SELECT 1 FROM jobs WHERE pipeline_id=p.id AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED','SKIPPED'))
 THEN CASE WHEN p.cancel_requested_at IS NOT NULL THEN 'CANCELLING' ELSE 'RUNNING' END
 WHEN p.cancel_requested_at IS NOT NULL THEN 'CANCELLED'
 WHEN EXISTS(SELECT 1 FROM jobs WHERE pipeline_id=p.id AND state='FAILED') THEN 'FAILED'
 WHEN EXISTS(SELECT 1 FROM jobs WHERE pipeline_id=p.id AND state='CANCELLED') THEN 'CANCELLED'
 WHEN EXISTS(SELECT 1 FROM jobs WHERE pipeline_id=p.id AND state='SKIPPED') THEN 'FAILED'
 ELSE 'SUCCEEDED' END,
 finished_at=CASE WHEN NOT EXISTS(SELECT 1 FROM jobs WHERE pipeline_id=p.id AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED','SKIPPED')) THEN COALESCE(p.finished_at,clock_timestamp()) ELSE NULL END WHERE p.id=$1 RETURNING state`, *id).Scan(&pipelineState)
	finalizeSpan.SetAttributes(attribute.String("result", pipelineState), attribute.Bool("transaction_pending", true))
	return err
}

func (s *Store) GetPipeline(ctx context.Context, id string) (domain.Pipeline, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return domain.Pipeline{}, err
	}
	defer tx.Rollback(ctx)
	p := domain.Pipeline{Jobs: []domain.Job{}}
	if err = tx.QueryRow(ctx, `SELECT id::text,state,created_at,finished_at,(SELECT min(a.started_at) FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.pipeline_id=pipelines.id) FROM pipelines WHERE id=$1`, id).Scan(&p.ID, &p.State, &p.CreatedAt, &p.FinishedAt, &p.StartedAt); err != nil {
		return p, err
	}
	rows, err := tx.Query(ctx, `SELECT id::text FROM jobs WHERE pipeline_id=$1 ORDER BY job_key`, id)
	if err != nil {
		return p, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return p, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return p, err
	}
	for _, jobID := range ids {
		j, err := getJob(ctx, tx, jobID)
		if err != nil {
			return p, err
		}
		p.Jobs = append(p.Jobs, j)
	}
	return p, tx.Commit(ctx)
}

func (s *Store) CancelPipeline(ctx context.Context, id string) ([]domain.Identity, error) {
	ctx, span := observability.Start(ctx, "pipeline.cancel", attribute.String("pipeline_id", id))
	defer span.End()
	span.SetAttributes(attribute.Bool("committed", false))
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = lockPipeline(ctx, tx, id, false); err != nil {
		return nil, err
	}
	var state string
	if err = tx.QueryRow(ctx, `SELECT state FROM pipelines WHERE id=$1`, id).Scan(&state); err != nil {
		return nil, err
	}
	if state == "SUCCEEDED" || state == "FAILED" || state == "CANCELLED" {
		return nil, domain.ErrTerminal
	}
	// Reserve all active attempt/session locks in global class/ID order before
	// cancelling several jobs. No job lock is newly acquired after a session.
	if _, err = tx.Exec(ctx, `SELECT a.id FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.pipeline_id=$1 AND a.state IN ('ASSIGNED','RUNNING') ORDER BY a.id FOR UPDATE OF a`, id); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `SELECT w.id FROM worker_sessions w WHERE EXISTS(SELECT 1 FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE j.pipeline_id=$1 AND a.worker_session_id=w.id AND a.state IN ('ASSIGNED','RUNNING')) ORDER BY w.id FOR UPDATE`, id); err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `UPDATE pipelines SET state='CANCELLING',cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp()) WHERE id=$1`, id); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id::text FROM jobs WHERE pipeline_id=$1 AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED','SKIPPED') ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var jobID string
		if err = rows.Scan(&jobID); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, jobID)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	active := []domain.Identity{}
	for _, jobID := range ids {
		identity, err := cancelJob(ctx, tx, jobID)
		if err != nil {
			return nil, err
		}
		if identity != nil {
			active = append(active, *identity)
		}
	}
	if err = refreshPipeline(ctx, tx, &id); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Bool("committed", true))
	fields := []any{"pipeline_id", id, "active_cancellations", len(active)}
	fields = append(fields, observability.TraceFields(ctx)...)
	slog.InfoContext(ctx, "pipeline cancellation committed", fields...)
	return active, nil
}
