package postgres

import (
	"context"
	"forgegrid/internal/domain"
	"github.com/jackc/pgx/v5"
	"time"
)

// Inspection transactions have no ownership locks and never write coordination state.
type Page[T any] struct {
	Items      []T  `json:"items"`
	NextOffset *int `json:"next_offset"`
}
type PipelineSummary struct {
	ID            string     `json:"id"`
	State         string     `json:"state"`
	CreatedAt     time.Time  `json:"created_at"`
	StartedAt     *time.Time `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at"`
	JobCount      int        `json:"job_count"`
	TerminalCount int        `json:"terminal_count"`
	AttemptCount  int        `json:"attempt_count"`
}

func (s *Store) Pipelines(ctx context.Context, offset int) (Page[PipelineSummary], error) {
	out := Page[PipelineSummary]{Items: []PipelineSummary{}}
	rows, err := s.Pool.Query(ctx, `SELECT p.id::text,p.state,p.created_at,p.finished_at,(SELECT min(a.started_at) FROM jobs j JOIN job_attempts a ON a.job_id=j.id WHERE j.pipeline_id=p.id),count(j.id),count(j.id) FILTER(WHERE j.state IN('SUCCEEDED','FAILED','CANCELLED','SKIPPED')),coalesce(sum(j.attempt_count),0) FROM pipelines p LEFT JOIN jobs j ON j.pipeline_id=p.id GROUP BY p.id ORDER BY p.created_at DESC,p.id DESC LIMIT 51 OFFSET $1`, offset)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var p PipelineSummary
		if err = rows.Scan(&p.ID, &p.State, &p.CreatedAt, &p.FinishedAt, &p.StartedAt, &p.JobCount, &p.TerminalCount, &p.AttemptCount); err != nil {
			return out, err
		}
		out.Items = append(out.Items, p)
	}
	if len(out.Items) > 50 {
		n := offset + 50
		out.NextOffset = &n
		out.Items = out.Items[:50]
	}
	return out, rows.Err()
}
func (s *Store) Jobs(ctx context.Context, offset int) (Page[domain.Job], error) {
	out := Page[domain.Job]{Items: []domain.Job{}}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id::text FROM jobs ORDER BY created_at DESC,id DESC LIMIT 51 OFFSET $1`, offset)
	if err != nil {
		return out, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return out, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(ids) > 50 {
		n := offset + 50
		out.NextOffset = &n
		ids = ids[:50]
	}
	for _, id := range ids {
		j, e := getJob(ctx, tx, id)
		if e != nil {
			return out, e
		}
		out.Items = append(out.Items, j)
	}
	return out, tx.Commit(ctx)
}
func (s *Store) AttemptJob(ctx context.Context, id string) (domain.Job, error) {
	var jobID string
	err := s.Pool.QueryRow(ctx, `SELECT job_id::text FROM job_attempts WHERE id=$1`, id).Scan(&jobID)
	if err != nil {
		return domain.Job{}, err
	}
	return s.GetJob(ctx, jobID)
}

type Overview struct {
	ServerTime time.Time        `json:"server_time"`
	Jobs       map[string]int   `json:"jobs"`
	Recent     []domain.Attempt `json:"recent_activity"`
	Active     []domain.Attempt `json:"active_attempts"`
}

func (s *Store) Overview(ctx context.Context) (Overview, error) {
	out := Overview{Jobs: map[string]int{}, Recent: []domain.Attempt{}, Active: []domain.Attempt{}}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&out.ServerTime); err != nil {
		return out, err
	}
	rows, err := tx.Query(ctx, `SELECT state,count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var state string
		var count int
		if err = rows.Scan(&state, &count); err != nil {
			rows.Close()
			return out, err
		}
		out.Jobs[state] = count
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return out, err
	}
	for _, active := range []bool{false, true} {
		predicate := `(a.state='LOST' OR a.attempt_number>1) ORDER BY coalesce(a.finished_at,a.assigned_at) DESC,a.id DESC LIMIT 50`
		if active {
			predicate = `a.state IN('ASSIGNED','RUNNING') ORDER BY a.assigned_at,a.id`
		}
		rows, err = tx.Query(ctx, `SELECT `+attemptColumns+` FROM job_attempts a JOIN worker_sessions w ON w.id=a.worker_session_id WHERE `+predicate)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			a, e := scanAttempt(rows)
			if e != nil {
				rows.Close()
				return out, e
			}
			if active {
				out.Active = append(out.Active, a)
			} else {
				out.Recent = append(out.Recent, a)
			}
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return out, err
		}
	}
	return out, tx.Commit(ctx)
}
