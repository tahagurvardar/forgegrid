package postgres

import (
	"context"
	"fmt"
	"forgegrid/internal/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"strings"
	"time"
)

// A separate bounded read-only pool means telemetry cannot exhaust the
// coordination pool. No sampling error is returned to scheduling/completion.
func (s *Store) Observe(ctx context.Context, m *observability.Metrics) {
	cfg := s.Pool.Config()
	cfg.MaxConns = 1
	cfg.MinConns = 0
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "1000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		m.SnapshotOK.Set(0)
		return
	}
	defer pool.Close()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		sample, cancel := context.WithTimeout(ctx, time.Second)
		err = snapshotMetrics(sample, pool, m)
		cancel()
		if err != nil {
			m.SnapshotOK.Set(0)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func snapshotMetrics(ctx context.Context, pool *pgxpool.Pool, m *observability.Metrics) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	metrics := []prometheus.Metric{}
	add := func(name, help string, kind prometheus.ValueType, value float64, labels []string, values ...string) {
		metrics = append(metrics, prometheus.MustNewConstMetric(prometheus.NewDesc(name, help, labels, nil), kind, value, values...))
	}
	counts := []struct {
		name, query string
		kind        prometheus.ValueType
	}{
		{"workers_online", `SELECT count(*) FROM (SELECT DISTINCT ON(worker_id) state FROM worker_sessions ORDER BY worker_id,started_at DESC,id DESC) s WHERE state='ONLINE'`, prometheus.GaugeValue},
		{"workers_offline", `SELECT count(*) FROM (SELECT DISTINCT ON(worker_id) state FROM worker_sessions ORDER BY worker_id,started_at DESC,id DESC) s WHERE state='OFFLINE'`, prometheus.GaugeValue},
		{"jobs_queued", `SELECT count(*) FROM jobs WHERE state='QUEUED'`, prometheus.GaugeValue},
		{"jobs_blocked", `SELECT count(*) FROM jobs WHERE state='BLOCKED'`, prometheus.GaugeValue},
		{"jobs_running", `SELECT count(*) FROM jobs WHERE state='RUNNING'`, prometheus.GaugeValue},
		{"attempts_started_total", `SELECT count(*) FROM job_attempts WHERE started_at IS NOT NULL`, prometheus.CounterValue},
		{"lease_expirations_total", `SELECT count(*) FROM job_attempts WHERE failure_kind='LEASE_EXPIRED'`, prometheus.CounterValue},
		{"pipeline_submissions_total", `SELECT count(*) FROM pipelines`, prometheus.CounterValue},
		{"log_bytes_total", `SELECT coalesce(sum(octet_length(payload)),0) FROM job_log_chunks`, prometheus.CounterValue},
	}
	// Fixed internal SQL only. One statement and one snapshot for simple counts.
	queries := []string{}
	for _, c := range counts {
		queries = append(queries, "("+c.query+")")
	}
	dest := make([]any, len(counts))
	values := make([]float64, len(counts))
	for i := range values {
		dest[i] = &values[i]
	}
	if err = tx.QueryRow(ctx, "SELECT "+strings.Join(queries, ",")).Scan(dest...); err != nil {
		return err
	}
	for i, c := range counts {
		add("forgegrid_"+c.name, "PostgreSQL committed snapshot: "+c.name, c.kind, values[i], nil)
	}
	for _, result := range []string{"SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED", "LOST"} {
		var count float64
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM job_attempts WHERE state=$1`, result).Scan(&count); err != nil {
			return err
		}
		add("forgegrid_attempts_completed_total", "Authoritative terminal attempts, including recovery.", prometheus.CounterValue, count, []string{"result"}, result)
	}
	for _, result := range []string{"SUCCEEDED", "FAILED", "CANCELLED"} {
		var count float64
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM pipelines WHERE state=$1`, result).Scan(&count); err != nil {
			return err
		}
		add("forgegrid_pipeline_completions_total", "Authoritative terminal pipelines.", prometheus.CounterValue, count, []string{"result"}, result)
	}
	for _, reason := range []string{"LEASE_EXPIRED", "EXECUTOR_INFRA_ERROR", "ASSIGNMENT_REJECTED", "OTHER"} {
		var count float64
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM job_attempts a JOIN job_attempts previous ON previous.job_id=a.job_id AND previous.attempt_number=a.attempt_number-1 WHERE CASE WHEN previous.failure_kind IN ('LEASE_EXPIRED','EXECUTOR_INFRA_ERROR','ASSIGNMENT_REJECTED') THEN previous.failure_kind ELSE 'OTHER' END=$1`, reason).Scan(&count); err != nil {
			return err
		}
		add("forgegrid_attempt_retries_total", "Committed retry attempts by preceding failure category.", prometheus.CounterValue, count, []string{"reason"}, reason)
	}
	for _, h := range []struct{ name, query string }{
		{"queue_wait_duration_seconds", `SELECT queue_wait_seconds seconds FROM job_attempts WHERE queue_wait_seconds IS NOT NULL`},
		{"attempt_duration_seconds", `SELECT extract(epoch FROM (finished_at-coalesce(started_at,assigned_at)))::float8 seconds FROM job_attempts WHERE finished_at IS NOT NULL`},
		{"submission_to_assignment_duration_seconds", `SELECT extract(epoch FROM (a.assigned_at-j.created_at))::float8 seconds FROM job_attempts a JOIN jobs j ON j.id=a.job_id`},
	} {
		q := `SELECT count(*),coalesce(sum(seconds),0)`
		buckets := observability.DurationBuckets
		for _, b := range buckets {
			q += fmt.Sprintf(",count(*) FILTER(WHERE seconds<=%g)", b)
		}
		q += " FROM (" + h.query + ") duration"
		var count uint64
		var sum float64
		bucketCounts := make([]uint64, len(buckets))
		dest := []any{&count, &sum}
		for i := range bucketCounts {
			dest = append(dest, &bucketCounts[i])
		}
		if err = tx.QueryRow(ctx, q).Scan(dest...); err != nil {
			return err
		}
		totals := map[float64]uint64{}
		for i, b := range buckets {
			totals[b] = bucketCounts[i]
		}
		metrics = append(metrics, prometheus.MustNewConstHistogram(prometheus.NewDesc("forgegrid_"+h.name, "Committed history duration; see metric catalog.", nil, nil), count, sum, totals))
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	m.SetSnapshot(metrics)
	return nil
}
