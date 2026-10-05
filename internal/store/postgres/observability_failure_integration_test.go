//go:build integration

package postgres

import (
	"errors"
	"forgegrid/internal/domain"
	"forgegrid/internal/observability"
	"go.opentelemetry.io/otel"
	"testing"
)

func TestUnavailableTelemetryAndMalformedContextCannotChangeAuthority(t *testing.T) {
	old := otel.GetTracerProvider()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	shutdown := observability.Init("postgres-test")
	t.Cleanup(func() { shutdown(); otel.SetTracerProvider(old) })
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	id := pipeline(t, s, ctx, node("parent"), node("child", "parent"))
	if _, err := s.Pool.Exec(ctx, `UPDATE jobs SET trace_parent='invalid telemetry' WHERE pipeline_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	a := claimKey(t, s, ctx, id, "parent", b)
	if _, err := s.Pool.Exec(ctx, `UPDATE job_attempts SET trace_parent='invalid telemetry' WHERE id=$1`, a.Attempt.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Advance(ctx, a.Attempt.Identity, "start"); err != nil {
		t.Fatal(err)
	}
	stale := a.Attempt.Identity
	stale.FencingToken++
	if _, err := s.Complete(ctx, stale, success()); !errors.Is(err, domain.ErrStale) {
		t.Fatal("telemetry bypassed fence")
	}
	if _, err := s.Complete(ctx, a.Attempt.Identity, success()); err != nil {
		t.Fatal(err)
	}
	finishKey(t, s, ctx, id, "child", b, success())
	p, _ := pipelineView(t, s, ctx, id)
	if p.State != "SUCCEEDED" {
		t.Fatal("telemetry changed pipeline state")
	}
	assertSlots(t, s, ctx)
}

func TestQueueWaitExcludesDependencyAndRetryWait(t *testing.T) {
	s, ctx := fixture(t)
	b := register(t, s, ctx, "worker-b")
	id := pipeline(t, s, ctx, node("parent"), node("child", "parent"))
	_, jobs := pipelineView(t, s, ctx, id)
	if _, err := s.Pool.Exec(ctx, `UPDATE jobs SET created_at=clock_timestamp()-interval '2 hours' WHERE pipeline_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	a := claimKey(t, s, ctx, id, "parent", b)
	if _, err := s.Complete(ctx, a.Attempt.Identity, infraFailure()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE jobs SET last_queued_at=clock_timestamp()-interval '1 hour' WHERE id=$1`, jobs["parent"].ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	a = claimKey(t, s, ctx, id, "parent", b)
	if _, err := s.Complete(ctx, a.Attempt.Identity, success()); err != nil {
		t.Fatal(err)
	}
	child := claimKey(t, s, ctx, id, "child", b)
	for _, attempt := range []domain.Attempt{a.Attempt, child.Attempt} {
		var wait, total float64
		if err := s.Pool.QueryRow(ctx, `SELECT a.queue_wait_seconds,extract(epoch FROM(a.assigned_at-j.created_at)) FROM job_attempts a JOIN jobs j ON j.id=a.job_id WHERE a.id=$1`, attempt.AttemptID).Scan(&wait, &total); err != nil {
			t.Fatal(err)
		}
		if wait < 0 || wait >= 30 || total < 3600 {
			t.Fatal("queue wait included blocked/backoff time")
		}
	}
}
