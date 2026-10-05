//go:build integration

package postgres

import (
	"context"
	"errors"
	"forgegrid/internal/domain"
	"forgegrid/internal/observability"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"net/http/httptest"
	"testing"
)

func TestCommittedMetricsAndRetryTraceCorrelation(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(old); _ = provider.Shutdown(context.Background()) })
	s, ctx := fixture(t)
	m := observability.NewMetrics()
	s.Metrics = m
	b := register(t, s, ctx, "worker-b")
	c := register(t, s, ctx, "worker-c")
	submit, root := observability.Start(ctx, "http.submit")
	id := pipeline(t, s, submit, node("parent"), node("child", "parent"))
	root.End()
	// The original request has already ended before asynchronous claim.
	first := claimKey(t, s, ctx, id, "parent", b)
	if _, err := s.Advance(ctx, first.Attempt.Identity, "start"); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLog(ctx, first.Attempt.Identity, LogChunk{Sequence: 1, Stream: "STDOUT", Payload: []byte("abc")}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLog(ctx, first.Attempt.Identity, LogChunk{Sequence: 1, Stream: "STDOUT", Payload: []byte("abc")}); err != nil {
		t.Fatal(err)
	}
	expire(t, s, ctx, first.Attempt.AttemptID)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	second := claimKey(t, s, ctx, id, "parent", c)
	if _, err := s.Advance(ctx, second.Attempt.Identity, "start"); err != nil {
		t.Fatal(err)
	}
	firstSC := trace.SpanContextFromContext(observability.Parent(ctx, first.Attempt.TraceParent))
	secondSC := trace.SpanContextFromContext(observability.Parent(ctx, second.Attempt.TraceParent))
	if !firstSC.IsValid() || firstSC.TraceID() != root.SpanContext().TraceID() || firstSC.TraceID() != secondSC.TraceID() || firstSC.SpanID() == secondSC.SpanID() {
		t.Fatal("retry context disconnected or reused")
	}
	if _, err := s.Complete(ctx, first.Attempt.Identity, success()); !errors.Is(err, domain.ErrStale) {
		t.Fatal("tracing weakened stale rejection")
	}
	if _, err := s.Complete(ctx, second.Attempt.Identity, success()); err != nil {
		t.Fatal(err)
	}
	third := claimKey(t, s, ctx, id, "child", c)
	if trace.SpanContextFromContext(observability.Parent(ctx, third.Attempt.TraceParent)).TraceID() != firstSC.TraceID() {
		t.Fatal("dependency release lost trace")
	}
	if _, err := s.Complete(ctx, third.Attempt.Identity, success()); err != nil {
		t.Fatal(err)
	}
	if dup, err := s.Complete(ctx, third.Attempt.Identity, success()); err != nil || !dup {
		t.Fatal("duplicate semantics changed")
	}
	if err := snapshotMetrics(ctx, s.Pool, m); err != nil {
		t.Fatal(err)
	}
	expected := map[string]float64{"forgegrid_attempts_started_total": 2, "forgegrid_lease_expirations_total": 1, "forgegrid_pipeline_submissions_total": 1, "forgegrid_log_bytes_total": 3, "forgegrid_stale_results_rejected_total": 1}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		for _, metric := range f.Metric {
			name := f.GetName()
			value := metric.GetCounter().GetValue()
			if want, ok := expected[name]; ok {
				if value != want {
					t.Fatalf("%s=%v want %v", name, value, want)
				}
				delete(expected, name)
			}
			for _, label := range metric.Label {
				if label.GetName() != "reason" && label.GetName() != "result" {
					t.Fatalf("unbounded label %s", label.GetName())
				}
				allowed := map[string]bool{"SUCCEEDED": true, "FAILED": true, "TIMED_OUT": true, "LOST": true, "CANCELLED": true, "LEASE_EXPIRED": true, "EXECUTOR_INFRA_ERROR": true, "ASSIGNMENT_REJECTED": true, "OTHER": true}
				if !allowed[label.GetValue()] {
					t.Fatalf("unbounded label value %s", label.GetValue())
				}
			}
			if name == "forgegrid_attempt_retries_total" && metric.Label[0].GetValue() == "LEASE_EXPIRED" && value != 1 {
				t.Fatal("retry count")
			}
			if name == "forgegrid_pipeline_completions_total" && metric.Label[0].GetValue() == "SUCCEEDED" && value != 1 {
				t.Fatal("pipeline count")
			}
			if name == "forgegrid_attempts_completed_total" {
				switch metric.Label[0].GetValue() {
				case "SUCCEEDED":
					if value != 2 {
						t.Fatal("duplicate counted")
					}
				case "LOST":
					if value != 1 {
						t.Fatal("lease completion missing")
					}
				}
			}
		}
	}
	if len(expected) != 0 {
		t.Fatalf("missing metrics %v", expected)
	}
	names := map[string]bool{}
	for _, span := range recorder.Ended() {
		names[span.Name()] = true
	}
	for _, name := range []string{"pipeline.submit", "scheduler.claim_job", "scheduler.select_worker", "scheduler.create_attempt", "controlplane.complete_attempt", "recovery.expire_attempt", "recovery.retry_job", "dag.release_dependencies", "pipeline.finalize"} {
		if !names[name] {
			t.Fatalf("missing span %s", name)
		}
	}
	assertSlots(t, s, ctx)
	// Scrape remains independent even when the coordination pool is closed.
	s.Pool.Close()
	w := httptest.NewRecorder()
	m.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 {
		t.Fatalf("scrape needs database: %d", w.Code)
	}
}

func TestRolledBackTransitionsDoNotIncrementDurableMetrics(t *testing.T) {
	s, ctx := fixture(t)
	m := observability.NewMetrics()
	b := register(t, s, ctx, "worker-b")
	id := pipeline(t, s, ctx, node("parent"), node("child", "parent"))
	a := claimKey(t, s, ctx, id, "parent", b)
	injectFailure(t, s, ctx, "job_attempts", "UPDATE", true)
	if _, err := s.Complete(ctx, a.Attempt.Identity, success()); err == nil {
		t.Fatal("commit fault missing")
	}
	if err := snapshotMetrics(ctx, s.Pool, m); err != nil {
		t.Fatal(err)
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "forgegrid_attempts_completed_total" || f.GetName() == "forgegrid_pipeline_completions_total" {
			for _, metric := range f.Metric {
				if metric.GetCounter().GetValue() != 0 {
					t.Fatal("rollback counted as committed")
				}
			}
		}
	}
	assertSlots(t, s, ctx)
}
