// Package observability observes coordination; it never grants execution authority.
package observability

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"forgegrid/internal/domain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var wire = propagation.TraceContext{} // No baggage, credentials, or arbitrary metadata.

// Init never contacts a backend synchronously. Empty endpoint disables export.
func Init(service string) func() {
	var lastError atomic.Int64
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {
		now := time.Now().Unix()
		old := lastError.Load()
		if now-old >= 30 && lastError.CompareAndSwap(old, now) {
			slog.Warn("telemetry export failed; execution unaffected")
		}
	}))
	opts := []sdktrace.TracerProviderOption{sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service)))}
	if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" && os.Getenv("OTEL_TRACES_EXPORTER") != "none" {
		// Let the SDK append /v1/traces to the generic environment base URL;
		// WithEndpointURL expects a complete signal URL and would override that path.
		exporter, err := otlptracehttp.New(context.Background(), otlptracehttp.WithTimeout(time.Second), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
		if err != nil {
			slog.Warn("telemetry exporter configuration invalid; export disabled")
		} else {
			opts = append(opts, sdktrace.WithBatcher(exporter, sdktrace.WithMaxQueueSize(1024), sdktrace.WithMaxExportBatchSize(256), sdktrace.WithBatchTimeout(200*time.Millisecond), sdktrace.WithExportTimeout(time.Second)))
		}
	}
	provider := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(provider)
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	}
}

func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer("forgegrid").Start(ctx, name, trace.WithAttributes(attrs...))
}
func Parent(ctx context.Context, parent string) context.Context {
	if len(parent) > 128 {
		return ctx
	}
	return wire.Extract(ctx, propagation.MapCarrier{"traceparent": parent})
}
func Carrier(ctx context.Context) string {
	m := propagation.MapCarrier{}
	wire.Inject(ctx, m)
	return m["traceparent"]
}
func Continue(ctx context.Context, fallback string) context.Context {
	if trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}
	return Parent(ctx, fallback)
}
func End(span trace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, "operation failed")
	}
	span.End()
}
func Attrs(j domain.Job, a domain.Attempt) []attribute.KeyValue {
	out := []attribute.KeyValue{attribute.String("job_id", j.ID), attribute.String("attempt_id", a.AttemptID), attribute.Int("attempt_number", a.Number), attribute.String("worker_id", a.WorkerID), attribute.String("worker_session_id", a.SessionID), attribute.Int64("fencing_token", a.FencingToken)}
	if j.PipelineID != nil {
		out = append(out, attribute.String("pipeline_id", *j.PipelineID))
	}
	return out
}
func Fields(ctx context.Context, j domain.Job, a domain.Attempt) []any {
	out := []any{"job_id", j.ID, "attempt_id", a.AttemptID, "attempt_number", a.Number, "worker_id", a.WorkerID, "worker_session_id", a.SessionID, "fencing_token", a.FencingToken}
	if j.PipelineID != nil {
		out = append(out, "pipeline_id", *j.PipelineID)
	}
	return append(out, TraceFields(ctx)...)
}
func TraceFields(ctx context.Context) []any {
	out := []any{}
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		out = append(out, "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
	}
	return out
}

// Incoming request ends with the HTTP response, never with the asynchronous job.
func HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.URL.Path == "/healthz" || r.Method == "GET" {
			next.ServeHTTP(w, r)
			return
		}
		ctx, span := Start(wire.Extract(r.Context(), propagation.HeaderCarrier(r.Header)), "http.request", attribute.String("http.request.method", r.Method))
		defer span.End()
		w.Header().Set("X-ForgeGrid-Trace-ID", span.SpanContext().TraceID().String())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
