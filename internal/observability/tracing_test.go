package observability

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExporterUsesOTLPSignalPathFromEnvironment(t *testing.T) {
	for _, c := range []struct {
		name, basePath, signalPath, want string
	}{
		{"base-url", "", "", "/v1/traces"},
		{"trailing-slash", "/", "", "/v1/traces"},
		{"base-prefix", "/telemetry", "", "/telemetry/v1/traces"},
		{"signal-override", "", "/custom/traces", "/custom/traces"},
	} {
		t.Run(c.name, func(t *testing.T) {
			paths := make(chan string, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths <- r.URL.Path
				if r.URL.Path != c.want {
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer backend.Close()
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", backend.URL+c.basePath)
			signal := ""
			if c.signalPath != "" {
				signal = backend.URL + c.signalPath
			}
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", signal)
			t.Setenv("OTEL_TRACES_EXPORTER", "")
			old := otel.GetTracerProvider()
			shutdown := Init("endpoint-test")
			defer func() { shutdown(); otel.SetTracerProvider(old) }()
			_, span := Start(context.Background(), "endpoint-proof")
			span.End()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := otel.GetTracerProvider().(*sdktrace.TracerProvider).ForceFlush(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-paths:
				if got != c.want {
					t.Fatalf("export path=%s, want %s", got, c.want)
				}
			case <-ctx.Done():
				t.Fatal("export did not reach the HTTP backend")
			}
		})
	}
}

func TestExporterRejectsOversizedCollectorResponseWithoutRetry(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(status)
				_, _ = w.Write(make([]byte, 8*1024*1024))
			}))
			defer backend.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(backend.URL), otlptracehttp.WithTimeout(time.Second), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
			if err != nil {
				t.Fatal(err)
			}
			defer exporter.Shutdown(ctx)
			err = exporter.ExportSpans(ctx, []sdktrace.ReadOnlySpan{tracetest.SpanStub{Name: "bounded-response"}.Snapshot()})
			if err == nil || !strings.Contains(err.Error(), "response body too large") {
				t.Fatalf("oversized collector response was not bounded: %v", err)
			}
			if requests.Load() != 1 {
				t.Fatalf("exporter retried an oversized response: %d requests", requests.Load())
			}
		})
	}
}

func TestTracePropagationPreservesCancellationAndRejectsMalformedContext(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(old); _ = provider.Shutdown(context.Background()) })
	root, span := Start(context.Background(), "submit")
	parent := Carrier(root)
	span.End()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	child := Parent(cancelled, parent)
	if child.Err() != context.Canceled || trace.SpanContextFromContext(child).TraceID() != span.SpanContext().TraceID() {
		t.Fatal("propagation changed cancellation or trace identity")
	}
	for _, invalid := range []string{"", "garbage", "00-00000000000000000000000000000000-0000000000000000-01"} {
		if trace.SpanContextFromContext(Parent(context.Background(), invalid)).IsValid() {
			t.Fatal("invalid parent accepted")
		}
	}
}
func TestUnavailableExporterAndFullQueueNeverBlockSpanEnd(t *testing.T) {
	old := otel.GetTracerProvider()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	shutdown := Init("test")
	defer func() { shutdown(); otel.SetTracerProvider(old) }()
	started := time.Now()
	for i := 0; i < 10000; i++ {
		_, s := Start(context.Background(), "test")
		s.End()
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("bounded queue blocked producer")
	}
}
func TestExporterTimeoutIsIndependentOfSpanProducer(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		w.WriteHeader(200)
	}))
	defer backend.Close()
	defer close(release)
	old := otel.GetTracerProvider()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", backend.URL)
	t.Setenv("OTEL_TRACES_EXPORTER", "")
	shutdown := Init("test")
	defer func() { shutdown(); otel.SetTracerProvider(old) }()
	_, s := Start(context.Background(), "first")
	s.End()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("export did not start")
	}
	started := time.Now()
	for i := 0; i < 5000; i++ {
		_, s := Start(context.Background(), "while_backend_hangs")
		s.End()
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("slow backend blocked producer")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	flushStarted := time.Now()
	_ = otel.GetTracerProvider().(*sdktrace.TracerProvider).ForceFlush(ctx)
	if time.Since(flushStarted) > 2*time.Second {
		t.Fatal("export timeout was unbounded")
	}
}
func TestMetricLabelsAreClosedAndContainNoIDs(t *testing.T) {
	m := NewWorkerMetrics()
	for _, input := range []string{"SUCCEEDED", "FAILED", "TIMED_OUT", "LOST", "CANCELLED", "random-worker-or-job-id"} {
		m.WorkerExecutions.WithLabelValues(Result(input)).Inc()
	}
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		for _, metric := range f.Metric {
			for _, label := range metric.Label {
				if label.GetName() != "result" {
					t.Fatalf("unexpected label %s", label.GetName())
				}
				switch label.GetValue() {
				case "SUCCEEDED", "FAILED", "TIMED_OUT", "LOST", "CANCELLED", "OTHER":
				default:
					t.Fatal("unbounded label")
				}
			}
		}
	}
}
