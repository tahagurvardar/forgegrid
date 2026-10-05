package controlplane

import (
	"context"
	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/domain"
	"forgegrid/internal/observability"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestAssignmentCarriesAttemptTraceAcrossProtobuf(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() { otel.SetTracerProvider(old); _ = provider.Shutdown(context.Background()) }()
	ctx, span := observability.Start(context.Background(), "scheduler.create_attempt")
	span.End()
	s := New(nil, 0)
	ch := s.attach("session")
	<-ch
	p := "pipeline"
	a := &domain.Assignment{Job: domain.Job{ID: "job", PipelineID: &p}, Attempt: domain.Attempt{Identity: domain.Identity{AttemptID: "attempt", SessionID: "session", FencingToken: 2}, TraceParent: observability.Carrier(ctx), Number: 2}}
	if !s.dispatch(context.Background(), a) {
		t.Fatal("dispatch failed")
	}
	sent := <-ch
	data, err := proto.Marshal(sent)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pb.ControlMessage
	if err = proto.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	run := decoded.GetRunAttempt()
	sc := trace.SpanContextFromContext(observability.Parent(context.Background(), run.TraceParent))
	if sc.TraceID() != span.SpanContext().TraceID() || run.PipelineId != p || run.Identity.FencingToken != 2 || run.AttemptNumber != 2 {
		t.Fatal("trace/attempt identity missing")
	}
}
