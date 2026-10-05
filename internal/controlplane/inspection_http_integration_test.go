//go:build integration

package controlplane

import (
	"bufio"
	"context"
	"encoding/json"
	"forgegrid/internal/domain"
	"forgegrid/internal/store/postgres"
	"forgegrid/internal/testutil"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReadOnlyConsoleInspectionAndSSEReplay(t *testing.T) {
	pool, ctx := testutil.Database(t)
	store := &postgres.Store{Pool: pool, Lease: 10 * time.Second, Offline: 6 * time.Second}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	session := uuid.NewString()
	if err := store.Register(ctx, "worker-b", session); err != nil {
		t.Fatal(err)
	}
	id, err := store.SubmitPipeline(ctx, domain.PipelineSpec{Jobs: []domain.PipelineJobSpec{{Key: "root", Spec: domain.Spec{Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 30, MaxAttempts: 2}}}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := store.Schedule(ctx, []string{session})
	if err != nil || a == nil {
		t.Fatal(err)
	}
	if _, err = store.Advance(ctx, a.Attempt.Identity, "start"); err != nil {
		t.Fatal(err)
	}
	for sequence := int64(1); sequence <= 3; sequence++ {
		if err = store.AppendLog(ctx, a.Attempt.Identity, postgres.LogChunk{Sequence: sequence, Stream: "STDOUT", Payload: []byte("proof")}); err != nil {
			t.Fatal(err)
		}
	}
	before := func() string {
		var v string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_object('jobs',(SELECT jsonb_agg(to_jsonb(j)) FROM jobs j),'attempts',(SELECT jsonb_agg(to_jsonb(a)) FROM job_attempts a),'workers',(SELECT jsonb_agg(to_jsonb(w)) FROM worker_sessions w))::text`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	snapshot := before()
	server := httptest.NewServer(New(store, 2*time.Second).HTTP())
	defer server.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	for _, path := range []string{"/api/v1/pipelines", "/api/v1/jobs", "/api/v1/overview", "/api/v1/attempts/" + a.Attempt.AttemptID, "/api/v1/workers"} {
		response, e := client.Get(server.URL + path)
		if e != nil {
			t.Fatal(e)
		}
		if response.StatusCode != 200 {
			t.Fatal(path, response.StatusCode)
		}
		response.Body.Close()
	}
	p, e := store.GetPipeline(ctx, id)
	if e != nil || p.CreatedAt.IsZero() || p.StartedAt == nil || p.Jobs[0].Attempts[0].AssignedAt.IsZero() {
		t.Fatal("missing persisted lifecycle timestamps", e)
	}
	req, e := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/v1/attempts/"+a.Attempt.AttemptID+"/logs/stream?after=1", nil)
	if e != nil {
		t.Fatal(e)
	}
	req.Header.Set("Last-Event-ID", "2")
	response, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	if response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatal("SSE unavailable")
	}
	scanner := bufio.NewScanner(response.Body)
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "id:") && line != "id: 3" {
			t.Fatal("replayed old cursor", line)
		}
		if strings.HasPrefix(line, "data:") {
			var c postgres.LogChunk
			if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &c); err != nil || c.Sequence != 3 {
				t.Fatal("invalid SSE payload", err)
			}
			found = true
			break
		}
	}
	response.Body.Close()
	if !found {
		t.Fatal("SSE chunk not received", scanner.Err())
	}
	if before() != snapshot {
		t.Fatal("inspection changed authoritative state")
	}
	for _, path := range []string{"/api/v1/jobs?offset=-1", "/api/v1/pipelines?offset=100001", "/api/v1/attempts/" + a.Attempt.AttemptID + "/logs/stream?after=-1"} {
		r, e := client.Get(server.URL + path)
		if e != nil {
			t.Fatal(e)
		}
		r.Body.Close()
		if r.StatusCode != 400 {
			t.Fatal("invalid cursor accepted", path)
		}
	}
	missing, e := client.Get(server.URL + "/api/v1/attempts/" + uuid.NewString() + "/logs/stream")
	if e != nil {
		t.Fatal(e)
	}
	missing.Body.Close()
	if missing.StatusCode != 404 {
		t.Fatal("missing attempt stream accepted")
	}
	// Complete and replay a late diagnostic log without reopening execution authority.
	if _, e = store.Complete(ctx, a.Attempt.Identity, domain.Result{State: "SUCCEEDED", ExitCode: 0}); e != nil {
		t.Fatal(e)
	}
	if e = store.AppendLog(ctx, a.Attempt.Identity, postgres.LogChunk{Sequence: 4, Stream: "STDERR", Payload: []byte("late")}); e != nil {
		t.Fatal(e)
	}
	timeout, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, _ = http.NewRequestWithContext(timeout, "GET", server.URL+"/api/v1/attempts/"+a.Attempt.AttemptID+"/logs/stream?after=3", nil)
	response, e = client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	scanner = bufio.NewScanner(response.Body)
	found = false
	for scanner.Scan() {
		if scanner.Text() == "id: 4" {
			found = true
			break
		}
	}
	response.Body.Close()
	if !found {
		t.Fatal("terminal diagnostics lost")
	}
}

func TestConsoleInspectionPagination(t *testing.T) {
	pool, ctx := testutil.Database(t)
	store := &postgres.Store{Pool: pool}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 51; i++ {
		if _, err := store.SubmitPipeline(ctx, domain.PipelineSpec{Jobs: []domain.PipelineJobSpec{{Key: "root", Spec: domain.Spec{Image: "alpine", Command: []string{"true"}, TimeoutSeconds: 30, MaxAttempts: 2}}}}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.Pipelines(ctx, 0)
	if err != nil || len(first.Items) != 50 || first.NextOffset == nil || *first.NextOffset != 50 {
		t.Fatal("pipeline first page", first, err)
	}
	last, err := store.Pipelines(ctx, 50)
	if err != nil || len(last.Items) != 1 || last.NextOffset != nil {
		t.Fatal("pipeline last page", last, err)
	}
	for _, p := range first.Items {
		if p.ID == last.Items[0].ID || p.JobCount != 1 || p.State != "RUNNING" {
			t.Fatal("overlap or incorrect summary", p)
		}
	}
	jobs, err := store.Jobs(ctx, 0)
	if err != nil || len(jobs.Items) != 50 || jobs.NextOffset == nil || *jobs.NextOffset != 50 {
		t.Fatal("jobs first page", jobs, err)
	}
	tail, err := store.Jobs(ctx, 50)
	if err != nil || len(tail.Items) != 1 || tail.NextOffset != nil || tail.Items[0].State != "QUEUED" {
		t.Fatal("jobs last page", tail, err)
	}
	for _, j := range jobs.Items {
		if j.ID == tail.Items[0].ID {
			t.Fatal("job pagination overlap")
		}
	}
}
