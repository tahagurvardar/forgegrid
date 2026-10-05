//go:build integration

package controlplane

import (
	"bytes"
	"encoding/json"
	"forgegrid/internal/domain"
	"forgegrid/internal/store/postgres"
	"forgegrid/internal/testutil"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPublicPipelineHTTPValidationAndCancellation(t *testing.T) {
	pool, ctx := testutil.Database(t)
	store := &postgres.Store{Pool: pool, Lease: 10 * time.Second, Offline: 6 * time.Second}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	handler := New(store, 2*time.Second).HTTP()
	request := func(method, path, body string, want int) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
		if w.Code != want {
			t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
		}
		return w
	}
	for _, body := range []string{
		`{"jobs":[{"key":"a","image":"alpine","command":["true"],"dependencies":["a"]}]}`,
		`{"jobs":[{"key":"a","image":"alpine","command":["true"],"dependencies":["missing"]}]}`,
		`{"jobs":[{"key":"a","image":"alpine","command":["true"]},{"key":"a","image":"alpine","command":["true"]}]}`,
		`{"jobs":[{"key":"a","image":"alpine","command":["true"],"dependencies":["b"]},{"key":"b","image":"alpine","command":["true"],"dependencies":["a"]}]}`,
		`{"jobs":[],"unknown":true}`,
	} {
		request("POST", "/api/v1/pipelines", body, 400)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pipelines`).Scan(&count); err != nil || count != 0 {
		t.Fatal("invalid HTTP DAG persisted")
	}
	w := request("POST", "/api/v1/pipelines", `{"jobs":[{"key":"root","image":"alpine","command":["true"]},{"key":"child","image":"alpine","command":["true"],"dependencies":["root"]}]}`, 201)
	var p domain.Pipeline
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	p, err := store.GetPipeline(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	var root, child domain.Job
	for _, j := range p.Jobs {
		if *j.Key == "root" {
			root = j
		} else {
			child = j
		}
	}
	if root.TimeoutSeconds != 30 || root.MaxAttempts != 2 || root.State != "QUEUED" || child.State != "BLOCKED" {
		t.Fatalf("defaults/states %+v", p)
	}
	request("POST", "/api/v1/jobs/"+child.ID+"/cancel", "", 202)
	request("POST", "/api/v1/jobs/"+root.ID+"/cancel", "", 202)
	request("POST", "/api/v1/jobs/"+root.ID+"/cancel", "", 409)
	request("GET", "/api/v1/pipelines/"+p.ID, "", 200)
	request("POST", "/api/v1/pipelines/"+p.ID+"/cancel", "", 409)
	request("POST", "/api/v1/pipelines/not-a-uuid/cancel", "", 400)
	request("GET", "/api/v1/pipelines/11111111-1111-1111-1111-111111111111", "", 404)
	w = request("POST", "/api/v1/pipelines", `{"jobs":[{"key":"root","image":"alpine","command":["true"]}]}`, 201)
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	request("POST", "/api/v1/pipelines/"+p.ID+"/cancel", "", 202)
	p, err = store.GetPipeline(ctx, p.ID)
	if err != nil || p.State != "CANCELLED" {
		t.Fatalf("public pipeline cancellation %+v %v", p, err)
	}
}
