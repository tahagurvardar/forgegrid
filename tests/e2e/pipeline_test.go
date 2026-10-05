//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"forgegrid/internal/domain"
	"io"
	"net/http"
	"testing"
	"time"
)

func pnode(key string, deps ...string) domain.PipelineJobSpec {
	return domain.PipelineJobSpec{Key: key, Spec: domain.Spec{Image: "alpine:3.22", Command: []string{"echo", key}, TimeoutSeconds: 30, MaxAttempts: 2}, Dependencies: deps}
}
func postPipeline(t *testing.T, nodes ...domain.PipelineJobSpec) string {
	t.Helper()
	data, err := json.Marshal(domain.PipelineSpec{Jobs: nodes})
	if err != nil {
		t.Fatal(err)
	}
	r, err := (&http.Client{Timeout: 5 * time.Second}).Post(baseURL()+"/api/v1/pipelines", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 201 {
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("pipeline submit: %d %s", r.StatusCode, body)
	}
	var p domain.Pipeline
	if err = json.NewDecoder(r.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	return p.ID
}
func pview(t *testing.T, id string) (domain.Pipeline, map[string]domain.Job) {
	t.Helper()
	var p domain.Pipeline
	get(t, "/api/v1/pipelines/"+id, &p)
	jobs := map[string]domain.Job{}
	for _, j := range p.Jobs {
		jobs[*j.Key] = j
	}
	return p, jobs
}
func awaitPipeline(t *testing.T, id string, condition func(domain.Pipeline, map[string]domain.Job) bool) domain.Pipeline {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		p, jobs := pview(t, id)
		if condition(p, jobs) {
			return p
		}
		time.Sleep(100 * time.Millisecond)
	}
	p, _ := pview(t, id)
	t.Fatalf("pipeline condition timed out: %+v", p)
	return domain.Pipeline{}
}
func terminalPipeline(p domain.Pipeline, _ map[string]domain.Job) bool {
	return p.State == "SUCCEEDED" || p.State == "FAILED" || p.State == "CANCELLED"
}
func cancelHTTP(t *testing.T, path string) {
	t.Helper()
	r, err := (&http.Client{Timeout: 5 * time.Second}).Post(baseURL()+path, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 202 {
		b, _ := io.ReadAll(r.Body)
		t.Fatalf("cancel %d: %s", r.StatusCode, b)
	}
}
func TestDockerPipelineSuccessShapes(t *testing.T) {
	for _, c := range []struct {
		name  string
		nodes []domain.PipelineJobSpec
	}{
		{"single", []domain.PipelineJobSpec{pnode("one")}},
		{"linear", []domain.PipelineJobSpec{pnode("a"), pnode("b", "a"), pnode("c", "b")}},
		{"fan-out", []domain.PipelineJobSpec{pnode("a"), pnode("b", "a"), pnode("c", "a")}},
		{"fan-in", []domain.PipelineJobSpec{pnode("a"), pnode("b"), pnode("c", "a", "b")}},
		{"diamond", []domain.PipelineJobSpec{pnode("build"), pnode("test", "build"), pnode("lint", "build"), pnode("package", "test", "lint")}},
	} {
		t.Run(c.name, func(t *testing.T) {
			id := postPipeline(t, c.nodes...)
			p := awaitPipeline(t, id, terminalPipeline)
			if p.State != "SUCCEEDED" {
				t.Fatalf("pipeline failed: %+v", p)
			}
			for _, j := range p.Jobs {
				if j.State != "SUCCEEDED" || j.AttemptCount != 1 || len(j.Attempts) != 1 {
					t.Fatalf("job %+v", j)
				}
			}
		})
	}
}
func TestDockerPipelineParallelBranchesAndFailure(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			build, test, lint, pkg := pnode("build"), pnode("unit-test", "build"), pnode("lint", "build"), pnode("package", "unit-test", "lint")
			test.Command = []string{"sleep", "3"}
			lint.Command = []string{"sleep", "3"}
			if failure {
				test.Command = []string{"/bin/sh", "-c", "sleep 1; exit 7"}
			}
			id := postPipeline(t, build, test, lint, pkg)
			if !failure {
				awaitPipeline(t, id, func(p domain.Pipeline, j map[string]domain.Job) bool {
					return p.State == "RUNNING" && j["unit-test"].State == "RUNNING" && j["lint"].State == "RUNNING" && j["package"].State == "BLOCKED"
				})
			} else {
				awaitPipeline(t, id, func(p domain.Pipeline, j map[string]domain.Job) bool {
					return p.State == "RUNNING" && j["package"].State == "SKIPPED" && !domain.TerminalJob(j["lint"].State)
				})
			}
			p := awaitPipeline(t, id, terminalPipeline)
			_, j := pview(t, id)
			want := "SUCCEEDED"
			if failure {
				want = "FAILED"
			}
			if p.State != want || j["lint"].State != "SUCCEEDED" {
				t.Fatalf("branch semantics %+v", p)
			}
			if failure {
				if j["package"].State != "SKIPPED" || j["package"].AttemptCount != 0 || j["unit-test"].AttemptCount != 1 {
					t.Fatal("failed branch retried or package executed")
				}
			} else if j["package"].State != "SUCCEEDED" {
				t.Fatal("fan-in not released")
			}
		})
	}
}
func TestDockerPipelineTimeoutAndPublicCancellation(t *testing.T) {
	for _, mode := range []string{"timeout", "job-cancel", "pipeline-cancel"} {
		t.Run(mode, func(t *testing.T) {
			root := pnode("root")
			root.Command = []string{"sleep", "20"}
			if mode == "timeout" {
				root.TimeoutSeconds = 1
			}
			id := postPipeline(t, root, pnode("downstream", "root"), pnode("independent"))
			if mode != "timeout" {
				awaitPipeline(t, id, func(_ domain.Pipeline, j map[string]domain.Job) bool { return j["root"].State == "RUNNING" })
				_, j := pview(t, id)
				if mode == "job-cancel" {
					cancelHTTP(t, "/api/v1/jobs/"+j["root"].ID+"/cancel")
				} else {
					cancelHTTP(t, "/api/v1/pipelines/"+id+"/cancel")
				}
			}
			p := awaitPipeline(t, id, terminalPipeline)
			_, j := pview(t, id)
			want := "CANCELLED"
			if mode == "timeout" {
				want = "FAILED"
			}
			if p.State != want || j["root"].AttemptCount != 1 {
				t.Fatalf("termination %+v", p)
			}
			if mode == "timeout" {
				if j["root"].Attempts[0].State != "TIMED_OUT" || j["downstream"].State != "SKIPPED" || j["independent"].State != "SUCCEEDED" {
					t.Fatal("timeout incorrectly retried or cancelled independent branch")
				}
			} else {
				if j["root"].State != "CANCELLED" {
					t.Fatal("cancellation lost")
				}
				if mode == "job-cancel" && j["downstream"].State != "SKIPPED" {
					t.Fatal("cancelled parent did not skip child")
				}
				if mode == "pipeline-cancel" && j["downstream"].State != "CANCELLED" {
					t.Fatal("blocked job not cancelled")
				}
			}
		})
	}
}
