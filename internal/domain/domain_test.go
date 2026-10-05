package domain

import (
	"testing"
	"time"
)

func TestAuthority(t *testing.T) {
	now := time.Now()
	id := Identity{"attempt", 1, "session"}
	current := "attempt"
	job := Job{CurrentAttemptID: &current, FencingToken: 1}
	attempt := Attempt{Identity: id, State: "RUNNING", LeaseExpiresAt: now.Add(time.Second)}
	if !Owns(job, attempt, id, now) {
		t.Fatal("valid owner rejected")
	}
	for _, mutate := range []func(*Job, *Attempt, *Identity){
		func(j *Job, a *Attempt, i *Identity) { i.AttemptID = "other" },
		func(j *Job, a *Attempt, i *Identity) { i.FencingToken = 0 },
		func(j *Job, a *Attempt, i *Identity) { i.SessionID = "old" },
		func(j *Job, a *Attempt, i *Identity) { j.CurrentAttemptID = nil },
		func(j *Job, a *Attempt, i *Identity) { a.State = "LOST" },
		func(j *Job, a *Attempt, i *Identity) { a.LeaseExpiresAt = now },
	} {
		j, a, i := job, attempt, id
		mutate(&j, &a, &i)
		if Owns(j, a, i, now) {
			t.Fatal("stale owner accepted")
		}
	}
}
func TestRetryPolicy(t *testing.T) {
	for _, c := range []struct {
		r      Result
		n, max int
		want   string
	}{
		{Result{State: "SUCCEEDED"}, 1, 2, "SUCCEEDED"},
		{Result{State: "LOST", FailureKind: "LEASE_EXPIRED"}, 1, 2, "RETRY_WAIT"},
		{Result{State: "FAILED", FailureKind: "EXECUTOR_INFRA_ERROR"}, 2, 2, "FAILED"},
		{Result{State: "FAILED", FailureKind: "EXIT_NON_ZERO"}, 1, 2, "FAILED"},
		{Result{State: "TIMED_OUT", FailureKind: "JOB_TIMEOUT"}, 1, 2, "FAILED"},
	} {
		if got := NextJobState(c.n, c.max, c.r); got != c.want {
			t.Fatalf("got %s want %s", got, c.want)
		}
	}
	if RetryDelay(100, time.Second) != 30*time.Second {
		t.Fatal("unbounded backoff")
	}
}
func TestSpecValidation(t *testing.T) {
	valid := Spec{"alpine:3.22", []string{"echo", "hello"}, 30, 2}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.Command = []string{"echo", "bad\x00"}
	if invalid.Validate() == nil {
		t.Fatal("NUL accepted")
	}
	invalid = valid
	invalid.MaxAttempts = 11
	if invalid.Validate() == nil {
		t.Fatal("unbounded retries accepted")
	}
}
