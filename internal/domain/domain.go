package domain

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrStale = errors.New("STALE_ATTEMPT")
var ErrSession = errors.New("INVALID_SESSION")
var ErrConflict = errors.New("CONFLICTING_COMPLETION")
var ErrCancelled = errors.New("CANCELLATION_REQUESTED")
var ErrTerminal = errors.New("ALREADY_TERMINAL")
var ErrTimeout = errors.New("EXECUTION_TIMEOUT")

type Spec struct {
	Image          string   `json:"image"`
	Command        []string `json:"command"`
	TimeoutSeconds int      `json:"timeout_seconds"`
	MaxAttempts    int      `json:"max_attempts"`
}

func (s Spec) Validate() error {
	if strings.TrimSpace(s.Image) == "" || len(s.Image) > 512 || strings.ContainsAny(s.Image, "\x00\r\n") {
		return errors.New("image is required (maximum 512 bytes)")
	}
	if len(s.Command) == 0 || len(s.Command) > 256 || s.Command[0] == "" {
		return errors.New("command must be a nonempty argv array")
	}
	total := 0
	for _, arg := range s.Command {
		total += len(arg)
		if strings.ContainsRune(arg, 0) {
			return errors.New("command contains NUL")
		}
	}
	if total > 65536 {
		return errors.New("command exceeds 64 KiB")
	}
	if s.TimeoutSeconds < 1 || s.TimeoutSeconds > 86400 {
		return errors.New("timeout_seconds must be 1..86400")
	}
	if s.MaxAttempts < 1 || s.MaxAttempts > 10 {
		return errors.New("max_attempts must be 1..10")
	}
	return nil
}

type Identity struct {
	AttemptID    string `json:"attempt_id"`
	FencingToken int64  `json:"fencing_token"`
	SessionID    string `json:"worker_session_id"`
}
type Attempt struct {
	Identity
	TraceParent       string     `json:"-"`
	AssignedAt        time.Time  `json:"assigned_at"`
	StartedAt         *time.Time `json:"started_at"`
	FinishedAt        *time.Time `json:"finished_at"`
	TraceID           string     `json:"trace_id,omitempty"`
	JobID             string     `json:"job_id"`
	Number            int        `json:"attempt_number"`
	WorkerID          string     `json:"worker_id"`
	State             string     `json:"state"`
	LeaseExpiresAt    time.Time  `json:"lease_expires_at"`
	ExecutionDeadline *time.Time `json:"execution_deadline_at,omitempty"`
	ExitCode          *int32     `json:"exit_code"`
	FailureKind       string     `json:"failure_kind"`
	FailureDetail     string     `json:"failure_detail"`
}
type Job struct {
	TraceParent      string     `json:"-"`
	CreatedAt        time.Time  `json:"created_at"`
	FinishedAt       *time.Time `json:"finished_at"`
	RetryAvailableAt *time.Time `json:"retry_available_at"`
	ID               string     `json:"id"`
	PipelineID       *string    `json:"pipeline_id,omitempty"`
	Key              *string    `json:"key,omitempty"`
	Dependencies     []string   `json:"dependencies,omitempty"`
	Spec
	State            string    `json:"state"`
	CurrentAttemptID *string   `json:"current_attempt_id"`
	FencingToken     int64     `json:"fencing_token"`
	AttemptCount     int       `json:"attempt_count"`
	Attempts         []Attempt `json:"attempts"`
}
type Assignment struct {
	Job     Job
	Attempt Attempt
}
type Result struct {
	State       string `json:"state"`
	ExitCode    int32  `json:"exit_code"`
	FailureKind string `json:"failure_kind"`
	Detail      string `json:"detail"`
}

func (r Result) Validate() error {
	switch r.State {
	case "SUCCEEDED":
		if r.ExitCode != 0 || r.FailureKind != "" {
			return errors.New("invalid success")
		}
	case "FAILED":
		if r.FailureKind != "INVALID_COMMAND" && r.FailureKind != "EXIT_NON_ZERO" && r.FailureKind != "EXECUTOR_INFRA_ERROR" && r.FailureKind != "ASSIGNMENT_REJECTED" {
			return errors.New("invalid failure kind")
		}
		if r.FailureKind == "EXIT_NON_ZERO" && r.ExitCode == 0 {
			return errors.New("nonzero exit required")
		}
	case "TIMED_OUT":
		if r.FailureKind != "JOB_TIMEOUT" {
			return errors.New("invalid timeout")
		}
	case "CANCELLED":
		if r.FailureKind != "JOB_CANCELLED" || r.ExitCode != -1 {
			return errors.New("invalid cancellation acknowledgement")
		}
	default:
		return errors.New("invalid terminal result")
	}
	return nil
}
func Active(state string) bool { return state == "ASSIGNED" || state == "RUNNING" }
func Owns(job Job, attempt Attempt, id Identity, now time.Time) bool {
	return job.CurrentAttemptID != nil && *job.CurrentAttemptID == id.AttemptID && attempt.AttemptID == id.AttemptID && job.FencingToken == id.FencingToken && attempt.FencingToken == id.FencingToken && attempt.SessionID == id.SessionID && Active(attempt.State) && now.Before(attempt.LeaseExpiresAt)
}
func Retryable(kind string) bool {
	return kind == "EXECUTOR_INFRA_ERROR" || kind == "ASSIGNMENT_REJECTED" || kind == "LEASE_EXPIRED"
}
func NextJobState(attempts, max int, result Result) string {
	if result.State == "CANCELLED" {
		return "CANCELLED"
	}
	if result.State == "SUCCEEDED" {
		return "SUCCEEDED"
	}
	if Retryable(result.FailureKind) && attempts < max {
		return "RETRY_WAIT"
	}
	return "FAILED"
}
func RetryDelay(attempt int, base time.Duration) time.Duration {
	if attempt > 6 {
		attempt = 6
	}
	if attempt < 1 {
		attempt = 1
	}
	d := base * time.Duration(1<<(attempt-1))
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
func BoundedDetail(s string) string {
	s = strings.ToValidUTF8(s, "?")
	s = strings.Map(func(r rune) rune {
		if r < 32 && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, s)
	if len(s) > 2048 {
		s = s[:2048]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}
