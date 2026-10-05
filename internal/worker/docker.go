package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/domain"
)

func dockerOutput(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker %s: %w: %s", args[0], err, domain.BoundedDetail(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}
func Reconcile(ctx context.Context, worker, session string) error {
	ids, err := dockerOutput(ctx, "ps", "-aq", "--filter", "label=forgegrid.worker_id="+worker)
	if err != nil {
		return err
	}
	for _, id := range strings.Fields(ids) {
		owner, err := dockerOutput(ctx, "inspect", "--format", "{{ index .Config.Labels \"forgegrid.worker_session_id\" }}", id)
		if err != nil {
			return err
		}
		if owner != session {
			if _, err = dockerOutput(ctx, "rm", "-f", id); err != nil {
				return err
			}
		}
	}
	return nil
}

type chunkWriter struct {
	ctx    context.Context
	stream string
	emit   func(string, []byte) error
}

func (w chunkWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > 16384 {
			n = 16384
		}
		if err := w.emit(w.stream, append([]byte(nil), p[:n]...)); err != nil {
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}
func execute(ctx context.Context, worker string, a *pb.RunAttempt, emit func(string, []byte) error, started func()) domain.Result {
	infra := func(err error) domain.Result {
		return domain.Result{State: "FAILED", ExitCode: -1, FailureKind: "EXECUTOR_INFRA_ERROR", Detail: err.Error()}
	}
	name := "forgegrid-attempt-" + a.Identity.AttemptId
	// Deterministic name permits cleanup even when Docker create succeeds but its reply is lost.
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := dockerOutput(cleanup, "rm", "-f", name); err != nil {
			slog.Warn("container cleanup failed", "container", name, "error", err)
		}
	}()
	if _, err := dockerOutput(ctx, "image", "inspect", a.Image); err != nil {
		if _, err = dockerOutput(ctx, "pull", a.Image); err != nil {
			return infra(err)
		}
	}
	args := []string{"create", "--name", name, "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--label", "forgegrid.worker_id=" + worker, "--label", "forgegrid.worker_session_id=" + a.Identity.WorkerSessionId, "--label", "forgegrid.job_id=" + a.JobId, "--label", "forgegrid.attempt_id=" + a.Identity.AttemptId, "--label", "forgegrid.fencing_token=" + strconv.FormatInt(a.Identity.FencingToken, 10), "--entrypoint", a.Command[0], a.Image}
	args = append(args, a.Command[1:]...)
	if _, err := dockerOutput(ctx, args...); err != nil {
		return infra(err)
	}
	if _, err := dockerOutput(ctx, "start", name); err != nil {
		detail, inspectErr := dockerOutput(ctx, "inspect", "--format", "{{.State.Error}}", name)
		if inspectErr == nil && (strings.Contains(detail, "executable file not found") || strings.Contains(detail, "no such file or directory") || strings.Contains(detail, "permission denied")) {
			return domain.Result{State: "FAILED", ExitCode: 127, FailureKind: "INVALID_COMMAND", Detail: detail}
		}
		return infra(err)
	}
	started()
	logs := exec.CommandContext(ctx, "docker", "logs", "--follow", name)
	logs.Stdout = chunkWriter{ctx: ctx, stream: "STDOUT", emit: emit}
	logs.Stderr = chunkWriter{ctx: ctx, stream: "STDERR", emit: emit}
	if err := logs.Start(); err != nil {
		return infra(err)
	}
	waitOutput, waitErr := dockerOutput(ctx, "wait", name)
	logsErr := logs.Wait()
	if ctx.Err() != nil {
		return infra(ctx.Err())
	}
	if waitErr != nil {
		return infra(waitErr)
	}
	if logsErr != nil {
		return infra(logsErr)
	}
	exit, err := strconv.ParseInt(waitOutput, 10, 32)
	if err != nil {
		return infra(err)
	}
	if exit != 0 {
		return domain.Result{State: "FAILED", ExitCode: int32(exit), FailureKind: "EXIT_NON_ZERO"}
	}
	return domain.Result{State: "SUCCEEDED", ExitCode: 0}
}

// Sequencing and enqueue occur under one mutex, so observed stdout/stderr order
// has one monotonic sequence. The bounded queue applies backpressure.
func runAttempt(ctx context.Context, conn pb.WorkerLogsClient, worker string, a *pb.RunAttempt, started func()) domain.Result {
	runctx, cancel := context.WithCancel(ctx)
	defer cancel()
	chunks := make(chan *pb.LogChunk, 64)
	logDone := make(chan error, 1)
	go func() {
		for c := range chunks {
			var err error
			for retry := 0; retry < 3; retry++ {
				rpcctx, stop := context.WithTimeout(runctx, 3*time.Second)
				stream, e := conn.Stream(rpcctx)
				if e == nil {
					e = stream.Send(c)
					if e == nil {
						var ack *pb.LogAck
						ack, e = stream.Recv()
						if e == nil && ack.Sequence != c.Sequence {
							e = fmt.Errorf("log ACK sequence mismatch")
						}
					}
					stream.CloseSend()
				}
				stop()
				err = e
				if err == nil {
					break
				}
				if runctx.Err() != nil {
					break
				}
			}
			if err != nil {
				cancel()
				logDone <- err
				return
			}
		}
		logDone <- nil
	}()
	var mu sync.Mutex
	sequence := int64(0)
	emit := func(stream string, p []byte) error {
		mu.Lock()
		defer mu.Unlock()
		sequence++
		select {
		case chunks <- &pb.LogChunk{Identity: a.Identity, Sequence: sequence, Stream: stream, Payload: p}:
			return nil
		case <-runctx.Done():
			return runctx.Err()
		}
	}
	result := execute(runctx, worker, a, emit, started)
	close(chunks)
	if err := <-logDone; err != nil && ctx.Err() == nil {
		return domain.Result{State: "FAILED", ExitCode: -1, FailureKind: "EXECUTOR_INFRA_ERROR", Detail: err.Error()}
	}
	if ctx.Err() == context.DeadlineExceeded {
		return domain.Result{State: "TIMED_OUT", ExitCode: -1, FailureKind: "JOB_TIMEOUT", Detail: "job timeout elapsed"}
	}
	return result
}

var _ io.Writer = chunkWriter{}
