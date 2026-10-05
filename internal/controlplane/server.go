package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/domain"
	"forgegrid/internal/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedWorkerControlServer
	pb.UnimplementedWorkerLogsServer
	Store     *postgres.Store
	Heartbeat time.Duration
	mu        sync.Mutex
	streams   map[string]chan *pb.ControlMessage
}

func New(store *postgres.Store, heartbeat time.Duration) *Server {
	return &Server{Store: store, Heartbeat: heartbeat, streams: make(map[string]chan *pb.ControlMessage)}
}
func identity(i *pb.AttemptIdentity) (domain.Identity, error) {
	if i == nil {
		return domain.Identity{}, status.Error(codes.InvalidArgument, "identity required")
	}
	if _, err := uuid.Parse(i.AttemptId); err != nil {
		return domain.Identity{}, status.Error(codes.InvalidArgument, "invalid attempt ID")
	}
	if _, err := uuid.Parse(i.WorkerSessionId); err != nil {
		return domain.Identity{}, status.Error(codes.InvalidArgument, "invalid session ID")
	}
	if i.FencingToken < 1 {
		return domain.Identity{}, status.Error(codes.InvalidArgument, "invalid fencing token")
	}
	return domain.Identity{AttemptID: i.AttemptId, FencingToken: i.FencingToken, SessionID: i.WorkerSessionId}, nil
}
func wire(i domain.Identity) *pb.AttemptIdentity {
	return &pb.AttemptIdentity{AttemptId: i.AttemptID, FencingToken: i.FencingToken, WorkerSessionId: i.SessionID}
}
func (s *Server) connected() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.streams))
	for id := range s.streams {
		ids = append(ids, id)
	}
	return ids
}
func (s *Server) send(session string, msg *pb.ControlMessage) bool {
	s.mu.Lock()
	ch := s.streams[session]
	s.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- msg:
		return true
	default:
		return false
	}
}
func (s *Server) Connect(stream pb.WorkerControl_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	r := first.GetRegister()
	if r == nil || r.CapacitySlots != 1 {
		return status.Error(codes.InvalidArgument, "first message must register capacity 1")
	}
	if err = s.Store.Register(stream.Context(), r.WorkerId, r.WorkerSessionId); err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	outgoing := s.attach(r.WorkerSessionId)
	defer func() {
		s.mu.Lock()
		delete(s.streams, r.WorkerSessionId)
		s.mu.Unlock()
		cleanup, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		if e := s.Store.Disconnect(cleanup, r.WorkerSessionId); e != nil {
			slog.Error("disconnect persistence failed", "error", e)
		}
	}()
	sendErrors := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-outgoing:
				if e := stream.Send(msg); e != nil {
					sendErrors <- e
					return
				}
			}
		}
	}()
	slog.Info("worker registered", "worker_id", r.WorkerId, "worker_session_id", r.WorkerSessionId)
	for {
		msg, e := stream.Recv()
		if e != nil {
			return e
		}
		select {
		case e = <-sendErrors:
			return e
		default:
		}
		opctx, done := context.WithTimeout(ctx, 5*time.Second)
		e = s.handle(opctx, r.WorkerSessionId, msg)
		done()
		if e != nil {
			return e
		}
	}
}
func (s *Server) handle(ctx context.Context, session string, msg *pb.WorkerMessage) error {
	if h := msg.GetHeartbeat(); h != nil {
		if h.WorkerSessionId != session {
			return status.Error(codes.PermissionDenied, "session mismatch")
		}
		return s.Store.Heartbeat(ctx, session)
	}
	var pi *pb.AttemptIdentity
	action := ""
	switch {
	case msg.GetAssignmentAccepted() != nil:
		pi = msg.GetAssignmentAccepted()
		action = "accept"
	case msg.GetAttemptStarted() != nil:
		pi = msg.GetAttemptStarted()
		action = "start"
	case msg.GetLeaseRenewRequest() != nil:
		pi = msg.GetLeaseRenewRequest().Identity
		action = "renew"
	case msg.GetAssignmentRejected() != nil:
		pi = msg.GetAssignmentRejected().Identity
	case msg.GetAttemptCompleted() != nil:
		pi = msg.GetAttemptCompleted().Identity
	case msg.GetAttemptFailed() != nil:
		pi = msg.GetAttemptFailed().Identity
	default:
		return status.Error(codes.InvalidArgument, "unsupported message")
	}
	id, err := identity(pi)
	if err != nil {
		return err
	}
	if id.SessionID != session {
		return status.Error(codes.PermissionDenied, "session mismatch")
	}
	if action != "" {
		ttl, err := s.Store.Advance(ctx, id, action)
		if err != nil {
			if errors.Is(err, domain.ErrStale) || errors.Is(err, domain.ErrSession) || errors.Is(err, domain.ErrCancelled) {
				s.send(session, &pb.ControlMessage{Body: &pb.ControlMessage_CancelAttempt{CancelAttempt: &pb.CancelAttempt{Identity: pi, Reason: err.Error()}}})
				return nil
			}
			return err
		}
		if action == "renew" {
			if !s.send(session, &pb.ControlMessage{Body: &pb.ControlMessage_LeaseRenewed{LeaseRenewed: &pb.LeaseRenewed{Identity: pi, RequestId: msg.GetLeaseRenewRequest().RequestId, TtlMs: ttl.Milliseconds()}}}) {
				return status.Error(codes.Unavailable, "control queue full")
			}
		}
		return nil
	}
	result := msg.GetAttemptCompleted()
	if result == nil {
		result = msg.GetAttemptFailed()
	}
	if rejected := msg.GetAssignmentRejected(); rejected != nil {
		result = &pb.AttemptResult{Identity: pi, State: "FAILED", ExitCode: -1, FailureKind: "ASSIGNMENT_REJECTED", Detail: rejected.Reason}
	}
	ack, err := s.ReportResult(ctx, result)
	if err != nil {
		return err
	}
	if !s.send(session, &pb.ControlMessage{Body: &pb.ControlMessage_ResultAck{ResultAck: ack}}) {
		return status.Error(codes.Unavailable, "control queue full")
	}
	return nil
}
func (s *Server) ReportResult(ctx context.Context, r *pb.AttemptResult) (*pb.ResultAck, error) {
	if r == nil {
		return nil, status.Error(codes.InvalidArgument, "result required")
	}
	id, err := identity(r.Identity)
	if err != nil {
		return nil, err
	}
	result := domain.Result{State: r.State, ExitCode: r.ExitCode, FailureKind: r.FailureKind, Detail: r.Detail}
	if err = result.Validate(); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	dup, err := s.Store.Complete(ctx, id, result)
	ack := &pb.ResultAck{Identity: r.Identity, Accepted: err == nil, Duplicate: dup}
	if err != nil {
		if !errors.Is(err, domain.ErrStale) && !errors.Is(err, domain.ErrConflict) && !errors.Is(err, domain.ErrSession) && !errors.Is(err, domain.ErrCancelled) {
			return nil, status.Error(codes.Unavailable, "database transition failed")
		}
		ack.Error = err.Error()
	}
	slog.Info("attempt result", "attempt_id", id.AttemptID, "fencing_token", id.FencingToken, "worker_session_id", id.SessionID, "accepted", ack.Accepted, "duplicate", dup, "error", ack.Error)
	return ack, nil
}

// Queue the registration ACK before making the session visible to schedulers.
func (s *Server) attach(session string) chan *pb.ControlMessage {
	outgoing := make(chan *pb.ControlMessage, 32)
	outgoing <- &pb.ControlMessage{Body: &pb.ControlMessage_Registered{Registered: &pb.Registered{WorkerSessionId: session, HeartbeatIntervalMs: s.Heartbeat.Milliseconds()}}}
	s.mu.Lock()
	s.streams[session] = outgoing
	s.mu.Unlock()
	return outgoing
}

// Internal only; there is deliberately no HTTP or public gRPC cancel endpoint.
func (s *Server) cancelJob(ctx context.Context, jobID string) error {
	id, err := s.Store.Cancel(ctx, jobID)
	if err != nil {
		return err
	}
	if id != nil {
		s.send(id.SessionID, &pb.ControlMessage{Body: &pb.ControlMessage_CancelAttempt{CancelAttempt: &pb.CancelAttempt{Identity: wire(*id), Reason: domain.ErrCancelled.Error()}}})
	}
	return nil
}
func (s *Server) Stream(stream pb.WorkerLogs_StreamServer) error {
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		id, err := identity(c.Identity)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(stream.Context(), 5*time.Second)
		err = s.Store.AppendLog(ctx, id, postgres.LogChunk{Sequence: c.Sequence, Stream: c.Stream, Payload: c.Payload})
		cancel()
		if err != nil {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		if err = stream.Send(&pb.LogAck{Sequence: c.Sequence}); err != nil {
			return err
		}
	}
}
func (s *Server) Run(ctx context.Context) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		iteration, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := s.Store.Recover(iteration); err != nil {
			slog.Error("recovery failed", "error", err)
			cancel()
			continue
		}
		for n := 0; n < 64; n++ {
			a, err := s.Store.Schedule(iteration, s.connected())
			if err != nil {
				slog.Error("scheduling failed", "error", err)
				break
			}
			if a == nil {
				break
			}
			msg := &pb.ControlMessage{Body: &pb.ControlMessage_RunAttempt{RunAttempt: &pb.RunAttempt{Identity: wire(a.Attempt.Identity), JobId: a.Job.ID, AttemptNumber: int32(a.Attempt.Number), Image: a.Job.Image, Command: a.Job.Command, TimeoutSeconds: int32(a.Job.TimeoutSeconds)}}}
			delivered := s.send(a.Attempt.SessionID, msg)
			slog.Info("attempt assigned", "job_id", a.Job.ID, "attempt_id", a.Attempt.AttemptID, "worker_id", a.Attempt.WorkerID, "fencing_token", a.Attempt.FencingToken, "delivered", delivered)
		}
		cancel()
	}
}
func jsonResponse(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("HTTP response write failed", "error", err)
	}
}
func (s *Server) HTTP() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.Pool.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		jsonResponse(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		var spec domain.Spec
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128*1024))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&spec); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		var extra any
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			http.Error(w, "expected one JSON object", 400)
			return
		}
		if spec.TimeoutSeconds == 0 {
			spec.TimeoutSeconds = 30
		}
		if spec.MaxAttempts == 0 {
			spec.MaxAttempts = 2
		}
		if err := spec.Validate(); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		id, err := s.Store.Submit(r.Context(), spec)
		if err != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		jsonResponse(w, 201, map[string]string{"id": id, "state": "QUEUED"})
	})
	mux.HandleFunc("GET /api/v1/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, err := uuid.Parse(id); err != nil {
			http.Error(w, "invalid ID", 400)
			return
		}
		j, err := s.Store.GetJob(r.Context(), id)
		if errors.Is(err, pgx.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		jsonResponse(w, 200, j)
	})
	mux.HandleFunc("GET /api/v1/workers", func(w http.ResponseWriter, r *http.Request) {
		sessions, err := s.Store.Sessions(r.Context())
		if err != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		jsonResponse(w, 200, sessions)
	})
	mux.HandleFunc("GET /api/v1/attempts/{id}/logs", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if _, err := uuid.Parse(id); err != nil {
			http.Error(w, "invalid ID", 400)
			return
		}
		after := int64(0)
		if v := r.URL.Query().Get("after"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				http.Error(w, "invalid after", 400)
				return
			}
			after = n
		}
		logs, err := s.Store.Logs(r.Context(), id, after)
		if err != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		jsonResponse(w, 200, logs)
	})
	return http.TimeoutHandler(mux, 10*time.Second, "request timed out")
}
