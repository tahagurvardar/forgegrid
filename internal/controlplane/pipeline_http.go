package controlplane

import (
	"encoding/json"
	"errors"
	pb "forgegrid/gen/go/forgegrid/v1"
	"forgegrid/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
)

func httpID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "invalid ID", 400)
		return "", false
	}
	return id, true
}
func transitionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		http.Error(w, "not found", 404)
	case errors.Is(err, domain.ErrTerminal):
		http.Error(w, err.Error(), 409)
	default:
		http.Error(w, "database transition failed", 503)
	}
}
func (s *Server) pipelineRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		var spec domain.PipelineSpec
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024))
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
		spec = spec.WithDefaults()
		if err := spec.Validate(); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		id, err := s.Store.SubmitPipeline(r.Context(), spec)
		if err != nil {
			transitionError(w, err)
			return
		}
		jsonResponse(w, 201, map[string]string{"id": id, "state": "RUNNING"})
	})
	mux.HandleFunc("GET /api/v1/pipelines/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := httpID(w, r)
		if !ok {
			return
		}
		p, err := s.Store.GetPipeline(r.Context(), id)
		if err != nil {
			transitionError(w, err)
			return
		}
		jsonResponse(w, 200, p)
	})
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		id, ok := httpID(w, r)
		if !ok {
			return
		}
		if err := s.cancelJob(r.Context(), id); err != nil {
			transitionError(w, err)
			return
		}
		jsonResponse(w, 202, map[string]string{"id": id, "status": "cancellation requested"})
	})
	mux.HandleFunc("POST /api/v1/pipelines/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		id, ok := httpID(w, r)
		if !ok {
			return
		}
		active, err := s.Store.CancelPipeline(r.Context(), id)
		if err != nil {
			transitionError(w, err)
			return
		}
		for _, i := range active {
			s.send(i.SessionID, &pb.ControlMessage{Body: &pb.ControlMessage_CancelAttempt{CancelAttempt: &pb.CancelAttempt{Identity: wire(i), Reason: domain.ErrCancelled.Error()}}})
		}
		jsonResponse(w, 202, map[string]string{"id": id, "status": "cancellation requested"})
	})
}
