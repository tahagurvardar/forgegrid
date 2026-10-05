package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

func pageOffset(w http.ResponseWriter, r *http.Request) (int, bool) {
	if r.URL.Query().Get("offset") == "" {
		return 0, true
	}
	n, e := strconv.Atoi(r.URL.Query().Get("offset"))
	if e != nil || n < 0 || n > 100000 {
		http.Error(w, "invalid offset", 400)
		return 0, false
	}
	return n, true
}
func (s *Server) inspectionRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/pipelines", func(w http.ResponseWriter, r *http.Request) {
		n, ok := pageOffset(w, r)
		if !ok {
			return
		}
		p, e := s.Store.Pipelines(r.Context(), n)
		if e != nil {
			transitionError(w, e)
			return
		}
		jsonResponse(w, 200, p)
	})
	mux.HandleFunc("GET /api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		n, ok := pageOffset(w, r)
		if !ok {
			return
		}
		p, e := s.Store.Jobs(r.Context(), n)
		if e != nil {
			transitionError(w, e)
			return
		}
		jsonResponse(w, 200, p)
	})
	mux.HandleFunc("GET /api/v1/overview", func(w http.ResponseWriter, r *http.Request) {
		p, e := s.Store.Overview(r.Context())
		if e != nil {
			transitionError(w, e)
			return
		}
		jsonResponse(w, 200, p)
	})
	mux.HandleFunc("GET /api/v1/attempts/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, ok := httpID(w, r)
		if !ok {
			return
		}
		j, e := s.Store.AttemptJob(r.Context(), id)
		if e != nil {
			transitionError(w, e)
			return
		}
		jsonResponse(w, 200, j)
	})
}
func logCursor(r *http.Request) (int64, error) {
	var after int64
	for _, v := range []string{r.URL.Query().Get("after"), r.Header.Get("Last-Event-ID")} {
		if v == "" {
			continue
		}
		n, e := strconv.ParseInt(v, 10, 64)
		if e != nil || n < 0 {
			return 0, errors.New("invalid log cursor")
		}
		if n > after {
			after = n
		}
	}
	return after, nil
}
func (s *Server) streamLogs(w http.ResponseWriter, r *http.Request) {
	id, ok := httpID(w, r)
	if !ok {
		return
	}
	after, err := logCursor(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	// Validate existence before sending SSE headers. Terminal attempts still accept
	// diagnostic late logs, so rotation/replay does not imply physical execution stopped.
	var exists bool
	err = s.Store.Pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM job_attempts WHERE id=$1)`, id).Scan(&exists)
	if err != nil {
		http.Error(w, "database unavailable", 503)
		return
	}
	if !exists {
		http.NotFound(w, r)
		return
	}
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	_ = rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err = fmt.Fprint(w, "retry: 1000\n\n"); err != nil {
		return
	}
	if rc.Flush() != nil {
		return
	}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	rotate := time.NewTimer(30 * time.Second)
	defer rotate.Stop()
	for {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		logs, e := s.Store.Logs(ctx, id, after)
		cancel()
		if e != nil {
			return
		}
		_ = rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
		for _, chunk := range logs {
			data, e := json.Marshal(chunk)
			if e != nil {
				return
			}
			if _, e = fmt.Fprintf(w, "id: %d\nevent: chunk\ndata: %s\n\n", chunk.Sequence, data); e != nil {
				return
			}
			after = chunk.Sequence
		}
		if _, err = fmt.Fprint(w, ": keepalive\n\n"); err != nil {
			return
		}
		if rc.Flush() != nil {
			return
		}
		// Drain full pages without waiting; every replay remains strictly sequence ordered.
		if len(logs) == 1000 {
			select {
			case <-r.Context().Done():
				return
			case <-rotate.C:
				return
			default:
			}
			continue
		}
		select {
		case <-r.Context().Done():
			return
		case <-rotate.C:
			return
		case <-tick.C:
		}
	}
}
