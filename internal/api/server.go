// Package api exposes the queue over a small JSON HTTP API.
package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/reshard-porter/conveyor/internal/metrics"
	"github.com/reshard-porter/conveyor/internal/queue"
)

type Server struct {
	db      *sql.DB
	client  *queue.Client
	log     *slog.Logger
	apiKey  string
	reg     *metrics.Registry
	reqs    *metrics.CounterVec
	latency *metrics.HistogramVec
}

type Config struct {
	DB       *sql.DB
	Logger   *slog.Logger
	APIKey   string // if set, /v1/* requires "Authorization: Bearer <key>"
	Registry *metrics.Registry
}

func New(cfg Config) *Server {
	s := &Server{
		db:     cfg.DB,
		client: queue.NewClient(cfg.DB),
		log:    cfg.Logger,
		apiKey: cfg.APIKey,
		reg:    cfg.Registry,
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	if s.reg == nil {
		s.reg = metrics.NewRegistry()
	}
	s.reqs = s.reg.NewCounterVec("conveyor_http_requests_total", "HTTP requests by route and status.", "route", "status")
	s.latency = s.reg.NewHistogramVec("conveyor_http_request_duration_seconds", "HTTP latency by route.", nil, "route")
	s.reg.Collect(s.collectQueueStats)
	return s
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, h http.HandlerFunc, auth bool) {
		var handler http.Handler = h
		if auth {
			handler = s.requireAPIKey(handler)
		}
		mux.Handle(pattern, s.instrument(pattern, handler))
	}
	route("POST /v1/jobs", s.enqueue, true)
	route("POST /v1/jobs/batch", s.enqueueBatch, true)
	route("GET /v1/jobs", s.listJobs, true)
	route("GET /v1/jobs/{id}", s.getJob, true)
	route("POST /v1/jobs/{id}/cancel", s.cancelJob, true)
	route("POST /v1/jobs/{id}/retry", s.retryJob, true)
	route("GET /v1/queues", s.stats, true)
	route("POST /v1/queues/{queue}/pause", s.pauseQueue, true)
	route("POST /v1/queues/{queue}/resume", s.resumeQueue, true)
	route("POST /v1/queues/{queue}/retry-dead", s.retryDead, true)
	route("GET /healthz", s.health, false)
	mux.Handle("GET /metrics", s.reg.Handler())
	// The dashboard page itself is public; its API calls carry the key.
	mux.HandleFunc("GET /{$}", serveDashboard)
	return s.recoverer(s.requestID(mux))
}

// ---- handlers ----

type enqueueRequest struct {
	Queue          string          `json:"queue"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          *time.Time      `json:"run_at"`
	DelaySeconds   float64         `json:"delay_seconds"`
	TimeoutSeconds float64         `json:"timeout_seconds"`
	UniqueKey      string          `json:"unique_key"`
}

func (r enqueueRequest) params() queue.EnqueueParams {
	p := queue.EnqueueParams{
		Queue: r.Queue, Kind: r.Kind, Priority: r.Priority, MaxAttempts: r.MaxAttempts,
		RunAt: r.RunAt, UniqueKey: r.UniqueKey,
		Delay:   time.Duration(r.DelaySeconds * float64(time.Second)),
		Timeout: time.Duration(r.TimeoutSeconds * float64(time.Second)),
	}
	if len(r.Payload) > 0 {
		p.Payload = r.Payload
	}
	return p
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if !s.decode(w, r, &req) {
		return
	}
	job, created, err := s.client.Enqueue(r.Context(), req.params())
	if err != nil {
		s.error(w, r, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK // deduplicated by unique_key
	}
	writeJSON(w, status, map[string]any{"job": job, "created": created})
}

func (s *Server) enqueueBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Jobs []enqueueRequest `json:"jobs"`
	}
	if !s.decode(w, r, &req) {
		return
	}
	if len(req.Jobs) == 0 || len(req.Jobs) > 1000 {
		s.error(w, r, &queue.ValidationError{Msg: "jobs must contain between 1 and 1000 entries"})
		return
	}
	ps := make([]queue.EnqueueParams, len(req.Jobs))
	for i, j := range req.Jobs {
		ps[i] = j.params()
	}
	ids, err := s.client.EnqueueMany(r.Context(), ps)
	if err != nil {
		s.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"ids": ids, "inserted": len(ids), "skipped": len(ps) - len(ids)})
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	p := queue.ListParams{Queue: q.Get("queue"), State: queue.State(q.Get("state")), Kind: q.Get("kind")}
	var err error
	if v := q.Get("before"); v != "" {
		if p.Before, err = strconv.ParseInt(v, 10, 64); err != nil {
			s.error(w, r, &queue.ValidationError{Msg: "before must be an integer"})
			return
		}
	}
	if v := q.Get("limit"); v != "" {
		if p.Limit, err = strconv.Atoi(v); err != nil {
			s.error(w, r, &queue.ValidationError{Msg: "limit must be an integer"})
			return
		}
	}
	jobs, err := s.client.List(r.Context(), p)
	if err != nil {
		s.error(w, r, err)
		return
	}
	if jobs == nil {
		jobs = []*queue.Job{}
	}
	resp := map[string]any{"jobs": jobs}
	if limit := p.EffectiveLimit(); len(jobs) == limit {
		resp["next_before"] = jobs[len(jobs)-1].ID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) withJobID(w http.ResponseWriter, r *http.Request, fn func(id int64) (*queue.Job, error)) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		s.error(w, r, &queue.ValidationError{Msg: "invalid job id"})
		return
	}
	job, err := fn(id)
	if err != nil {
		s.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	s.withJobID(w, r, func(id int64) (*queue.Job, error) { return s.client.Get(r.Context(), id) })
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	s.withJobID(w, r, func(id int64) (*queue.Job, error) { return s.client.Cancel(r.Context(), id) })
}

func (s *Server) retryJob(w http.ResponseWriter, r *http.Request) {
	s.withJobID(w, r, func(id int64) (*queue.Job, error) { return s.client.Retry(r.Context(), id) })
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.client.Stats(r.Context())
	if err != nil {
		s.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": st})
}

func (s *Server) pauseQueue(w http.ResponseWriter, r *http.Request) {
	if err := s.client.Pause(r.Context(), r.PathValue("queue")); err != nil {
		s.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queue": r.PathValue("queue"), "paused": true})
}

func (s *Server) resumeQueue(w http.ResponseWriter, r *http.Request) {
	if err := s.client.Resume(r.Context(), r.PathValue("queue")); err != nil {
		s.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queue": r.PathValue("queue"), "paused": false})
}

func (s *Server) retryDead(w http.ResponseWriter, r *http.Request) {
	n, err := s.client.RetryDead(r.Context(), r.PathValue("queue"))
	if err != nil {
		s.error(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"requeued": n})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "unhealthy", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) collectQueueStats(w io.Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	st, err := s.client.Stats(ctx)
	if err != nil {
		s.log.Warn("metrics: queue stats failed", "err", err)
		return
	}
	fmt.Fprintln(w, "# HELP conveyor_queue_jobs Jobs per queue and state.\n# TYPE conveyor_queue_jobs gauge")
	for _, q := range st {
		for state, v := range map[string]int64{"ready": q.Ready, "scheduled": q.Scheduled, "running": q.Running,
			"succeeded": q.Succeeded, "dead": q.Dead, "cancelled": q.Cancelled} {
			metrics.WriteGauge(w, "conveyor_queue_jobs", float64(v), "queue", q.Queue, "state", state)
		}
	}
	fmt.Fprintln(w, "# HELP conveyor_queue_oldest_ready_seconds Age of the oldest runnable job (queue latency).\n# TYPE conveyor_queue_oldest_ready_seconds gauge")
	for _, q := range st {
		metrics.WriteGauge(w, "conveyor_queue_oldest_ready_seconds", q.OldestReadyAgeS, "queue", q.Queue)
	}
}

//go:embed dashboard.html
var dashboardHTML []byte

func serveDashboard(w http.ResponseWriter, _ *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:; connect-src 'self'")
	_, _ = w.Write(dashboardHTML)
}

// ---- plumbing ----

const maxBody = 1 << 20

func (s *Server) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		s.error(w, r, &queue.ValidationError{Msg: "invalid JSON body: " + err.Error()})
		return false
	}
	return true
}

func (s *Server) error(w http.ResponseWriter, r *http.Request, err error) {
	var ve *queue.ValidationError
	switch {
	case errors.As(err, &ve):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": ve.Msg})
	case errors.Is(err, queue.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, queue.ErrInvalidState):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, context.Canceled):
		w.WriteHeader(499) // client went away
	default:
		s.log.Error("request failed", "path", r.URL.Path, "request_id", r.Header.Get("X-Request-Id"), "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func (s *Server) requireAPIKey(next http.Handler) http.Handler {
	if s.apiKey == "" {
		return next
	}
	want := []byte("Bearer " + s.apiKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or invalid API key"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *Server) instrument(route string, next http.Handler) http.Handler {
	name := route[strings.IndexByte(route, ' ')+1:]
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		d := time.Since(start)
		s.reqs.Inc(name, strconv.Itoa(rec.status))
		s.latency.Observe(d.Seconds(), name)
		s.log.Debug("http request", "method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration", d, "request_id", r.Header.Get("X-Request-Id"))
	})
}

func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" || len(id) > 128 {
			b := make([]byte, 8)
			_, _ = rand.Read(b)
			id = hex.EncodeToString(b)
			r.Header.Set("X-Request-Id", id)
		}
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil && v != http.ErrAbortHandler {
				s.log.Error("panic in handler", "panic", v, "stack", string(debug.Stack()))
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}
