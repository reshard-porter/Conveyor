package queue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/reshard-porter/conveyor/internal/metrics"
	"github.com/reshard-porter/conveyor/internal/pgwire"
)

// Handler processes one job. The returned value (if non-nil) is stored as
// the job's JSON result. Return Permanent(err) to skip remaining retries,
// or Snooze(d) to reschedule without consuming an attempt.
type Handler func(ctx context.Context, job *Job) (any, error)

type WorkerConfig struct {
	ID                string        // unique per process; defaults to host-pid-random
	Queues            []string      // default ["default"]
	Concurrency       int           // max jobs in flight, default 10
	PollInterval      time.Duration // fallback polling when idle, default 1s
	LeaseDuration     time.Duration // how long a claim is valid without heartbeat, default 30s
	HeartbeatInterval time.Duration // default LeaseDuration/3
	ShutdownTimeout   time.Duration // grace period for in-flight jobs, default 30s
	Backoff           BackoffFunc   // default DefaultBackoff
	Logger            *slog.Logger
	Metrics           *Metrics
}

// Metrics groups the worker's Prometheus series. A nil *Metrics is valid.
type Metrics struct {
	completed *metrics.CounterVec
	duration  *metrics.HistogramVec
	inFlight  *metrics.GaugeVec
	reaped    *metrics.CounterVec
	cleaned   *metrics.CounterVec
}

func NewMetrics(r *metrics.Registry) *Metrics {
	return &Metrics{
		completed: r.NewCounterVec("conveyor_jobs_completed_total",
			"Job attempts finished, by outcome (succeeded, retried, dead, released, snoozed, lost).", "queue", "kind", "outcome"),
		duration: r.NewHistogramVec("conveyor_job_duration_seconds",
			"Handler execution time per attempt.", nil, "queue", "kind"),
		inFlight: r.NewGaugeVec("conveyor_worker_jobs_in_flight", "Jobs currently executing in this process."),
		reaped:   r.NewCounterVec("conveyor_jobs_reaped_total", "Running jobs recovered after their lease expired."),
		cleaned:  r.NewCounterVec("conveyor_jobs_deleted_total", "Finished jobs deleted by retention cleanup.", "state"),
	}
}

func (m *Metrics) observe(j *Job, outcome string, d time.Duration) {
	if m == nil {
		return
	}
	m.completed.Inc(j.Queue, j.Kind, outcome)
	m.duration.Observe(d.Seconds(), j.Queue, j.Kind)
}

func (m *Metrics) addInFlight(d float64) {
	if m != nil {
		m.inFlight.Add(d)
	}
}

// Worker claims and executes jobs.
type Worker struct {
	db       *sql.DB
	cfg      WorkerConfig
	log      *slog.Logger
	handlers map[string]Handler
	wake     chan struct{}
	inFlight atomic.Int64
	running  atomic.Bool
}

func NewWorker(db *sql.DB, cfg WorkerConfig) *Worker {
	if cfg.ID == "" {
		host, _ := os.Hostname()
		b := make([]byte, 3)
		_, _ = rand.Read(b)
		cfg.ID = fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"default"}
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.LeaseDuration / 3
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Worker{
		db:       db,
		cfg:      cfg,
		log:      cfg.Logger.With("worker", cfg.ID),
		handlers: map[string]Handler{},
		wake:     make(chan struct{}, 1),
	}
}

func (w *Worker) ID() string { return w.cfg.ID }

// Handle registers a handler for a job kind. Must be called before Run.
// The worker only claims kinds it has handlers for, so different services
// can share a queue table safely.
func (w *Worker) Handle(kind string, h Handler) {
	if w.running.Load() {
		panic("queue: Handle called after Run")
	}
	w.handlers[kind] = h
}

// Notify asks the worker to poll now. Wire it to LISTEN conveyor_jobs; an
// empty queue name (e.g. listener reconnected) always wakes the worker.
func (w *Worker) Notify(queue string) {
	if queue != "" && !contains(w.cfg.Queues, queue) {
		return
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

var (
	errLeaseLost       = errors.New("lease lost (job cancelled or reclaimed)")
	errTimeout         = errors.New("job exceeded its timeout")
	errHandlerReturned = errors.New("handler returned")
)

// Run claims and executes jobs until ctx is cancelled, then stops claiming,
// waits up to ShutdownTimeout for in-flight jobs, and finally cancels any
// stragglers (whose jobs are released back to the queue).
func (w *Worker) Run(ctx context.Context) error {
	if len(w.handlers) == 0 {
		return errors.New("queue: worker has no handlers")
	}
	w.running.Store(true)
	kinds := make([]string, 0, len(w.handlers))
	for k := range w.handlers {
		kinds = append(kinds, k)
	}
	queuesArr, kindsArr := pgwire.TextArray(w.cfg.Queues), pgwire.TextArray(kinds)
	w.log.Info("worker started", "queues", w.cfg.Queues, "kinds", kinds, "concurrency", w.cfg.Concurrency)

	// Job contexts are detached from ctx: shutdown should let jobs finish
	// gracefully, not yank them mid-flight.
	jobCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()
	var wg sync.WaitGroup
	slotFreed := make(chan struct{}, 1)
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()
	failures := 0

	for ctx.Err() == nil {
		free := w.cfg.Concurrency - int(w.inFlight.Load())
		if free > 0 {
			jobs, err := w.claim(ctx, queuesArr, kindsArr, free)
			switch {
			case err != nil && ctx.Err() == nil:
				failures++
				w.log.Error("claim failed", "err", err)
				// Back off on repeated DB errors instead of hammering a sick database.
				sleep(ctx, min(time.Duration(failures)*time.Second, 10*time.Second))
				continue
			case err == nil:
				failures = 0
			}
			for _, j := range jobs {
				w.inFlight.Add(1)
				w.cfg.Metrics.addInFlight(1)
				wg.Add(1)
				go func(j *Job) {
					defer func() {
						w.inFlight.Add(-1)
						w.cfg.Metrics.addInFlight(-1)
						wg.Done()
						select {
						case slotFreed <- struct{}{}:
						default:
						}
					}()
					w.execute(ctx, jobCtx, j)
				}(j)
			}
			// A full batch suggests there's more work: claim again right away.
			if len(jobs) == free {
				continue
			}
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		case <-w.wake:
		case <-slotFreed:
		}
	}

	w.log.Info("worker draining", "in_flight", w.inFlight.Load(), "timeout", w.cfg.ShutdownTimeout)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.cfg.ShutdownTimeout):
		w.log.Warn("shutdown timeout reached; cancelling in-flight jobs", "in_flight", w.inFlight.Load())
		cancelJobs()
		<-done
	}
	w.log.Info("worker stopped")
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

var qualifiedJobColumns = func() string {
	cols := strings.Split(jobColumns, ",")
	for i, c := range cols {
		cols[i] = "j." + strings.TrimSpace(c)
	}
	return strings.Join(cols, ", ")
}()

// claimSQL is the heart of the queue. FOR UPDATE SKIP LOCKED lets any number
// of workers select from the same index concurrently without blocking each
// other or double-claiming: rows another transaction has locked are skipped.
const claimSQL = `
WITH next AS (
    SELECT id
    FROM conveyor_jobs j
    WHERE state = 'pending'
      AND run_at <= now()
      AND queue = ANY($1::text[])
      AND kind  = ANY($2::text[])
      AND NOT EXISTS (SELECT 1 FROM conveyor_paused_queues p WHERE p.queue = j.queue)
    ORDER BY priority DESC, run_at, id
    LIMIT $3::int
    FOR UPDATE SKIP LOCKED
)
UPDATE conveyor_jobs j
SET state        = 'running',
    attempts     = j.attempts + 1,
    locked_by    = $4::text,
    lease_token  = gen_random_uuid(),
    locked_until = now() + $5::bigint * interval '1 millisecond',
    started_at   = now(),
    updated_at   = now()
FROM next
WHERE j.id = next.id
RETURNING `

func (w *Worker) claim(ctx context.Context, queues, kinds string, limit int) ([]*Job, error) {
	rows, err := w.db.QueryContext(ctx, claimSQL+qualifiedJobColumns+", j.lease_token::text",
		queues, kinds, int64(limit), w.cfg.ID, w.cfg.LeaseDuration.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []*Job
	for rows.Next() {
		var token string
		j, err := scanJob(scanFunc(func(dest ...any) error { return rows.Scan(append(dest, &token)...) }))
		if err != nil {
			return nil, err
		}
		j.leaseToken = token
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

type scanFunc func(dest ...any) error

func (f scanFunc) Scan(dest ...any) error { return f(dest...) }

func (w *Worker) execute(runCtx, jobCtx context.Context, job *Job) {
	log := w.log.With("job_id", job.ID, "kind", job.Kind, "attempt", job.Attempts)
	handler := w.handlers[job.Kind]

	ctx, cancel := context.WithCancelCause(jobCtx)
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, job.Timeout(), errTimeout)
	defer cancelTimeout()

	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(ctx, job, cancel, log)
	}()

	start := time.Now()
	result, err := safeCall(ctx, handler, job)
	elapsed := time.Since(start)
	cause := context.Cause(ctx)
	cancel(errHandlerReturned)
	<-hbDone

	// DB writes after the handler must not be cut short by shutdown.
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(jobCtx), 10*time.Second)
	defer fcancel()

	var snooze *snoozeError
	var outcome string
	var ferr error
	switch {
	case errors.Is(cause, errLeaseLost):
		outcome = "lost"
		log.Warn("lease lost; abandoning result")
	case err == nil:
		outcome, ferr = "succeeded", w.complete(fctx, job, result)
	case jobCtx.Err() != nil && runCtx.Err() != nil:
		// Forced shutdown: hand the job back without burning an attempt.
		outcome, ferr = "released", w.release(fctx, job, 0)
	case errors.As(err, &snooze):
		outcome, ferr = "snoozed", w.release(fctx, job, snooze.d)
	default:
		if errors.Is(cause, errTimeout) {
			err = fmt.Errorf("timed out after %s: %w", job.Timeout(), err)
		}
		outcome, ferr = w.fail(fctx, job, err)
		log.Warn("job attempt failed", "err", err, "outcome", outcome)
	}
	if ferr != nil {
		// The lease will expire and the reaper will recover the job.
		log.Error("could not record job outcome", "outcome", outcome, "err", ferr)
		return
	}
	w.cfg.Metrics.observe(job, outcome, elapsed)
	log.Debug("job finished", "outcome", outcome, "duration", elapsed)
}

func safeCall(ctx context.Context, h Handler, job *Job) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			if len(stack) > 2000 {
				stack = stack[:2000]
			}
			err = fmt.Errorf("panic: %v\n%s", r, stack)
		}
	}()
	return h(ctx, job)
}

func (w *Worker) heartbeat(ctx context.Context, job *Job, cancel context.CancelCauseFunc, log *slog.Logger) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		res, err := w.db.ExecContext(ctx, `UPDATE conveyor_jobs
			SET locked_until = now() + $3::bigint * interval '1 millisecond', updated_at = now()
			WHERE id = $1::bigint AND lease_token = $2::uuid AND state = 'running'`,
			job.ID, job.leaseToken, w.cfg.LeaseDuration.Milliseconds())
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("heartbeat failed", "err", err)
			}
			continue
		}
		if n, _ := res.RowsAffected(); n == 0 {
			cancel(errLeaseLost)
			return
		}
	}
}

func (w *Worker) complete(ctx context.Context, job *Job, result any) error {
	var resultJSON any
	if result != nil {
		b, err := json.Marshal(result)
		if err != nil {
			_, ferr := w.fail(ctx, job, Permanent(fmt.Errorf("marshal result: %w", err)))
			return ferr
		}
		resultJSON = string(b)
	}
	_, err := w.db.ExecContext(ctx, `UPDATE conveyor_jobs
		SET state = 'succeeded', result = $3::jsonb, last_error = NULL, finished_at = now(), updated_at = now(),
		    locked_by = NULL, locked_until = NULL, lease_token = NULL
		WHERE id = $1::bigint AND lease_token = $2::uuid AND state = 'running'`,
		job.ID, job.leaseToken, resultJSON)
	return err
}

// fail records the error and either schedules a retry or marks the job dead.
// The last 20 errors are kept on the row for debugging.
func (w *Worker) fail(ctx context.Context, job *Job, jobErr error) (string, error) {
	msg := jobErr.Error()
	if len(msg) > 4000 {
		msg = msg[:4000]
	}
	var state string
	err := w.db.QueryRowContext(ctx, `
		UPDATE conveyor_jobs SET
		    state       = CASE WHEN $4::bool OR attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
		    run_at      = CASE WHEN $4::bool OR attempts >= max_attempts THEN run_at
		                       ELSE now() + $5::bigint * interval '1 millisecond' END,
		    finished_at = CASE WHEN $4::bool OR attempts >= max_attempts THEN now() END,
		    last_error  = $3::text,
		    errors      = jsonb_path_query_array(
		                      errors || jsonb_build_array(jsonb_build_object(
		                          'attempt', attempts, 'error', $3::text, 'at', now(), 'worker', locked_by)),
		                      '$[last-19 to last]'),
		    locked_by = NULL, locked_until = NULL, lease_token = NULL, updated_at = now()
		WHERE id = $1::bigint AND lease_token = $2::uuid AND state = 'running'
		RETURNING state`,
		job.ID, job.leaseToken, msg, IsPermanent(jobErr), w.cfg.Backoff(job.Attempts).Milliseconds()).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "lost", nil
	}
	if err != nil {
		return "", err
	}
	if state == string(StateDead) {
		return "dead", nil
	}
	return "retried", nil
}

// release returns a running job to pending without counting the attempt.
func (w *Worker) release(ctx context.Context, job *Job, delay time.Duration) error {
	_, err := w.db.ExecContext(ctx, `UPDATE conveyor_jobs
		SET state = 'pending', attempts = attempts - 1, run_at = now() + $3::bigint * interval '1 millisecond',
		    locked_by = NULL, locked_until = NULL, lease_token = NULL, updated_at = now()
		WHERE id = $1::bigint AND lease_token = $2::uuid AND state = 'running'`,
		job.ID, job.leaseToken, delay.Milliseconds())
	return err
}
