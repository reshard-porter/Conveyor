package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reshard-porter/conveyor/internal/migrate"
	"github.com/reshard-porter/conveyor/internal/pgwire"
)

var testDB *sql.DB

func TestMain(m *testing.M) {
	dsn := os.Getenv("CONVEYOR_TEST_DATABASE_URL")
	if dsn == "" {
		fmt.Println("CONVEYOR_TEST_DATABASE_URL not set; skipping integration tests")
		os.Exit(0)
	}
	db, err := sql.Open(pgwire.DriverName, dsn)
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(60)
	ctx := context.Background()
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS conveyor_jobs, conveyor_paused_queues, conveyor_schema_migrations CASCADE`,
		`DROP FUNCTION IF EXISTS conveyor_notify CASCADE`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			panic(err)
		}
	}
	if _, err := migrate.Up(ctx, db, quietLog()); err != nil {
		panic(err)
	}
	// Running migrations twice must be a no-op.
	if n, err := migrate.Up(ctx, db, quietLog()); err != nil || n != 0 {
		panic(fmt.Sprintf("second migrate: n=%d err=%v", n, err))
	}
	testDB = db
	code := m.Run()
	db.Close()
	os.Exit(code)
}

func quietLog() *slog.Logger {
	if os.Getenv("TEST_VERBOSE_LOGS") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func reset(t *testing.T) *Client {
	t.Helper()
	if _, err := testDB.Exec(`TRUNCATE conveyor_jobs, conveyor_paused_queues RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
	return NewClient(testDB)
}

func testWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Queues:            []string{"default"},
		Concurrency:       4,
		PollInterval:      20 * time.Millisecond,
		LeaseDuration:     2 * time.Second,
		HeartbeatInterval: 50 * time.Millisecond,
		ShutdownTimeout:   2 * time.Second,
		Backoff:           func(int) time.Duration { return time.Millisecond },
		Logger:            quietLog(),
	}
}

// startWorker runs w in the background and stops it when the test ends.
// The returned func stops it early and waits for Run to return.
func startWorker(t *testing.T, w *Worker) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		if err := w.Run(ctx); err != nil {
			t.Errorf("worker: %v", err)
		}
		close(done)
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitState(t *testing.T, c *Client, id int64, want State) *Job {
	t.Helper()
	var job *Job
	waitFor(t, fmt.Sprintf("job %d to be %s", id, want), func() bool {
		var err error
		job, err = c.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		return job.State == want
	})
	return job
}

func mustEnqueue(t *testing.T, c *Client, p EnqueueParams) *Job {
	t.Helper()
	j, created, err := c.Enqueue(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatalf("expected job to be created")
	}
	return j
}

func errorCount(t *testing.T, j *Job) int {
	var errs []map[string]any
	if err := json.Unmarshal(j.Errors, &errs); err != nil {
		t.Fatal(err)
	}
	return len(errs)
}

// ---------------------------------------------------------------------------

func TestBackoffBounds(t *testing.T) {
	b := ExponentialBackoff(time.Second, 10*time.Second)
	for attempt, max := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 10: 10 * time.Second} {
		for range 100 {
			d := b(attempt)
			if d < max/2 || d > max {
				t.Fatalf("attempt %d: %s not in [%s, %s]", attempt, d, max/2, max)
			}
		}
	}
}

func TestEnqueueValidation(t *testing.T) {
	c := reset(t)
	for _, p := range []EnqueueParams{
		{Kind: ""},
		{Kind: "has space"},
		{Kind: "ok", Priority: 101},
		{Kind: "ok", MaxAttempts: 1000},
		{Kind: "ok", Payload: json.RawMessage(`{bad`)},
		{Kind: "ok", Delay: -time.Second},
	} {
		_, _, err := c.Enqueue(context.Background(), p)
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Errorf("%+v: expected ValidationError, got %v", p, err)
		}
	}
}

func TestProcessesJobAndStoresResult(t *testing.T) {
	c := reset(t)
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("greet", func(_ context.Context, j *Job) (any, error) {
		var p struct{ Name string }
		_ = j.DecodePayload(&p)
		return map[string]string{"greeting": "hello " + p.Name}, nil
	})
	startWorker(t, w)

	job := mustEnqueue(t, c, EnqueueParams{Kind: "greet", Payload: map[string]string{"name": "ada"}})
	done := waitState(t, c, job.ID, StateSucceeded)
	if string(done.Result) != `{"greeting": "hello ada"}` {
		t.Fatalf("result = %s", done.Result)
	}
	if done.Attempts != 1 || done.LockedBy != nil || done.FinishedAt == nil || done.LastError != nil {
		t.Fatalf("unexpected final job: %+v", done)
	}
}

func TestRetriesThenSucceeds(t *testing.T) {
	c := reset(t)
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("flaky", func(_ context.Context, j *Job) (any, error) {
		if j.Attempts < 3 {
			return nil, fmt.Errorf("boom %d", j.Attempts)
		}
		return nil, nil
	})
	startWorker(t, w)

	job := mustEnqueue(t, c, EnqueueParams{Kind: "flaky"})
	done := waitState(t, c, job.ID, StateSucceeded)
	if done.Attempts != 3 || errorCount(t, done) != 2 {
		t.Fatalf("attempts=%d errors=%s", done.Attempts, done.Errors)
	}
}

func TestDeadAfterMaxAttemptsAndManualRetry(t *testing.T) {
	c := reset(t)
	var healed atomic.Bool
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("broken", func(context.Context, *Job) (any, error) {
		if healed.Load() {
			return "fixed", nil
		}
		return nil, errors.New("still broken")
	})
	startWorker(t, w)

	job := mustEnqueue(t, c, EnqueueParams{Kind: "broken", MaxAttempts: 3})
	dead := waitState(t, c, job.ID, StateDead)
	if dead.Attempts != 3 || dead.LastError == nil || *dead.LastError != "still broken" || errorCount(t, dead) != 3 {
		t.Fatalf("unexpected dead job: %+v errors=%s", dead, dead.Errors)
	}

	if _, err := c.Retry(context.Background(), job.ID+999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry missing job: %v", err)
	}
	healed.Store(true)
	if _, err := c.Retry(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	done := waitState(t, c, job.ID, StateSucceeded)
	if done.Attempts != 1 {
		t.Fatalf("retry should reset attempts, got %d", done.Attempts)
	}
	if _, err := c.Retry(context.Background(), job.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("retrying a succeeded job should be ErrInvalidState, got %v", err)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	c := reset(t)
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("bad-input", func(context.Context, *Job) (any, error) {
		return nil, Permanent(errors.New("invalid email"))
	})
	startWorker(t, w)
	job := mustEnqueue(t, c, EnqueueParams{Kind: "bad-input", MaxAttempts: 10})
	dead := waitState(t, c, job.ID, StateDead)
	if dead.Attempts != 1 {
		t.Fatalf("permanent error should not retry; attempts=%d", dead.Attempts)
	}
}

func TestSnoozeDoesNotConsumeAttempt(t *testing.T) {
	c := reset(t)
	var calls atomic.Int32
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("rate-limited", func(context.Context, *Job) (any, error) {
		if calls.Add(1) == 1 {
			return nil, Snooze(100 * time.Millisecond)
		}
		return nil, nil
	})
	startWorker(t, w)
	job := mustEnqueue(t, c, EnqueueParams{Kind: "rate-limited", MaxAttempts: 1})
	done := waitState(t, c, job.ID, StateSucceeded)
	if done.Attempts != 1 || calls.Load() != 2 {
		t.Fatalf("attempts=%d calls=%d", done.Attempts, calls.Load())
	}
}

func TestPanicIsRecovered(t *testing.T) {
	c := reset(t)
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("panics", func(context.Context, *Job) (any, error) { panic("kaboom") })
	startWorker(t, w)
	job := mustEnqueue(t, c, EnqueueParams{Kind: "panics", MaxAttempts: 1})
	dead := waitState(t, c, job.ID, StateDead)
	if !strings.HasPrefix(*dead.LastError, "panic: kaboom") {
		t.Fatalf("last_error = %q", *dead.LastError)
	}
}

func TestTimeout(t *testing.T) {
	c := reset(t)
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("slow", func(ctx context.Context, _ *Job) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	startWorker(t, w)
	job := mustEnqueue(t, c, EnqueueParams{Kind: "slow", MaxAttempts: 1, Timeout: 100 * time.Millisecond})
	dead := waitState(t, c, job.ID, StateDead)
	if !strings.Contains(*dead.LastError, "timed out after 100ms") {
		t.Fatalf("last_error = %q", *dead.LastError)
	}
}

func TestUniqueKeyDeduplicates(t *testing.T) {
	c := reset(t)
	ctx := context.Background()
	a, created, err := c.Enqueue(ctx, EnqueueParams{Kind: "sync", UniqueKey: "user:42"})
	if err != nil || !created {
		t.Fatal(err, created)
	}
	b, created, err := c.Enqueue(ctx, EnqueueParams{Kind: "sync", UniqueKey: "user:42"})
	if err != nil || created || b.ID != a.ID {
		t.Fatalf("expected dedupe to job %d, got %+v created=%v err=%v", a.ID, b, created, err)
	}
	ids, err := c.EnqueueMany(ctx, []EnqueueParams{
		{Kind: "sync", UniqueKey: "user:42"}, {Kind: "sync", UniqueKey: "user:43"}, {Kind: "sync", UniqueKey: "user:43"},
	})
	if err != nil || len(ids) != 1 {
		t.Fatalf("batch dedupe: ids=%v err=%v", ids, err)
	}

	// Once the job finishes, the key is free again.
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("sync", func(context.Context, *Job) (any, error) { return nil, nil })
	startWorker(t, w)
	waitState(t, c, a.ID, StateSucceeded)
	again, created, err := c.Enqueue(ctx, EnqueueParams{Kind: "sync", UniqueKey: "user:42"})
	if err != nil || !created || again.ID == a.ID {
		t.Fatalf("expected a new job after completion: %+v created=%v err=%v", again, created, err)
	}
}

func TestEnqueueTxIsTransactional(t *testing.T) {
	c := reset(t)
	ctx := context.Background()

	tx, _ := testDB.BeginTx(ctx, nil)
	rolled, _, err := EnqueueTx(ctx, tx, EnqueueParams{Kind: "welcome-email"})
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
	if _, err := c.Get(ctx, rolled.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back job must not exist, got %v", err)
	}

	tx, _ = testDB.BeginTx(ctx, nil)
	committed, _, err := EnqueueTx(ctx, tx, EnqueueParams{Kind: "welcome-email"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, committed.ID); err != nil {
		t.Fatalf("committed job missing: %v", err)
	}
}

func TestDelayedJobWaitsForRunAt(t *testing.T) {
	c := reset(t)
	var ranAt atomic.Int64
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("later", func(context.Context, *Job) (any, error) { ranAt.Store(time.Now().UnixNano()); return nil, nil })
	startWorker(t, w)
	start := time.Now()
	job := mustEnqueue(t, c, EnqueueParams{Kind: "later", Delay: 300 * time.Millisecond})
	waitState(t, c, job.ID, StateSucceeded)
	if waited := time.Duration(ranAt.Load() - start.UnixNano()); waited < 300*time.Millisecond {
		t.Fatalf("ran after %s, before its delay", waited)
	}
}

func TestPriorityOrdering(t *testing.T) {
	c := reset(t)
	for i, prio := range []int{0, 10, -5, 50, 10} {
		mustEnqueue(t, c, EnqueueParams{Kind: "ordered", Priority: prio, Payload: map[string]int{"n": i}})
	}
	var mu sync.Mutex
	var order []int
	cfg := testWorkerConfig()
	cfg.Concurrency = 1
	w := NewWorker(testDB, cfg)
	w.Handle("ordered", func(_ context.Context, j *Job) (any, error) {
		mu.Lock()
		order = append(order, j.Priority)
		mu.Unlock()
		return nil, nil
	})
	startWorker(t, w)
	waitFor(t, "all jobs", func() bool { mu.Lock(); defer mu.Unlock(); return len(order) == 5 })
	if fmt.Sprint(order) != "[50 10 10 0 -5]" {
		t.Fatalf("order = %v", order)
	}
}

// The core safety property: many workers racing on one table never run a
// job twice and never drop one.
func TestExactlyOnceClaimingUnderConcurrency(t *testing.T) {
	c := reset(t)
	const jobs, workers = 600, 4
	params := make([]EnqueueParams, jobs)
	for i := range params {
		params[i] = EnqueueParams{Kind: "count"}
	}
	if _, err := c.EnqueueMany(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	var runs sync.Map
	var total atomic.Int64
	for i := range workers {
		cfg := testWorkerConfig()
		cfg.ID = fmt.Sprintf("w%d", i)
		cfg.Concurrency = 8
		w := NewWorker(testDB, cfg)
		w.Handle("count", func(_ context.Context, j *Job) (any, error) {
			n, _ := runs.LoadOrStore(j.ID, new(atomic.Int32))
			n.(*atomic.Int32).Add(1)
			total.Add(1)
			return nil, nil
		})
		startWorker(t, w)
	}
	waitFor(t, "all jobs to succeed", func() bool {
		var n int
		_ = testDB.QueryRow(`SELECT count(*) FROM conveyor_jobs WHERE state = 'succeeded'`).Scan(&n)
		return n == jobs
	})
	if total.Load() != jobs {
		t.Fatalf("handler ran %d times for %d jobs", total.Load(), jobs)
	}
	runs.Range(func(k, v any) bool {
		if v.(*atomic.Int32).Load() != 1 {
			t.Errorf("job %v ran %d times", k, v.(*atomic.Int32).Load())
		}
		return true
	})
}

func TestCancelRunningJobInterruptsHandler(t *testing.T) {
	c := reset(t)
	started := make(chan struct{})
	causeCh := make(chan error, 1)
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("long", func(ctx context.Context, _ *Job) (any, error) {
		close(started)
		<-ctx.Done()
		causeCh <- context.Cause(ctx)
		return nil, ctx.Err()
	})
	startWorker(t, w)
	job := mustEnqueue(t, c, EnqueueParams{Kind: "long"})
	<-started
	if _, err := c.Cancel(context.Background(), job.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case cause := <-causeCh:
		if !errors.Is(cause, errLeaseLost) {
			t.Fatalf("cause = %v", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not interrupted")
	}
	time.Sleep(100 * time.Millisecond)
	if j, _ := c.Get(context.Background(), job.ID); j.State != StateCancelled {
		t.Fatalf("state = %s; the abandoned worker must not overwrite cancellation", j.State)
	}
}

func TestGracefulShutdownReleasesUnfinishedJobs(t *testing.T) {
	c := reset(t)
	started := make(chan struct{})
	cfg := testWorkerConfig()
	cfg.ShutdownTimeout = 100 * time.Millisecond
	w := NewWorker(testDB, cfg)
	w.Handle("stubborn", func(ctx context.Context, _ *Job) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	stop := startWorker(t, w)
	job := mustEnqueue(t, c, EnqueueParams{Kind: "stubborn", MaxAttempts: 1})
	<-started
	stop()
	j, _ := c.Get(context.Background(), job.ID)
	if j.State != StatePending || j.Attempts != 0 || j.LockedBy != nil {
		t.Fatalf("job should be released untouched, got state=%s attempts=%d", j.State, j.Attempts)
	}
}

func TestGracefulShutdownLetsJobsFinish(t *testing.T) {
	c := reset(t)
	started := make(chan struct{})
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("quick", func(context.Context, *Job) (any, error) {
		close(started)
		time.Sleep(200 * time.Millisecond)
		return "done", nil
	})
	stop := startWorker(t, w)
	job := mustEnqueue(t, c, EnqueueParams{Kind: "quick"})
	<-started
	stop()
	if j, _ := c.Get(context.Background(), job.ID); j.State != StateSucceeded {
		t.Fatalf("in-flight job should finish during grace period, got %s", j.State)
	}
}

func TestReaperRecoversCrashedWorkersJobs(t *testing.T) {
	c := reset(t)
	ctx := context.Background()
	retryable := mustEnqueue(t, c, EnqueueParams{Kind: "x", MaxAttempts: 3})
	exhausted := mustEnqueue(t, c, EnqueueParams{Kind: "x", MaxAttempts: 1})
	healthy := mustEnqueue(t, c, EnqueueParams{Kind: "x"})
	// Simulate workers that claimed these jobs and then died.
	_, err := testDB.Exec(`UPDATE conveyor_jobs SET state = 'running', attempts = 1, locked_by = 'dead-worker',
		lease_token = gen_random_uuid(),
		locked_until = CASE WHEN id = $1::bigint THEN now() + interval '1 hour' ELSE now() - interval '1 second' END`,
		healthy.ID)
	if err != nil {
		t.Fatal(err)
	}
	m := NewMaintainer(testDB, MaintenanceConfig{Logger: quietLog()})
	n, err := m.Reap(ctx)
	if err != nil || n != 2 {
		t.Fatalf("reaped %d, err %v", n, err)
	}
	for id, want := range map[int64]State{retryable.ID: StatePending, exhausted.ID: StateDead, healthy.ID: StateRunning} {
		j, _ := c.Get(ctx, id)
		if j.State != want {
			t.Errorf("job %d: state %s, want %s", id, j.State, want)
		}
	}
}

// A worker that stalls past its lease must not clobber the job after
// another worker has taken it over: every write is fenced on lease_token.
func TestFencingPreventsStaleWorkerWrites(t *testing.T) {
	c := reset(t)
	ctx := context.Background()
	job := mustEnqueue(t, c, EnqueueParams{Kind: "fenced"})
	queues, kinds := pgwire.TextArray([]string{"default"}), pgwire.TextArray([]string{"fenced"})

	cfgA := testWorkerConfig()
	cfgA.ID = "worker-a"
	a := NewWorker(testDB, cfgA)
	claimedA, err := a.claim(ctx, queues, kinds, 1)
	if err != nil || len(claimedA) != 1 {
		t.Fatal(err, len(claimedA))
	}

	// A stalls; its lease expires and the reaper hands the job to B.
	_, _ = testDB.Exec(`UPDATE conveyor_jobs SET locked_until = now() - interval '1s' WHERE id = $1::bigint`, job.ID)
	if _, err := NewMaintainer(testDB, MaintenanceConfig{}).Reap(ctx); err != nil {
		t.Fatal(err)
	}
	cfgB := testWorkerConfig()
	cfgB.ID = "worker-b"
	b := NewWorker(testDB, cfgB)
	claimedB, err := b.claim(ctx, queues, kinds, 1)
	if err != nil || len(claimedB) != 1 {
		t.Fatal(err, len(claimedB))
	}

	// A wakes up and tries every kind of write. None may land.
	if err := a.complete(ctx, claimedA[0], "stale result"); err != nil {
		t.Fatal(err)
	}
	if outcome, err := a.fail(ctx, claimedA[0], errors.New("stale failure")); err != nil || outcome != "lost" {
		t.Fatalf("stale fail: outcome=%s err=%v", outcome, err)
	}
	if err := a.release(ctx, claimedA[0], 0); err != nil {
		t.Fatal(err)
	}
	j, _ := c.Get(ctx, job.ID)
	if j.State != StateRunning || *j.LockedBy != "worker-b" || j.Result != nil {
		t.Fatalf("stale worker modified job: state=%s locked_by=%v result=%s", j.State, *j.LockedBy, j.Result)
	}

	if err := b.complete(ctx, claimedB[0], "fresh result"); err != nil {
		t.Fatal(err)
	}
	j, _ = c.Get(ctx, job.ID)
	if j.State != StateSucceeded || string(j.Result) != `"fresh result"` || j.Attempts != 2 {
		t.Fatalf("final job: state=%s result=%s attempts=%d", j.State, j.Result, j.Attempts)
	}
}

func TestPauseAndResumeQueue(t *testing.T) {
	c := reset(t)
	ctx := context.Background()
	if err := c.Pause(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	var ran atomic.Bool
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("p", func(context.Context, *Job) (any, error) { ran.Store(true); return nil, nil })
	startWorker(t, w)
	job := mustEnqueue(t, c, EnqueueParams{Kind: "p"})
	time.Sleep(200 * time.Millisecond)
	if ran.Load() {
		t.Fatal("paused queue was processed")
	}
	stats, _ := c.Stats(ctx)
	if len(stats) != 1 || !stats[0].Paused || stats[0].Ready != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := c.Resume(ctx, "default"); err != nil {
		t.Fatal(err)
	}
	waitState(t, c, job.ID, StateSucceeded)
}

func TestWorkerOnlyClaimsRegisteredKindsAndQueues(t *testing.T) {
	c := reset(t)
	other := mustEnqueue(t, c, EnqueueParams{Kind: "not-mine"})
	elsewhere := mustEnqueue(t, c, EnqueueParams{Kind: "mine", Queue: "other-queue"})
	mine := mustEnqueue(t, c, EnqueueParams{Kind: "mine"})
	w := NewWorker(testDB, testWorkerConfig())
	w.Handle("mine", func(context.Context, *Job) (any, error) { return nil, nil })
	startWorker(t, w)
	waitState(t, c, mine.ID, StateSucceeded)
	for _, id := range []int64{other.ID, elsewhere.ID} {
		if j, _ := c.Get(context.Background(), id); j.State != StatePending {
			t.Fatalf("job %d should be untouched, is %s", id, j.State)
		}
	}
}

func TestListPagination(t *testing.T) {
	c := reset(t)
	ctx := context.Background()
	for i := range 7 {
		q := "a"
		if i%2 == 1 {
			q = "b"
		}
		mustEnqueue(t, c, EnqueueParams{Kind: "k", Queue: q})
	}
	page1, _ := c.List(ctx, ListParams{Queue: "a", Limit: 2})
	page2, _ := c.List(ctx, ListParams{Queue: "a", Limit: 2, Before: page1[1].ID})
	ids := []int64{page1[0].ID, page1[1].ID, page2[0].ID, page2[1].ID}
	if fmt.Sprint(ids) != "[7 5 3 1]" {
		t.Fatalf("ids = %v", ids)
	}
	if _, err := c.List(ctx, ListParams{State: "bogus"}); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestCleanupHonoursRetention(t *testing.T) {
	c := reset(t)
	ctx := context.Background()
	for range 5 {
		mustEnqueue(t, c, EnqueueParams{Kind: "old"})
	}
	_, _ = testDB.Exec(`UPDATE conveyor_jobs SET state = 'succeeded', finished_at = now() - interval '2 days' WHERE id <= 3`)
	_, _ = testDB.Exec(`UPDATE conveyor_jobs SET state = 'dead', finished_at = now() - interval '2 days' WHERE id = 4`)
	m := NewMaintainer(testDB, MaintenanceConfig{BatchSize: 2, Logger: quietLog()})
	n, err := m.Cleanup(ctx)
	if err != nil || n != 3 {
		t.Fatalf("deleted %d, err %v (dead jobs have 7d retention)", n, err)
	}
	var left int
	_ = testDB.QueryRow(`SELECT count(*) FROM conveyor_jobs`).Scan(&left)
	if left != 2 {
		t.Fatalf("%d jobs left", left)
	}
}

func TestMaintenanceLeaderElection(t *testing.T) {
	reset(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m1 := NewMaintainer(testDB, MaintenanceConfig{Logger: quietLog()})
	m2 := NewMaintainer(testDB, MaintenanceConfig{Logger: quietLog()})
	leading := make(chan bool, 1)
	go func() { led, _ := m1.leadOnce(ctx); leading <- led }()
	waitFor(t, "m1 to hold the lock", func() bool {
		var held bool
		_ = testDB.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = $1::bigint)`,
			int64(leaderLockKey)).Scan(&held)
		return held
	})
	if led, err := m2.leadOnce(ctx); led || err != nil {
		t.Fatalf("second maintainer must not lead: led=%v err=%v", led, err)
	}
	cancel()
	if !<-leading {
		t.Fatal("m1 should have led")
	}
}

func TestNotifyWakesWorkerFasterThanPolling(t *testing.T) {
	c := reset(t)
	cfg := testWorkerConfig()
	cfg.PollInterval = time.Hour // only Notify can wake it
	w := NewWorker(testDB, cfg)
	w.Handle("ping", func(context.Context, *Job) (any, error) { return nil, nil })
	startWorker(t, w)
	time.Sleep(50 * time.Millisecond) // let the first (empty) poll happen
	job := mustEnqueue(t, c, EnqueueParams{Kind: "ping"})
	w.Notify("some-other-queue") // ignored
	time.Sleep(50 * time.Millisecond)
	if j, _ := c.Get(context.Background(), job.ID); j.State != StatePending {
		t.Fatal("worker should not wake for another queue")
	}
	w.Notify("default")
	waitState(t, c, job.ID, StateSucceeded)
}
