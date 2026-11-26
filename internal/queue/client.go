package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"
)

// DBTX is satisfied by *sql.DB, *sql.Tx and *sql.Conn.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Client is the producer/admin side of the queue.
type Client struct{ db *sql.DB }

func NewClient(db *sql.DB) *Client { return &Client{db: db} }

// EnqueueParams describes a new job. Zero values get sensible defaults.
type EnqueueParams struct {
	Queue       string        `json:"queue"`
	Kind        string        `json:"kind"`
	Payload     any           `json:"payload"`
	Priority    int           `json:"priority"`     // -100..100, higher runs first
	MaxAttempts int           `json:"max_attempts"` // default 5
	RunAt       *time.Time    `json:"run_at"`       // absolute schedule
	Delay       time.Duration `json:"-"`            // relative schedule (ignored if RunAt set)
	Timeout     time.Duration `json:"-"`            // per attempt, default 60s
	UniqueKey   string        `json:"unique_key"`   // dedupe while an equal key is pending/running
}

var namePattern = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,100}$`)

// ValidationError is returned for bad input; the API maps it to HTTP 400.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

func (p *EnqueueParams) normalize() (payload string, err error) {
	if p.Queue == "" {
		p.Queue = "default"
	}
	if !namePattern.MatchString(p.Queue) {
		return "", invalid("queue must match %s", namePattern)
	}
	if !namePattern.MatchString(p.Kind) {
		return "", invalid("kind is required and must match %s", namePattern)
	}
	if p.Priority < -100 || p.Priority > 100 {
		return "", invalid("priority must be between -100 and 100")
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 5
	}
	if p.MaxAttempts < 1 || p.MaxAttempts > 100 {
		return "", invalid("max_attempts must be between 1 and 100")
	}
	if p.Timeout == 0 {
		p.Timeout = time.Minute
	}
	if p.Timeout < time.Millisecond || p.Timeout > 24*time.Hour {
		return "", invalid("timeout must be between 1ms and 24h")
	}
	if p.Delay < 0 {
		return "", invalid("delay must not be negative")
	}
	if len(p.UniqueKey) > 255 {
		return "", invalid("unique_key must be at most 255 characters")
	}
	switch v := p.Payload.(type) {
	case nil:
		return "{}", nil
	case json.RawMessage:
		if !json.Valid(v) {
			return "", invalid("payload is not valid JSON")
		}
		return string(v), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", invalid("payload: %v", err)
		}
		return string(b), nil
	}
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func optTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

const insertJobSQL = `
INSERT INTO conveyor_jobs (queue, kind, payload, priority, max_attempts, run_at, timeout_ms, unique_key)
VALUES ($1::text, $2::text, $3::jsonb, $4::smallint, $5::int,
        COALESCE($6::timestamptz, now() + $7::bigint * interval '1 millisecond'),
        $8::int, $9::text)
ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND state IN ('pending', 'running')
DO NOTHING
RETURNING ` + jobColumns

// Enqueue inserts a job. If UniqueKey matches an active job, that job is
// returned with created=false and nothing is inserted.
func (c *Client) Enqueue(ctx context.Context, p EnqueueParams) (job *Job, created bool, err error) {
	return EnqueueTx(ctx, c.db, p)
}

// EnqueueTx enqueues using any DBTX, typically a *sql.Tx holding your own
// writes: the job becomes visible only if that transaction commits.
func EnqueueTx(ctx context.Context, db DBTX, p EnqueueParams) (*Job, bool, error) {
	payload, err := p.normalize()
	if err != nil {
		return nil, false, err
	}
	// Loop because the conflicting job may finish between our INSERT and
	// SELECT, in which case inserting again will succeed.
	for range 3 {
		job, err := scanJob(db.QueryRowContext(ctx, insertJobSQL,
			p.Queue, p.Kind, payload, int64(p.Priority), int64(p.MaxAttempts),
			optTime(p.RunAt), p.Delay.Milliseconds(), p.Timeout.Milliseconds(), nullable(p.UniqueKey)))
		if err == nil {
			return job, true, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, false, err
		}
		job, err = scanJob(db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM conveyor_jobs
			WHERE unique_key = $1::text AND state IN ('pending', 'running')`, p.UniqueKey))
		if err == nil {
			return job, false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, false, err
		}
	}
	return nil, false, errors.New("enqueue: unique_key contention, try again")
}

// EnqueueMany inserts jobs in a single statement using jsonb_to_recordset.
// Jobs whose unique_key collides with an active job are skipped; the
// returned slice holds IDs of the rows actually inserted.
func (c *Client) EnqueueMany(ctx context.Context, ps []EnqueueParams) ([]int64, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	type rec struct {
		Queue       string          `json:"queue"`
		Kind        string          `json:"kind"`
		Payload     json.RawMessage `json:"payload"`
		Priority    int             `json:"priority"`
		MaxAttempts int             `json:"max_attempts"`
		RunAt       time.Time       `json:"run_at"`
		TimeoutMS   int64           `json:"timeout_ms"`
		UniqueKey   *string         `json:"unique_key"`
	}
	now := time.Now()
	recs := make([]rec, len(ps))
	for i := range ps {
		payload, err := ps[i].normalize()
		if err != nil {
			return nil, fmt.Errorf("job %d: %w", i, err)
		}
		runAt := now.Add(ps[i].Delay)
		if ps[i].RunAt != nil {
			runAt = *ps[i].RunAt
		}
		r := rec{Queue: ps[i].Queue, Kind: ps[i].Kind, Payload: json.RawMessage(payload), Priority: ps[i].Priority,
			MaxAttempts: ps[i].MaxAttempts, RunAt: runAt, TimeoutMS: ps[i].Timeout.Milliseconds()}
		if ps[i].UniqueKey != "" {
			r.UniqueKey = &ps[i].UniqueKey
		}
		recs[i] = r
	}
	body, err := json.Marshal(recs)
	if err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, `
		INSERT INTO conveyor_jobs (queue, kind, payload, priority, max_attempts, run_at, timeout_ms, unique_key)
		SELECT queue, kind, payload, priority, max_attempts, run_at, timeout_ms, unique_key
		FROM jsonb_to_recordset($1::jsonb) AS r(queue text, kind text, payload jsonb, priority smallint,
		     max_attempts int, run_at timestamptz, timeout_ms int, unique_key text)
		ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND state IN ('pending', 'running')
		DO NOTHING
		RETURNING id`, string(body))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int64, 0, len(ps))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (c *Client) Get(ctx context.Context, id int64) (*Job, error) {
	j, err := scanJob(c.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM conveyor_jobs WHERE id = $1::bigint`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

type ListParams struct {
	Queue  string
	State  State
	Kind   string
	Before int64 // keyset pagination: only ids < Before
	Limit  int
}

// EffectiveLimit is the page size List will actually use.
func (p ListParams) EffectiveLimit() int {
	if p.Limit <= 0 || p.Limit > 500 {
		return 50
	}
	return p.Limit
}

// List returns jobs newest first. Pass the last ID as Before for the next page.
func (c *Client) List(ctx context.Context, p ListParams) ([]*Job, error) {
	p.Limit = p.EffectiveLimit()
	if p.State != "" && !p.State.Valid() {
		return nil, invalid("unknown state %q", p.State)
	}
	rows, err := c.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM conveyor_jobs
		WHERE ($1::text = '' OR queue = $1::text)
		  AND ($2::text = '' OR state = $2::text)
		  AND ($3::text = '' OR kind = $3::text)
		  AND ($4::bigint = 0 OR id < $4::bigint)
		ORDER BY id DESC
		LIMIT $5::int`, p.Queue, string(p.State), p.Kind, p.Before, int64(p.Limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []*Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Cancel stops a pending or running job. A running job's worker notices on
// its next heartbeat and its context is cancelled.
func (c *Client) Cancel(ctx context.Context, id int64) (*Job, error) {
	return c.transition(ctx, id, `UPDATE conveyor_jobs
		SET state = 'cancelled', finished_at = now(), updated_at = now(), locked_by = NULL, locked_until = NULL, lease_token = NULL
		WHERE id = $1::bigint AND state IN ('pending', 'running')
		RETURNING `+jobColumns)
}

// Retry puts a dead or cancelled job back in the queue with a fresh attempt budget.
func (c *Client) Retry(ctx context.Context, id int64) (*Job, error) {
	return c.transition(ctx, id, `UPDATE conveyor_jobs
		SET state = 'pending', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
		WHERE id = $1::bigint AND state IN ('dead', 'cancelled')
		RETURNING `+jobColumns)
}

func (c *Client) transition(ctx context.Context, id int64, query string) (*Job, error) {
	j, err := scanJob(c.db.QueryRowContext(ctx, query, id))
	if errors.Is(err, sql.ErrNoRows) {
		if _, gerr := c.Get(ctx, id); gerr != nil {
			return nil, gerr
		}
		return nil, ErrInvalidState
	}
	return j, err
}

// RetryDead re-queues every dead job in a queue and returns how many.
func (c *Client) RetryDead(ctx context.Context, queue string) (int64, error) {
	res, err := c.db.ExecContext(ctx, `UPDATE conveyor_jobs
		SET state = 'pending', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
		WHERE queue = $1::text AND state = 'dead'`, queue)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (c *Client) Pause(ctx context.Context, queue string) error {
	if !namePattern.MatchString(queue) {
		return invalid("invalid queue name")
	}
	_, err := c.db.ExecContext(ctx, `INSERT INTO conveyor_paused_queues (queue) VALUES ($1::text)
		ON CONFLICT DO NOTHING`, queue)
	return err
}

func (c *Client) Resume(ctx context.Context, queue string) error {
	_, err := c.db.ExecContext(ctx, `DELETE FROM conveyor_paused_queues WHERE queue = $1::text`, queue)
	if err == nil {
		// Wake workers: jobs may have been waiting the whole time.
		_, err = c.db.ExecContext(ctx, `SELECT pg_notify('conveyor_jobs', $1::text)`, queue)
	}
	return err
}

type QueueStats struct {
	Queue           string  `json:"queue"`
	Ready           int64   `json:"ready"`
	Scheduled       int64   `json:"scheduled"`
	Running         int64   `json:"running"`
	Succeeded       int64   `json:"succeeded"`
	Dead            int64   `json:"dead"`
	Cancelled       int64   `json:"cancelled"`
	OldestReadyAgeS float64 `json:"oldest_ready_age_seconds"`
	Paused          bool    `json:"paused"`
}

func (c *Client) Stats(ctx context.Context) ([]QueueStats, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT queue,
		       count(*) FILTER (WHERE state = 'pending' AND run_at <= now()),
		       count(*) FILTER (WHERE state = 'pending' AND run_at >  now()),
		       count(*) FILTER (WHERE state = 'running'),
		       count(*) FILTER (WHERE state = 'succeeded'),
		       count(*) FILTER (WHERE state = 'dead'),
		       count(*) FILTER (WHERE state = 'cancelled'),
		       COALESCE(EXTRACT(EPOCH FROM now() - min(run_at)
		                FILTER (WHERE state = 'pending' AND run_at <= now())), 0)::float8
		FROM conveyor_jobs
		GROUP BY queue`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byQueue := map[string]*QueueStats{}
	for rows.Next() {
		var s QueueStats
		if err := rows.Scan(&s.Queue, &s.Ready, &s.Scheduled, &s.Running, &s.Succeeded, &s.Dead,
			&s.Cancelled, &s.OldestReadyAgeS); err != nil {
			return nil, err
		}
		byQueue[s.Queue] = &s
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	paused, err := c.db.QueryContext(ctx, `SELECT queue FROM conveyor_paused_queues`)
	if err != nil {
		return nil, err
	}
	defer paused.Close()
	for paused.Next() {
		var q string
		if err := paused.Scan(&q); err != nil {
			return nil, err
		}
		if byQueue[q] == nil {
			byQueue[q] = &QueueStats{Queue: q}
		}
		byQueue[q].Paused = true
	}
	out := make([]QueueStats, 0, len(byQueue))
	for _, s := range byQueue {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queue < out[j].Queue })
	return out, paused.Err()
}
