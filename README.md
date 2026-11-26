# Conveyor

A durable, distributed background-job queue built on PostgreSQL and Go.

Run as many API and worker processes as you like against one Postgres database. Jobs are
retried with exponential backoff, recovered automatically when a worker crashes, deduplicated
by key, and can be enqueued **inside your own database transactions**. Workers wake up within
milliseconds via `LISTEN/NOTIFY`, and everything is observable through Prometheus metrics.

It builds with **only the Go standard library**. That includes a small, from-scratch PostgreSQL
wire-protocol driver for `database/sql` (SCRAM auth, TLS, LISTEN/NOTIFY, context cancellation).

```
 producers ──HTTP / Go API──▶  conveyor_jobs (Postgres)  ◀──claim (SKIP LOCKED)── workers × N
                                   │  ▲                                          │
                     pg_notify ────┘  └──── heartbeat / complete / fail ─────────┘
                                   ▲
                    maintainer (advisory-lock leader): reap expired leases, retention cleanup
```

## Features

| | |
|---|---|
| **Safe concurrency** | `FOR UPDATE SKIP LOCKED` claiming. Any number of workers, zero double-processing (tested with 4 workers × 8 slots racing for 600 jobs). |
| **Leases + fencing** | Each claim gets a fresh `lease_token`, extended by heartbeats. Every write is fenced on that token, so a stalled worker can never overwrite the newer owner's result. |
| **Crash recovery** | A leader-elected maintainer returns jobs with expired leases to the queue (or marks them dead when out of attempts). |
| **Retries** | Exponential backoff with jitter. `Permanent(err)` skips retries; `Snooze(d)` reschedules without using an attempt (for rate limits). The last 20 errors are stored on the job. |
| **Transactional enqueue** | `EnqueueTx(ctx, tx, …)`: the job exists if and only if your business transaction commits (the transactional outbox pattern). |
| **Deduplication** | `unique_key` with a partial unique index. At most one *active* job per key; the key becomes free again once the job finishes. |
| **Scheduling & priority** | `run_at` / `delay`, priorities −100…100, per-attempt timeouts. |
| **Low latency** | Postgres trigger → `pg_notify` → workers poll immediately. Polling is only a fallback. |
| **Graceful shutdown** | Stops claiming, lets in-flight jobs finish, then cancels stragglers and *releases* them without burning an attempt. |
| **Operations** | Cancel (including running jobs), retry, retry-all-dead, pause/resume queues, per-queue stats, retention cleanup. |
| **Observability** | Structured `slog` logging, request IDs, Prometheus metrics: throughput by outcome, durations, queue depth, queue latency. |

## Quick start

```bash
docker compose up --build            # postgres + 1 API + 2 workers
```

```bash
# Enqueue a job that fails twice, then succeeds
curl -s -XPOST localhost:8080/v1/jobs -d '{"kind":"demo.flaky","payload":{"fail_times":2}}'

# Watch it retry with backoff
curl -s localhost:8080/v1/jobs/1

# Queue overview and metrics
curl -s localhost:8080/v1/queues
curl -s localhost:8080/metrics | grep conveyor_
```

Without Docker, with any Postgres 13+:

```bash
export DATABASE_URL='postgres://user:pass@localhost:5432/db?sslmode=disable'
go run ./cmd/conveyor serve                # API + worker + maintainer in one process
go run ./cmd/conveyor serve -role worker   # or split roles across processes
```

## Dashboard

Open **http://localhost:8080/** once the server is running. It's a single page embedded in the
binary, with no build step and no external assets. It shows:

- live totals by state (click a tile to filter) and a per-queue breakdown with pause/resume and retry-dead
- a job list that refreshes every 1.5s and highlights rows that change state
- a job detail panel with the payload, result and error timeline, plus cancel, retry and clone buttons
- an enqueue form with presets for every demo job and bulk enqueue (×10 / ×100 / ×1000)

If `CONVEYOR_API_KEY` is set, click **API key** in the header and paste it.

## HTTP API

All `/v1/*` routes require `Authorization: Bearer $CONVEYOR_API_KEY` when that variable is set.

| Method & path | Description |
|---|---|
| `POST /v1/jobs` | Enqueue. Body: `kind` (required), `queue`, `payload`, `priority`, `max_attempts`, `run_at` or `delay_seconds`, `timeout_seconds`, `unique_key`. Returns **201**, or **200** with `"created": false` when deduplicated. |
| `POST /v1/jobs/batch` | `{"jobs":[…]}` with up to 1000 jobs, inserted in one statement. |
| `GET /v1/jobs?queue=&state=&kind=&limit=&before=` | Newest first, keyset-paginated via `next_before`. |
| `GET /v1/jobs/{id}` | Full job including result, attempts and error history. |
| `POST /v1/jobs/{id}/cancel` | Pending or running → cancelled (**409** otherwise). A running handler's context is cancelled on its next heartbeat. |
| `POST /v1/jobs/{id}/retry` | Dead or cancelled → pending with a fresh attempt budget. |
| `GET /v1/queues` | Per-queue counts by state, oldest-ready age, paused flag. |
| `POST /v1/queues/{q}/pause` · `/resume` · `/retry-dead` | Queue operations. |
| `GET /healthz` · `GET /metrics` | Health check and Prometheus metrics (no auth). |
| `GET /` | The web dashboard. |

## Using it as a library

```go
db := sql.OpenDB(pgwire.NewConnector(cfg))

// Producer: enqueue atomically with your own writes.
tx, _ := db.BeginTx(ctx, nil)
tx.ExecContext(ctx, `INSERT INTO users (email) VALUES ($1::text)`, email)
queue.EnqueueTx(ctx, tx, queue.EnqueueParams{
    Kind: "email.send", Queue: "emails",
    Payload: map[string]string{"to": email},
    UniqueKey: "welcome:" + email,
})
tx.Commit()

// Consumer
w := queue.NewWorker(db, queue.WorkerConfig{Queues: []string{"emails"}, Concurrency: 20})
w.Handle("email.send", func(ctx context.Context, job *queue.Job) (any, error) {
    var p struct{ To string }
    if err := job.DecodePayload(&p); err != nil {
        return nil, queue.Permanent(err)      // retrying won't help
    }
    return sendEmail(ctx, p.To)               // returned value is stored as the result
})
w.Run(ctx) // blocks; cancel ctx for a graceful shutdown
```

## Design notes

**Why Postgres and not Redis or Kafka?** If your data already lives in Postgres, keeping the queue
there gives you transactional enqueue, one system to back up and monitor, and SQL for
inspecting jobs. `SKIP LOCKED` makes Postgres a fine queue at thousands of jobs per second. Past
that point, or for fan-out streaming, a dedicated broker becomes the better choice.

**Claiming.** The claim is a single statement: a CTE picks the top runnable rows from a
partial index (`WHERE state = 'pending'`) with `FOR UPDATE SKIP LOCKED`, and the outer `UPDATE`
marks them running and issues a lease. Workers never block each other, and the partial index
stays small no matter how much history accumulates.

**Leases and fencing.** A worker can stall (GC pause, network partition) and then wake up after
its lease expired and the job was handed to someone else. Every write a worker makes
(`complete`, `fail`, `release`, heartbeat) includes `WHERE lease_token = $token AND state =
'running'`. The stale worker's writes match zero rows, and a heartbeat that matches zero rows
cancels the handler's context. `TestFencingPreventsStaleWorkerWrites` walks through this
scenario step by step. Fencing on a per-claim token rather than `(worker_id, attempts)` matters
because an admin retry resets `attempts`, which could otherwise produce a collision.

**Delivery semantics.** Execution is at-least-once: a worker can finish the side effect and
crash before recording success. Handlers should therefore be idempotent. The webhook handler
shows the usual approach by sending an `Idempotency-Key` derived from the job ID.

**Leader election.** Several processes run the maintainer, but only the holder of a
session-level `pg_try_advisory_lock` does the work. If the leader dies, Postgres frees the lock
along with its session and another process takes over within `LeaderRetry`. The maintenance
queries are idempotent and use `SKIP LOCKED`, so a brief overlap would be harmless anyway.

**Cleanup in batches.** Retention deletes run in `LIMIT`ed batches. One huge `DELETE` would hold
locks, bloat WAL and stall replicas.

**The `pgwire` driver.** I wrote this because the build environment had no access to the
module proxy, and it turned into a feature: the project has zero third-party dependencies. It
implements startup, cleartext/MD5/SCRAM-SHA-256 auth, TLS (`sslmode=require|verify-full`), the
extended query protocol with text-format parameters, transactions with isolation levels,
cancellation (by expiring the socket deadline and discarding the connection), and a reconnecting
LISTEN client. **For production, swap in pgx:** change the import to
`github.com/jackc/pgx/v5/stdlib` and open with `sql.Open("pgx", dsn)`. The queue code only uses
`database/sql`. Queries cast every parameter explicitly (`$1::bigint`), which keeps them correct
under either driver.

## Testing

The integration tests run against a real Postgres. Nothing is mocked, because the interesting
bugs in a queue live in locking and transaction semantics.

```bash
make test-db     # throwaway postgres in docker on :5433
make test        # go test -race -count=1 -p 1 ./...
```

There are 43 tests, including:

- Exactly-once claiming under contention (4 workers × 8 slots, 600 jobs)
- Fencing against a stalled worker; crash recovery via lease expiry; leader election
- Retries/backoff, permanent errors, snooze, panics, timeouts
- Cancelling a running job; graceful shutdown (finish vs. release)
- Transactional enqueue (rollback ⇒ no job), dedup across single and batch enqueue
- Priority ordering, delayed jobs, pause/resume, kind/queue routing, NOTIFY wake-up
- HTTP API: lifecycle, validation, auth, pagination, metrics exposition
- Driver: type round-trips, error recovery, context cancellation, LISTEN/NOTIFY, SCRAM and MD5 auth

An end-to-end check: run `serve`, enqueue a long `report.generate` job, `kill -9` the process
mid-job, start another, and watch it take over the maintainer role and finish the job on attempt 2
with `"lease expired"` recorded in its error history.

## Benchmark

`conveyor bench -jobs 20000 -concurrency 50`, run on a 2-vCPU VM with Postgres on the same machine:

```
enqueued 20000 jobs in 368ms (54325 jobs/s)       # one jsonb_to_recordset INSERT per 1000 jobs
processed 20000 jobs in 14.5s (1381 jobs/s)       # no-op handler; claim + complete round trips
```

Processing is bounded by one `UPDATE` per job completion. The obvious next optimisation is
batching completions.

## Project layout

```
cmd/conveyor/          CLI: migrate | serve [-role all|api|worker] | bench
internal/pgwire/       PostgreSQL wire-protocol driver for database/sql + LISTEN client
internal/migrate/      embedded SQL migrations, advisory-locked runner
internal/queue/        client (enqueue/admin), worker, maintainer, backoff  ← the core
internal/api/          JSON HTTP API, auth, request IDs, metrics middleware
internal/handlers/     example jobs: email, webhook (429/5xx/4xx handling), long report
internal/metrics/      minimal Prometheus text-format exporter
```

## Possible extensions

- Batch completions/heartbeats to raise throughput
- Cron-style recurring jobs (unique_key per schedule slot makes them naturally idempotent)
- Workflow dependencies (job B waits for job A)
- Per-queue concurrency and rate limits
- Partition `conveyor_jobs` by state or time for very large histories
- Live updates over server-sent events instead of polling
