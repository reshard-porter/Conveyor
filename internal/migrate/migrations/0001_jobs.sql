-- Core job table. One row per job; state machine:
--
--   pending ──claim──▶ running ──ok──▶ succeeded
--      ▲                  │
--      └──retry (backoff)─┤──attempts exhausted / permanent──▶ dead
--                         └──cancel──▶ cancelled
CREATE TABLE conveyor_jobs (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue         TEXT        NOT NULL DEFAULT 'default',
    kind          TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}',
    state         TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (state IN ('pending', 'running', 'succeeded', 'dead', 'cancelled')),
    priority      SMALLINT    NOT NULL DEFAULT 0 CHECK (priority BETWEEN -100 AND 100),
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 100),
    run_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    timeout_ms    INT         NOT NULL DEFAULT 60000 CHECK (timeout_ms > 0),
    unique_key    TEXT,
    locked_by     TEXT,
    lease_token   UUID,        -- fresh per claim; fences every write a worker makes
    locked_until  TIMESTAMPTZ,
    result        JSONB,
    last_error    TEXT,
    errors        JSONB       NOT NULL DEFAULT '[]',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    CONSTRAINT running_has_lease CHECK (state <> 'running' OR (locked_by IS NOT NULL AND locked_until IS NOT NULL AND lease_token IS NOT NULL))
);

-- The hot path: workers claim the highest-priority, oldest runnable job.
-- Partial index keeps it tiny no matter how many finished jobs exist.
CREATE INDEX conveyor_jobs_claim_idx
    ON conveyor_jobs (queue, priority DESC, run_at, id)
    WHERE state = 'pending';

-- Lets the reaper find expired leases without scanning.
CREATE INDEX conveyor_jobs_lease_idx
    ON conveyor_jobs (locked_until)
    WHERE state = 'running';

-- Deduplication: at most one active job per unique_key.
-- Finished jobs don't count, so the same key can be enqueued again later.
CREATE UNIQUE INDEX conveyor_jobs_unique_idx
    ON conveyor_jobs (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('pending', 'running');

-- Retention cleanup and dashboards.
CREATE INDEX conveyor_jobs_finished_idx
    ON conveyor_jobs (state, finished_at)
    WHERE state IN ('succeeded', 'dead', 'cancelled');

CREATE INDEX conveyor_jobs_queue_state_id_idx
    ON conveyor_jobs (queue, state, id DESC);

-- Paused queues are skipped by the claim query.
CREATE TABLE conveyor_paused_queues (
    queue      TEXT PRIMARY KEY,
    paused_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Wake idle workers immediately when runnable work appears, instead of
-- waiting for their next poll. pg_notify de-duplicates identical payloads
-- within a transaction, so a bulk insert sends one notification per queue.
CREATE FUNCTION conveyor_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = 'pending' AND NEW.run_at <= now() THEN
        PERFORM pg_notify('conveyor_jobs', NEW.queue);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER conveyor_jobs_notify
    AFTER INSERT OR UPDATE OF state, run_at ON conveyor_jobs
    FOR EACH ROW EXECUTE FUNCTION conveyor_notify();
