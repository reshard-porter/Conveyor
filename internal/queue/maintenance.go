package queue

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

type MaintenanceConfig struct {
	ReapInterval       time.Duration // how often to recover expired leases, default 5s
	CleanupInterval    time.Duration // how often to delete old finished jobs, default 1m
	SucceededRetention time.Duration // keep succeeded/cancelled jobs this long, default 24h
	DeadRetention      time.Duration // keep dead jobs this long, default 7d
	BatchSize          int           // rows per cleanup/reap statement, default 1000
	LeaderRetry        time.Duration // how often non-leaders try to take over, default 10s
	Logger             *slog.Logger
	Metrics            *Metrics
}

// Maintainer recovers jobs from crashed workers and enforces retention.
// Any number of processes may run one; a Postgres advisory lock elects a
// single leader, and leadership fails over automatically when the leader's
// session ends.
type Maintainer struct {
	db  *sql.DB
	cfg MaintenanceConfig
	log *slog.Logger
}

const leaderLockKey = 0x636f6e6d // "conm"

func NewMaintainer(db *sql.DB, cfg MaintenanceConfig) *Maintainer {
	if cfg.ReapInterval <= 0 {
		cfg.ReapInterval = 5 * time.Second
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = time.Minute
	}
	if cfg.SucceededRetention <= 0 {
		cfg.SucceededRetention = 24 * time.Hour
	}
	if cfg.DeadRetention <= 0 {
		cfg.DeadRetention = 7 * 24 * time.Hour
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.LeaderRetry <= 0 {
		cfg.LeaderRetry = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Maintainer{db: db, cfg: cfg, log: cfg.Logger.With("component", "maintainer")}
}

func (m *Maintainer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		led, err := m.leadOnce(ctx)
		if err != nil && ctx.Err() == nil {
			m.log.Warn("maintenance leadership ended", "err", err)
		}
		if !led || err != nil {
			sleep(ctx, m.cfg.LeaderRetry)
		}
	}
	return nil
}

// leadOnce tries to become leader and, if successful, runs maintenance until
// ctx ends or the lock connection fails.
func (m *Maintainer) leadOnce(ctx context.Context) (bool, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	var got bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1::bigint)`, int64(leaderLockKey)).Scan(&got); err != nil {
		return false, err
	}
	if !got {
		return false, nil
	}
	m.log.Info("became maintenance leader")
	defer conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1::bigint)`, int64(leaderLockKey))

	reap := time.NewTicker(m.cfg.ReapInterval)
	defer reap.Stop()
	clean := time.NewTicker(m.cfg.CleanupInterval)
	defer clean.Stop()
	for {
		select {
		case <-ctx.Done():
			return true, nil
		case <-reap.C:
			// Holding the lock is only meaningful while its session is alive.
			if err := conn.PingContext(ctx); err != nil {
				return true, err
			}
			if n, err := m.Reap(ctx); err != nil {
				m.log.Error("reap failed", "err", err)
			} else if n > 0 {
				m.log.Warn("recovered jobs with expired leases", "count", n)
			}
		case <-clean.C:
			if n, err := m.Cleanup(ctx); err != nil {
				m.log.Error("cleanup failed", "err", err)
			} else if n > 0 {
				m.log.Info("deleted old finished jobs", "count", n)
			}
		}
	}
}

// Reap returns running jobs whose lease has expired (their worker crashed,
// was partitioned, or stalled) to pending, or to dead if out of attempts.
func (m *Maintainer) Reap(ctx context.Context) (int64, error) {
	res, err := m.db.ExecContext(ctx, `
		UPDATE conveyor_jobs SET
		    state       = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
		    finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
		    run_at      = now(),
		    last_error  = 'lease expired: worker ' || locked_by || ' stopped heartbeating',
		    errors      = jsonb_path_query_array(errors || jsonb_build_array(jsonb_build_object(
		                      'attempt', attempts, 'error', 'lease expired', 'at', now(), 'worker', locked_by)),
		                  '$[last-19 to last]'),
		    locked_by = NULL, locked_until = NULL, lease_token = NULL, updated_at = now()
		WHERE id IN (
		    SELECT id FROM conveyor_jobs
		    WHERE state = 'running' AND locked_until < now()
		    LIMIT $1::int
		    FOR UPDATE SKIP LOCKED)`, int64(m.cfg.BatchSize))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if m.cfg.Metrics != nil && n > 0 {
		m.cfg.Metrics.reaped.Add(float64(n))
	}
	return n, nil
}

// Cleanup deletes finished jobs past retention, in batches so it never holds
// long locks or bloats WAL with one giant DELETE.
func (m *Maintainer) Cleanup(ctx context.Context) (int64, error) {
	var total int64
	for _, rule := range []struct {
		states    string
		retention time.Duration
		label     string
	}{
		{"{succeeded,cancelled}", m.cfg.SucceededRetention, "succeeded_or_cancelled"},
		{"{dead}", m.cfg.DeadRetention, "dead"},
	} {
		for {
			res, err := m.db.ExecContext(ctx, `
				DELETE FROM conveyor_jobs WHERE id IN (
				    SELECT id FROM conveyor_jobs
				    WHERE state = ANY($1::text[])
				      AND finished_at < now() - $2::bigint * interval '1 millisecond'
				    LIMIT $3::int)`, rule.states, rule.retention.Milliseconds(), int64(m.cfg.BatchSize))
			if err != nil {
				return total, err
			}
			n, _ := res.RowsAffected()
			total += n
			if m.cfg.Metrics != nil && n > 0 {
				m.cfg.Metrics.cleaned.Add(float64(n), rule.label)
			}
			if n < int64(m.cfg.BatchSize) || ctx.Err() != nil {
				break
			}
		}
	}
	return total, nil
}
