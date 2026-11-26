// Package migrate applies the embedded SQL migrations in order.
//
// Each migration runs in its own transaction together with the bookkeeping
// insert, and the whole run holds a session-level advisory lock so several
// replicas starting at once can't race each other.
package migrate

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var files embed.FS

// lockKey is an arbitrary constant shared by every Conveyor process.
const lockKey = 0x636f6e76 // "conv"

// Up applies all pending migrations and returns how many ran.
func Up(ctx context.Context, db *sql.DB, log *slog.Logger) (int, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1::bigint)`, int64(lockKey)); err != nil {
		return 0, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1::bigint)`, int64(lockKey))

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS conveyor_schema_migrations (
		version    TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return 0, err
	}

	applied := map[string]bool{}
	rows, err := conn.QueryContext(ctx, `SELECT version FROM conveyor_schema_migrations`)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return 0, err
		}
		applied[v] = true
	}
	rows.Close()

	names, err := fs.Glob(files, "migrations/*.sql")
	if err != nil {
		return 0, err
	}
	sort.Strings(names)

	count := 0
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		if applied[version] {
			continue
		}
		body, err := files.ReadFile(name)
		if err != nil {
			return count, err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return count, err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			tx.Rollback()
			return count, fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO conveyor_schema_migrations (version) VALUES ($1::text)`, version); err != nil {
			tx.Rollback()
			return count, err
		}
		if err := tx.Commit(); err != nil {
			return count, err
		}
		log.Info("applied migration", "version", version)
		count++
	}
	return count, nil
}
