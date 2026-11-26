package pgwire

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("CONVEYOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CONVEYOR_TEST_DATABASE_URL not set")
	}
	return dsn
}

func openDB(t *testing.T) *sql.DB {
	db, err := sql.Open(DriverName, testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestParseDSN(t *testing.T) {
	cfg, err := ParseDSN("postgres://u:p%40ss@db.example:6543/app?sslmode=require&application_name=x&connect_timeout=3")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Host != "db.example" || cfg.Port != 6543 || cfg.User != "u" || cfg.Password != "p@ss" ||
		cfg.Database != "app" || cfg.SSLMode != "require" || cfg.AppName != "x" || cfg.ConnectTimeout != 3*time.Second {
		t.Fatalf("unexpected config %+v", cfg)
	}
	cfg, err = ParseDSN("postgres://postgres@/db?host=/tmp")
	if err != nil {
		t.Fatal(err)
	}
	if n, a := cfg.network(); n != "unix" || a != "/tmp/.s.PGSQL.5432" {
		t.Fatalf("socket: %s %s", n, a)
	}
	if _, err := ParseDSN("mysql://x"); err == nil {
		t.Fatal("expected scheme error")
	}
}

func TestTextArray(t *testing.T) {
	got := TextArray([]string{"a", `b"c`, `d\e`, "f,g"})
	want := `{"a","b\"c","d\\e","f,g"}`
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestRoundTripTypes(t *testing.T) {
	db := openDB(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	var (
		i     int64
		f     float64
		b     bool
		s     string
		ts    time.Time
		blob  []byte
		null  sql.NullString
		arr   string
		jsonv string
	)
	err := db.QueryRow(`SELECT $1::bigint, $2::float8, $3::bool, $4::text, $5::timestamptz, $6::bytea,
	                           NULL::text, ($7::text[])[2], $8::jsonb->>'k'`,
		int64(1<<40), 3.25, true, "héllo 'world'", now, []byte{0, 1, 255}, TextArray([]string{"x", "y,z"}), `{"k":"v"}`).
		Scan(&i, &f, &b, &s, &ts, &blob, &null, &arr, &jsonv)
	if err != nil {
		t.Fatal(err)
	}
	if i != 1<<40 || f != 3.25 || !b || s != "héllo 'world'" || !ts.Equal(now) ||
		string(blob) != string([]byte{0, 1, 255}) || null.Valid || arr != "y,z" || jsonv != "v" {
		t.Fatalf("mismatch: %v %v %v %q %v %v %v %q %q", i, f, b, s, ts, blob, null, arr, jsonv)
	}
}

func TestErrorsAndRecovery(t *testing.T) {
	db := openDB(t)
	db.SetMaxOpenConns(1)
	_, err := db.Exec(`SELECT 1/0`)
	var pgErr *Error
	if !errors.As(err, &pgErr) || pgErr.Code != "22012" {
		t.Fatalf("want division_by_zero, got %v", err)
	}
	// Same (only) connection must still be usable after an error.
	var n int
	if err := db.QueryRow(`SELECT $1::int + 1`, 41).Scan(&n); err != nil || n != 42 {
		t.Fatalf("connection not usable after error: %v %d", err, n)
	}
}

func TestTransactions(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(`CREATE TEMP TABLE IF NOT EXISTS pgw_tx (v int)`); err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1) // temp table lives on this connection
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO pgw_tx VALUES ($1::int)`, 7); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM pgw_tx`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rollback failed: %v %d", err, n)
	}
	res, err := db.Exec(`INSERT INTO pgw_tx SELECT generate_series(1, 5)`)
	if err != nil {
		t.Fatal(err)
	}
	if ra, _ := res.RowsAffected(); ra != 5 {
		t.Fatalf("RowsAffected = %d", ra)
	}
}

func TestContextCancel(t *testing.T) {
	db := openDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := db.ExecContext(ctx, `SELECT pg_sleep(5)`)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("cancellation took too long")
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("pool should recover: %v", err)
	}
}

func TestListener(t *testing.T) {
	db := openDB(t)
	cfg, _ := ParseDSN(testDSN(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan Notification, 8)
	l := NewListener(cfg, nil, "pgw_test")
	go l.Run(ctx, out)

	deadline := time.After(5 * time.Second)
	for {
		// Keep notifying until the listener is subscribed and receives one.
		if _, err := db.Exec(`SELECT pg_notify('pgw_test', $1::text)`, "hello"); err != nil {
			t.Fatal(err)
		}
		select {
		case n := <-out:
			if n.Channel != "pgw_test" || n.Payload != "hello" {
				t.Fatalf("unexpected notification %+v", n)
			}
			return
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("no notification received")
		}
	}
}

func TestPasswordAuth(t *testing.T) {
	for name, dsn := range map[string]string{
		"scram": os.Getenv("CONVEYOR_TEST_SCRAM_URL"),
		"md5":   os.Getenv("CONVEYOR_TEST_MD5_URL"),
	} {
		t.Run(name, func(t *testing.T) {
			if dsn == "" {
				t.Skip("auth test DSN not set")
			}
			db, err := sql.Open(DriverName, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Ping(); err != nil {
				t.Fatalf("auth failed: %v", err)
			}
		})
	}
}
