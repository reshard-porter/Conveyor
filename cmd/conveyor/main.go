// Command conveyor runs the job queue service.
//
//	conveyor migrate                 apply database migrations
//	conveyor serve [-role all|api|worker] [flags]
//	conveyor bench [-jobs N] [-concurrency C]
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/reshard-porter/conveyor/internal/api"
	"github.com/reshard-porter/conveyor/internal/handlers"
	"github.com/reshard-porter/conveyor/internal/metrics"
	"github.com/reshard-porter/conveyor/internal/migrate"
	"github.com/reshard-porter/conveyor/internal/pgwire"
	"github.com/reshard-porter/conveyor/internal/queue"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	log := newLogger()
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "migrate":
		err = runMigrate(ctx, log)
	case "serve":
		err = runServe(ctx, log, os.Args[2:])
	case "bench":
		err = runBench(ctx, log, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: conveyor <migrate|serve|bench> [flags]\n\nEnvironment:\n"+
		"  DATABASE_URL       postgres://user:pass@host:5432/db?sslmode=disable (required)\n"+
		"  CONVEYOR_API_KEY   bearer token for /v1/* (optional)\n"+
		"  LOG_LEVEL          debug|info|warn|error (default info)\n"+
		"  LOG_FORMAT         json|text (default json)")
	os.Exit(2)
}

func newLogger() *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(envOr("LOG_LEVEL", "info")))
	opts := &slog.HandlerOptions{Level: level}
	if os.Getenv("LOG_FORMAT") == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func openDB(ctx context.Context, maxConns int) (*sql.DB, *pgwire.Config, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, nil, errors.New("DATABASE_URL is required")
	}
	cfg, err := pgwire.ParseDSN(dsn)
	if err != nil {
		return nil, nil, err
	}
	db := sql.OpenDB(pgwire.NewConnector(cfg))
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	// Wait for the database: in docker-compose it may still be starting.
	var pingErr error
	for i := 0; i < 30; i++ {
		if pingErr = db.PingContext(ctx); pingErr == nil {
			return db, cfg, nil
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, nil, fmt.Errorf("database not reachable: %w", pingErr)
}

func runMigrate(ctx context.Context, log *slog.Logger) error {
	db, _, err := openDB(ctx, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	n, err := migrate.Up(ctx, db, log)
	if err == nil {
		log.Info("migrations complete", "applied", n)
	}
	return err
}

func runServe(ctx context.Context, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	role := fs.String("role", "all", "all | api | worker")
	addr := fs.String("addr", envOr("ADDR", ":8080"), "HTTP listen address (api role; worker role serves /metrics and /healthz)")
	queues := fs.String("queues", "default,emails,webhooks,reports", "comma-separated queues to work")
	concurrency := fs.Int("concurrency", 20, "max concurrent jobs per process")
	lease := fs.Duration("lease", 30*time.Second, "job lease duration")
	shutdown := fs.Duration("shutdown-timeout", 30*time.Second, "grace period for in-flight jobs")
	autoMigrate := fs.Bool("migrate", true, "apply migrations on startup")
	_ = fs.Parse(args)
	if *role != "all" && *role != "api" && *role != "worker" {
		return fmt.Errorf("invalid -role %q", *role)
	}

	db, pgcfg, err := openDB(ctx, *concurrency+10)
	if err != nil {
		return err
	}
	defer db.Close()
	if *autoMigrate {
		if _, err := migrate.Up(ctx, db, log); err != nil {
			return err
		}
	}

	reg := metrics.NewRegistry()
	qm := queue.NewMetrics(reg)
	var wg sync.WaitGroup
	errc := make(chan error, 4)
	start := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil && !errors.Is(err, context.Canceled) {
				errc <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}

	// HTTP: full API for api/all roles; health + metrics only for workers.
	var handler http.Handler
	if *role == "worker" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", reg.Handler())
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
			if err := db.PingContext(r.Context()); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			fmt.Fprintln(w, "ok")
		})
		handler = mux
	} else {
		handler = api.New(api.Config{DB: db, Logger: log, APIKey: os.Getenv("CONVEYOR_API_KEY"), Registry: reg}).Handler()
	}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	start("http", func() error {
		log.Info("http listening", "addr", *addr, "role", *role)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	if *role != "api" {
		w := queue.NewWorker(db, queue.WorkerConfig{
			Queues:          splitCSV(*queues),
			Concurrency:     *concurrency,
			LeaseDuration:   *lease,
			ShutdownTimeout: *shutdown,
			Logger:          log,
			Metrics:         qm,
		})
		handlers.Register(w, nil)

		notes := make(chan pgwire.Notification, 64)
		listener := pgwire.NewListener(pgcfg, log, "conveyor_jobs")
		start("listener", func() error { return listener.Run(ctx, notes) })
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case n := <-notes:
					w.Notify(n.Payload)
				}
			}
		}()
		start("worker", func() error { return w.Run(ctx) })
		m := queue.NewMaintainer(db, queue.MaintenanceConfig{Logger: log, Metrics: qm})
		start("maintainer", func() error { return m.Run(ctx) })
	}

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received")
	case err := <-errc:
		log.Error("component failed; shutting down", "err", err)
		stopCtx, cancel := context.WithTimeout(context.Background(), *shutdown)
		defer cancel()
		_ = srv.Shutdown(stopCtx)
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	wg.Wait()
	log.Info("bye")
	return nil
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// runBench measures end-to-end throughput: enqueue N no-op jobs in batches,
// then run a worker until all are done.
func runBench(ctx context.Context, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	n := fs.Int("jobs", 10000, "number of jobs")
	concurrency := fs.Int("concurrency", 50, "worker concurrency")
	_ = fs.Parse(args)

	db, _, err := openDB(ctx, *concurrency+5)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := migrate.Up(ctx, db, log); err != nil {
		return err
	}
	client := queue.NewClient(db)
	queueName := fmt.Sprintf("bench-%d", time.Now().UnixNano())

	t0 := time.Now()
	for i := 0; i < *n; i += 1000 {
		batch := make([]queue.EnqueueParams, min(1000, *n-i))
		for j := range batch {
			batch[j] = queue.EnqueueParams{Queue: queueName, Kind: "bench.noop"}
		}
		if _, err := client.EnqueueMany(ctx, batch); err != nil {
			return err
		}
	}
	enqueueDur := time.Since(t0)

	quiet := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	w := queue.NewWorker(db, queue.WorkerConfig{Queues: []string{queueName}, Concurrency: *concurrency,
		PollInterval: 50 * time.Millisecond, Logger: quiet})
	w.Handle("bench.noop", handlers.Noop)
	wctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = w.Run(wctx); close(done) }()

	t1 := time.Now()
	for {
		var remaining int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM conveyor_jobs WHERE queue = $1::text AND state <> 'succeeded'`,
			queueName).Scan(&remaining); err != nil {
			cancel()
			return err
		}
		if remaining == 0 {
			break
		}
		if ctx.Err() != nil {
			cancel()
			return ctx.Err()
		}
		time.Sleep(100 * time.Millisecond)
	}
	processDur := time.Since(t1)
	cancel()
	<-done
	_, _ = db.ExecContext(context.Background(), `DELETE FROM conveyor_jobs WHERE queue = $1::text`, queueName)

	fmt.Printf("enqueued %d jobs in %s (%.0f jobs/s)\n", *n, enqueueDur.Round(time.Millisecond), float64(*n)/enqueueDur.Seconds())
	fmt.Printf("processed %d jobs in %s (%.0f jobs/s) with concurrency %d\n", *n, processDur.Round(time.Millisecond),
		float64(*n)/processDur.Seconds(), *concurrency)
	return nil
}
