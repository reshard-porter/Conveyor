package pgwire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// DriverName is the name registered with database/sql.
const DriverName = "pgwire"

func init() { sql.Register(DriverName, &Driver{}) }

// Driver implements driver.Driver and driver.DriverContext.
type Driver struct{}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &Connector{cfg: cfg, drv: d}, nil
}

// Connector lets callers build a *sql.DB from a parsed Config.
type Connector struct {
	cfg *Config
	drv *Driver
}

// NewConnector returns a driver.Connector for use with sql.OpenDB.
func NewConnector(cfg *Config) *Connector { return &Connector{cfg: cfg, drv: &Driver{}} }

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	pc, err := dial(ctx, c.cfg)
	if err != nil {
		return nil, err
	}
	return &sqlConn{c: pc}, nil
}

func (c *Connector) Driver() driver.Driver { return c.drv }

// sqlConn adapts *conn to the database/sql driver interfaces.
type sqlConn struct{ c *conn }

var (
	_ driver.ExecerContext      = (*sqlConn)(nil)
	_ driver.QueryerContext     = (*sqlConn)(nil)
	_ driver.ConnBeginTx        = (*sqlConn)(nil)
	_ driver.Pinger             = (*sqlConn)(nil)
	_ driver.SessionResetter    = (*sqlConn)(nil)
	_ driver.Validator          = (*sqlConn)(nil)
	_ driver.ConnPrepareContext = (*sqlConn)(nil)
)

func (s *sqlConn) Prepare(query string) (driver.Stmt, error) {
	return s.PrepareContext(context.Background(), query)
}

// PrepareContext returns a lightweight statement; each execution re-parses on
// the unnamed statement, which keeps the driver simple and pooler-friendly.
func (s *sqlConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: s, query: query}, nil
}

func (s *sqlConn) Close() error { return s.c.close() }

func (s *sqlConn) Begin() (driver.Tx, error) {
	return s.BeginTx(context.Background(), driver.TxOptions{})
}

func (s *sqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	q := "BEGIN"
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	case sql.LevelReadCommitted:
		q += " ISOLATION LEVEL READ COMMITTED"
	case sql.LevelRepeatableRead, sql.LevelSnapshot:
		q += " ISOLATION LEVEL REPEATABLE READ"
	case sql.LevelSerializable:
		q += " ISOLATION LEVEL SERIALIZABLE"
	default:
		return nil, fmt.Errorf("pgwire: unsupported isolation level %d", opts.Isolation)
	}
	if opts.ReadOnly {
		q += " READ ONLY"
	}
	if _, err := s.c.exec(ctx, q, nil); err != nil {
		return nil, err
	}
	return &tx{conn: s}, nil
}

func (s *sqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res, err := s.c.exec(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return execResult(res.tag), nil
}

func (s *sqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res, err := s.c.exec(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &rows{res: res}, nil
}

func (s *sqlConn) Ping(ctx context.Context) error {
	_, err := s.c.exec(ctx, "SELECT 1", nil)
	return err
}

func (s *sqlConn) ResetSession(context.Context) error {
	if s.c.bad {
		return driver.ErrBadConn
	}
	return nil
}

func (s *sqlConn) IsValid() bool { return !s.c.bad }

type stmt struct {
	conn  *sqlConn
	query string
}

func (s *stmt) Close() error  { return nil }
func (s *stmt) NumInput() int { return -1 }
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.ExecContext(context.Background(), s.query, named(args))
}
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.QueryContext(context.Background(), s.query, named(args))
}
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

type tx struct{ conn *sqlConn }

func (t *tx) Commit() error {
	_, err := t.conn.c.exec(context.Background(), "COMMIT", nil)
	return err
}

func (t *tx) Rollback() error {
	if t.conn.c.bad {
		return driver.ErrBadConn
	}
	_, err := t.conn.c.exec(context.Background(), "ROLLBACK", nil)
	return err
}

type execResult string

func (r execResult) LastInsertId() (int64, error) {
	return 0, errors.New("pgwire: LastInsertId is not supported; use RETURNING")
}

func (r execResult) RowsAffected() (int64, error) {
	fields := strings.Fields(string(r))
	if len(fields) == 0 {
		return 0, nil
	}
	n, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil {
		return 0, nil
	}
	return n, nil
}

type rows struct {
	res *result
	pos int
}

func (r *rows) Columns() []string {
	names := make([]string, len(r.res.cols))
	for i, c := range r.res.cols {
		names[i] = c.name
	}
	return names
}

func (r *rows) Close() error { return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.res.rows) {
		return io.EOF
	}
	copy(dest, r.res.rows[r.pos])
	r.pos++
	return nil
}
