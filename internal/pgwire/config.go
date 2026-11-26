// Package pgwire is a small, dependency-free PostgreSQL driver for database/sql.
//
// It implements the parts of the v3 wire protocol that a backend service
// actually needs: startup with cleartext/MD5/SCRAM-SHA-256 auth, optional TLS,
// the extended query protocol with text-format parameters, transactions,
// context cancellation, and LISTEN/NOTIFY via a dedicated Listener.
//
// It exists so the project builds with nothing but the Go standard library.
// In production you can swap it for github.com/jackc/pgx/v5/stdlib by changing
// the driver import and name; the rest of the codebase only uses database/sql.
package pgwire

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config describes how to reach a PostgreSQL server.
type Config struct {
	Host           string // hostname, IP, or a directory containing a Unix socket
	Port           int
	User           string
	Password       string
	Database       string
	SSLMode        string // disable (default), require, verify-full
	AppName        string
	ConnectTimeout time.Duration
}

// ParseDSN parses a postgres:// URL. Supported query parameters:
// sslmode, application_name, connect_timeout (seconds), host (socket dir).
func ParseDSN(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgwire: invalid dsn: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("pgwire: dsn scheme must be postgres://, got %q", u.Scheme)
	}
	cfg := &Config{
		Host:           u.Hostname(),
		Port:           5432,
		Database:       strings.TrimPrefix(u.Path, "/"),
		SSLMode:        "disable",
		AppName:        "conveyor",
		ConnectTimeout: 10 * time.Second,
	}
	if p := u.Port(); p != "" {
		if cfg.Port, err = strconv.Atoi(p); err != nil {
			return nil, fmt.Errorf("pgwire: invalid port %q", p)
		}
	}
	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}
	q := u.Query()
	if h := q.Get("host"); h != "" {
		cfg.Host = h
	}
	if m := q.Get("sslmode"); m != "" {
		switch m {
		case "disable", "require", "verify-full":
			cfg.SSLMode = m
		default:
			return nil, fmt.Errorf("pgwire: unsupported sslmode %q (use disable, require or verify-full)", m)
		}
	}
	if a := q.Get("application_name"); a != "" {
		cfg.AppName = a
	}
	if t := q.Get("connect_timeout"); t != "" {
		secs, err := strconv.Atoi(t)
		if err != nil {
			return nil, fmt.Errorf("pgwire: invalid connect_timeout %q", t)
		}
		cfg.ConnectTimeout = time.Duration(secs) * time.Second
	}
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	if cfg.User == "" {
		cfg.User = os.Getenv("USER")
	}
	if cfg.Database == "" {
		cfg.Database = cfg.User
	}
	return cfg, nil
}

func (c *Config) network() (string, string) {
	if strings.HasPrefix(c.Host, "/") {
		return "unix", fmt.Sprintf("%s/.s.PGSQL.%d", c.Host, c.Port)
	}
	return "tcp", net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}
