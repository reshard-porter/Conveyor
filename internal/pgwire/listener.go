package pgwire

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"time"
)

// Listener holds a dedicated connection subscribed to one or more channels
// and delivers notifications. It reconnects with backoff when the connection
// drops, and emits a Notification with an empty Channel after every
// reconnect so consumers know they may have missed messages.
type Listener struct {
	cfg      *Config
	channels []string
	log      *slog.Logger
	// PingInterval controls how often an idle connection is probed.
	PingInterval time.Duration
}

func NewListener(cfg *Config, log *slog.Logger, channels ...string) *Listener {
	if log == nil {
		log = slog.Default()
	}
	return &Listener{cfg: cfg, channels: channels, log: log, PingInterval: 30 * time.Second}
}

// Run blocks until ctx is cancelled, sending notifications to out.
// Sends are non-blocking: if the consumer is slow, notifications are
// coalesced, which is fine for "wake up and poll" semantics.
func (l *Listener) Run(ctx context.Context, out chan<- Notification) error {
	backoff := 250 * time.Millisecond
	first := true
	for {
		err := l.session(ctx, out, !first)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		first = false
		l.log.Warn("listener disconnected; reconnecting", "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}

func (l *Listener) session(ctx context.Context, out chan<- Notification, reconnected bool) error {
	c, err := dial(ctx, l.cfg)
	if err != nil {
		return err
	}
	defer c.close()

	deliver := func(n Notification) {
		select {
		case out <- n:
		default:
		}
	}
	c.onNotify = deliver
	for _, ch := range l.channels {
		if _, err := c.exec(ctx, "LISTEN "+quoteIdent(ch), nil); err != nil {
			return err
		}
	}
	if reconnected {
		deliver(Notification{})
	}

	// Close the socket when ctx ends so the blocking read returns.
	stop := context.AfterFunc(ctx, func() { c.nc.Close() })
	defer stop()

	for {
		_ = c.nc.SetReadDeadline(time.Now().Add(l.PingInterval))
		// Peek the first byte so a timeout never interrupts a half-read message.
		if _, err := c.br.Peek(1); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				_ = c.nc.SetReadDeadline(time.Time{})
				c.msg('Q', func() { c.putString("") }) // empty query = cheap keepalive
				if err := c.flush(); err != nil {
					return err
				}
				continue
			}
			return err
		}
		_ = c.nc.SetReadDeadline(time.Time{})
		typ, body, err := c.readMessage()
		if err != nil {
			return err
		}
		switch typ {
		case 'A':
			c.dispatchNotification(body)
		case 'E':
			return parseError(body)
		}
	}
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
