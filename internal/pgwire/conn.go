package pgwire

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/tls"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

const protocolVersion = 196608 // 3.0

// Error is a PostgreSQL ErrorResponse.
type Error struct {
	Severity   string
	Code       string // SQLSTATE, e.g. 23505 for unique_violation
	Message    string
	Detail     string
	Hint       string
	Table      string
	Constraint string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("postgres: %s: %s (SQLSTATE %s)", e.Severity, e.Message, e.Code)
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// Notification is a message delivered by NOTIFY.
type Notification struct {
	PID     int32
	Channel string
	Payload string
}

type column struct {
	name string
	oid  uint32
}

type result struct {
	cols []column
	rows [][]driver.Value
	tag  string
}

// conn is a single physical connection speaking the v3 protocol.
type conn struct {
	nc       net.Conn
	br       *bufio.Reader
	wbuf     []byte
	cfg      *Config
	bad      bool
	txStatus byte
	params   map[string]string
	// onNotify receives asynchronous notifications; nil means drop them.
	onNotify func(Notification)
}

func dial(ctx context.Context, cfg *Config) (*conn, error) {
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}
	network, addr := cfg.network()
	var d net.Dialer
	nc, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("pgwire: connect %s: %w", addr, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(dl)
	}
	c := &conn{nc: nc, cfg: cfg, params: map[string]string{}}
	if network == "tcp" && cfg.SSLMode != "disable" {
		if err := c.upgradeTLS(); err != nil {
			nc.Close()
			return nil, err
		}
	}
	c.br = bufio.NewReaderSize(c.nc, 16*1024)
	if err := c.startup(); err != nil {
		c.nc.Close()
		return nil, err
	}
	_ = c.nc.SetDeadline(time.Time{})
	return c, nil
}

func (c *conn) upgradeTLS() error {
	// SSLRequest: length 8, code 80877103.
	req := []byte{0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f}
	if _, err := c.nc.Write(req); err != nil {
		return err
	}
	resp := make([]byte, 1)
	if _, err := io.ReadFull(c.nc, resp); err != nil {
		return err
	}
	if resp[0] != 'S' {
		return errors.New("pgwire: server does not support TLS but sslmode requires it")
	}
	tcfg := &tls.Config{ServerName: c.cfg.Host, MinVersion: tls.VersionTLS12}
	if c.cfg.SSLMode == "require" {
		// Matches libpq: "require" encrypts but does not verify the certificate.
		tcfg.InsecureSkipVerify = true
	}
	tc := tls.Client(c.nc, tcfg)
	if err := tc.Handshake(); err != nil {
		return fmt.Errorf("pgwire: tls handshake: %w", err)
	}
	c.nc = tc
	return nil
}

// ---- low-level framing ----

func (c *conn) begin(typ byte) {
	c.wbuf = append(c.wbuf, typ, 0, 0, 0, 0)
}

// finish patches the length of the message that starts at offset start.
func (c *conn) finish(start int) {
	binary.BigEndian.PutUint32(c.wbuf[start+1:], uint32(len(c.wbuf)-start-1))
}

func (c *conn) putString(s string) { c.wbuf = append(c.wbuf, s...); c.wbuf = append(c.wbuf, 0) }
func (c *conn) putInt32(n int32)   { c.wbuf = binary.BigEndian.AppendUint32(c.wbuf, uint32(n)) }
func (c *conn) putInt16(n int16)   { c.wbuf = binary.BigEndian.AppendUint16(c.wbuf, uint16(n)) }

func (c *conn) msg(typ byte, build func()) {
	start := len(c.wbuf)
	c.begin(typ)
	build()
	c.finish(start)
}

func (c *conn) flush() error {
	_, err := c.nc.Write(c.wbuf)
	c.wbuf = c.wbuf[:0]
	if err != nil {
		c.bad = true
	}
	return err
}

func (c *conn) readMessage() (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(c.br, hdr[:]); err != nil {
		c.bad = true
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr[1:])) - 4
	if n < 0 || n > 1<<30 {
		c.bad = true
		return 0, nil, fmt.Errorf("pgwire: invalid message length %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(c.br, body); err != nil {
		c.bad = true
		return 0, nil, err
	}
	return hdr[0], body, nil
}

// ---- startup & auth ----

func (c *conn) startup() error {
	params := []string{
		"user", c.cfg.User,
		"database", c.cfg.Database,
		"application_name", c.cfg.AppName,
		"client_encoding", "UTF8",
		"DateStyle", "ISO",
		"TimeZone", "UTC",
	}
	buf := []byte{0, 0, 0, 0}
	buf = binary.BigEndian.AppendUint32(buf, protocolVersion)
	for _, p := range params {
		buf = append(buf, p...)
		buf = append(buf, 0)
	}
	buf = append(buf, 0)
	binary.BigEndian.PutUint32(buf, uint32(len(buf)))
	if _, err := c.nc.Write(buf); err != nil {
		return err
	}

	var scram *scramClient
	for {
		typ, body, err := c.readMessage()
		if err != nil {
			return fmt.Errorf("pgwire: startup: %w", err)
		}
		switch typ {
		case 'R':
			code := binary.BigEndian.Uint32(body)
			switch code {
			case 0: // AuthenticationOk
			case 3: // cleartext
				c.msg('p', func() { c.putString(c.cfg.Password) })
				if err := c.flush(); err != nil {
					return err
				}
			case 5: // MD5
				salt := body[4:8]
				inner := md5hex(c.cfg.Password + c.cfg.User)
				pw := "md5" + md5hex(inner+string(salt))
				c.msg('p', func() { c.putString(pw) })
				if err := c.flush(); err != nil {
					return err
				}
			case 10: // SASL
				mechs := strings.Split(strings.TrimRight(string(body[4:]), "\x00"), "\x00")
				if !contains(mechs, "SCRAM-SHA-256") {
					return fmt.Errorf("pgwire: unsupported SASL mechanisms %v", mechs)
				}
				scram = newScramClient(c.cfg.Password)
				first := scram.clientFirst()
				c.msg('p', func() {
					c.putString("SCRAM-SHA-256")
					c.putInt32(int32(len(first)))
					c.wbuf = append(c.wbuf, first...)
				})
				if err := c.flush(); err != nil {
					return err
				}
			case 11: // SASLContinue
				if scram == nil {
					return errors.New("pgwire: unexpected SASLContinue")
				}
				final, err := scram.clientFinal(body[4:])
				if err != nil {
					return err
				}
				c.msg('p', func() { c.wbuf = append(c.wbuf, final...) })
				if err := c.flush(); err != nil {
					return err
				}
			case 12: // SASLFinal
				if scram == nil {
					return errors.New("pgwire: unexpected SASLFinal")
				}
				if err := scram.verifyServer(body[4:]); err != nil {
					return err
				}
			default:
				return fmt.Errorf("pgwire: unsupported auth method %d", code)
			}
		case 'S':
			parts := strings.SplitN(string(body), "\x00", 3)
			if len(parts) >= 2 {
				c.params[parts[0]] = parts[1]
			}
		case 'K': // BackendKeyData: not used (we cancel by closing the socket)
		case 'E':
			return parseError(body)
		case 'N':
		case 'Z':
			c.txStatus = body[0]
			return nil
		default:
			return fmt.Errorf("pgwire: unexpected startup message %q", typ)
		}
	}
}

func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func parseError(body []byte) *Error {
	e := &Error{}
	for len(body) > 1 {
		field := body[0]
		end := 1
		for end < len(body) && body[end] != 0 {
			end++
		}
		val := string(body[1:end])
		body = body[min(end+1, len(body)):]
		switch field {
		case 'V':
			e.Severity = val
		case 'S':
			if e.Severity == "" {
				e.Severity = val
			}
		case 'C':
			e.Code = val
		case 'M':
			e.Message = val
		case 'D':
			e.Detail = val
		case 'H':
			e.Hint = val
		case 't':
			e.Table = val
		case 'n':
			e.Constraint = val
		}
	}
	return e
}

// ---- queries ----

// watchCancel makes a blocking read/write fail when ctx is cancelled by
// pushing the socket deadline into the past. The returned func must be called
// when the round trip is over; it reports whether the context fired.
func (c *conn) watchCancel(ctx context.Context) func() bool {
	if ctx.Done() == nil {
		return func() bool { return false }
	}
	stop := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.nc.SetDeadline(time.Unix(1, 0))
			done <- true
		case <-stop:
			done <- false
		}
	}()
	return func() bool {
		close(stop)
		fired := <-done
		if fired {
			_ = c.nc.SetDeadline(time.Time{})
		}
		return fired
	}
}

// exec runs a statement. With no args it uses the simple query protocol
// (which allows multiple statements, handy for migrations); otherwise the
// extended protocol with text-encoded parameters.
func (c *conn) exec(ctx context.Context, query string, args []driver.NamedValue) (*result, error) {
	if c.bad {
		return nil, driver.ErrBadConn
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stop := c.watchCancel(ctx)
	res, err := c.roundTrip(query, args)
	if fired := stop(); fired {
		// The protocol stream may be mid-message; the connection can't be reused.
		c.bad = true
		c.nc.Close()
		return nil, ctx.Err()
	}
	return res, err
}

func (c *conn) roundTrip(query string, args []driver.NamedValue) (*result, error) {
	if len(args) == 0 {
		c.msg('Q', func() { c.putString(query) })
	} else {
		params := make([][]byte, len(args))
		nulls := make([]bool, len(args))
		for i, a := range args {
			b, isNull, err := encodeParam(a.Value)
			if err != nil {
				return nil, err
			}
			params[i], nulls[i] = b, isNull
		}
		c.msg('P', func() { c.putString(""); c.putString(query); c.putInt16(0) })
		c.msg('B', func() {
			c.putString("") // portal
			c.putString("") // statement
			c.putInt16(0)   // all params text
			c.putInt16(int16(len(params)))
			for i, p := range params {
				if nulls[i] {
					c.putInt32(-1)
					continue
				}
				c.putInt32(int32(len(p)))
				c.wbuf = append(c.wbuf, p...)
			}
			c.putInt16(0) // all results text
		})
		c.msg('D', func() { c.wbuf = append(c.wbuf, 'P'); c.putString("") })
		c.msg('E', func() { c.putString(""); c.putInt32(0) })
		c.msg('S', func() {})
	}
	if err := c.flush(); err != nil {
		return nil, err
	}

	res := &result{}
	var pgErr *Error
	for {
		typ, body, err := c.readMessage()
		if err != nil {
			return nil, err
		}
		switch typ {
		case '1', '2', 'n', 'I': // ParseComplete, BindComplete, NoData, EmptyQuery
		case 'T':
			// A new RowDescription in a multi-statement simple query starts a new result set.
			res.cols, res.rows = parseRowDescription(body), nil
		case 'D':
			row, err := parseDataRow(body, res.cols)
			if err != nil {
				return nil, err
			}
			res.rows = append(res.rows, row)
		case 'C':
			res.tag = strings.TrimRight(string(body), "\x00")
		case 'E':
			pgErr = parseError(body)
		case 'A':
			c.dispatchNotification(body)
		case 'N', 'S':
		case 'Z':
			c.txStatus = body[0]
			if pgErr != nil {
				return nil, pgErr
			}
			return res, nil
		default:
			c.bad = true
			return nil, fmt.Errorf("pgwire: unexpected message %q", typ)
		}
	}
}

func (c *conn) dispatchNotification(body []byte) {
	if c.onNotify == nil || len(body) < 4 {
		return
	}
	pid := int32(binary.BigEndian.Uint32(body))
	parts := strings.SplitN(string(body[4:]), "\x00", 3)
	n := Notification{PID: pid}
	if len(parts) > 0 {
		n.Channel = parts[0]
	}
	if len(parts) > 1 {
		n.Payload = parts[1]
	}
	c.onNotify(n)
}

func parseRowDescription(body []byte) []column {
	n := int(binary.BigEndian.Uint16(body))
	body = body[2:]
	cols := make([]column, n)
	for i := 0; i < n; i++ {
		end := 0
		for body[end] != 0 {
			end++
		}
		cols[i].name = string(body[:end])
		body = body[end+1:]
		// table oid(4) attnum(2) type oid(4) typlen(2) typmod(4) format(2)
		cols[i].oid = binary.BigEndian.Uint32(body[6:10])
		body = body[18:]
	}
	return cols
}

func parseDataRow(body []byte, cols []column) ([]driver.Value, error) {
	n := int(binary.BigEndian.Uint16(body))
	body = body[2:]
	row := make([]driver.Value, n)
	for i := 0; i < n; i++ {
		l := int32(binary.BigEndian.Uint32(body))
		body = body[4:]
		if l < 0 {
			row[i] = nil
			continue
		}
		var oid uint32
		if i < len(cols) {
			oid = cols[i].oid
		}
		v, err := decodeText(oid, body[:l])
		if err != nil {
			return nil, fmt.Errorf("pgwire: column %d: %w", i, err)
		}
		row[i] = v
		body = body[l:]
	}
	return row, nil
}

func (c *conn) close() error {
	if c.nc == nil {
		return nil
	}
	if !c.bad {
		c.msg('X', func() {})
		_ = c.flush()
	}
	err := c.nc.Close()
	c.nc = nil
	c.bad = true
	return err
}
