package pgwire

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Type OIDs we decode into native Go values. Everything else is returned as
// []byte in PostgreSQL's text format (which database/sql converts to string).
const (
	oidBool        = 16
	oidBytea       = 17
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidFloat4      = 700
	oidFloat8      = 701
	oidTimestamp   = 1114
	oidTimestamptz = 1184
	oidDate        = 1082
)

func encodeParam(v driver.Value) ([]byte, bool, error) {
	switch x := v.(type) {
	case nil:
		return nil, true, nil
	case int64:
		return strconv.AppendInt(nil, x, 10), false, nil
	case float64:
		return strconv.AppendFloat(nil, x, 'g', -1, 64), false, nil
	case bool:
		if x {
			return []byte("t"), false, nil
		}
		return []byte("f"), false, nil
	case string:
		return []byte(x), false, nil
	case []byte:
		// Sent as a bytea hex literal. To send JSON, pass a string instead.
		out := make([]byte, 2+hex.EncodedLen(len(x)))
		out[0], out[1] = '\\', 'x'
		hex.Encode(out[2:], x)
		return out, false, nil
	case time.Time:
		return []byte(x.UTC().Format("2006-01-02 15:04:05.999999Z07:00")), false, nil
	default:
		return nil, false, fmt.Errorf("pgwire: unsupported parameter type %T", v)
	}
}

var timestampLayouts = []string{
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07:00:00",
}

func decodeText(oid uint32, b []byte) (driver.Value, error) {
	switch oid {
	case oidBool:
		return len(b) == 1 && b[0] == 't', nil
	case oidInt2, oidInt4, oidInt8:
		return strconv.ParseInt(string(b), 10, 64)
	case oidFloat4, oidFloat8:
		return strconv.ParseFloat(string(b), 64)
	case oidTimestamptz:
		s := string(b)
		for _, layout := range timestampLayouts {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UTC(), nil
			}
		}
		return nil, fmt.Errorf("cannot parse timestamptz %q", s)
	case oidTimestamp:
		return time.Parse("2006-01-02 15:04:05.999999999", string(b))
	case oidDate:
		return time.Parse("2006-01-02", string(b))
	case oidBytea:
		s := string(b)
		if !strings.HasPrefix(s, `\x`) {
			return nil, fmt.Errorf("unsupported bytea format")
		}
		return hex.DecodeString(s[2:])
	default:
		out := make([]byte, len(b))
		copy(out, b)
		return out, nil
	}
}

// TextArray renders a Go string slice as a PostgreSQL array literal so it can
// be sent as a text parameter and cast with $n::text[].
func TextArray(xs []string) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, x := range xs {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('"')
		for _, r := range x {
			if r == '"' || r == '\\' {
				sb.WriteByte('\\')
			}
			sb.WriteRune(r)
		}
		sb.WriteByte('"')
	}
	sb.WriteByte('}')
	return sb.String()
}
