package queue

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

var (
	ErrNotFound     = errors.New("job not found")
	ErrInvalidState = errors.New("job is not in a state that allows this operation")
)

// permanentError marks a failure that retrying cannot fix (bad input, 4xx).
type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the job goes straight to the dead state without
// using up its remaining attempts.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// snoozeError reschedules a job without counting the attempt, e.g. when a
// downstream rate limit says "come back in 30s".
type snoozeError struct{ d time.Duration }

func (e *snoozeError) Error() string { return fmt.Sprintf("snoozed for %s", e.d) }

func Snooze(d time.Duration) error { return &snoozeError{d: d} }

// BackoffFunc returns the delay before the next attempt, given the number of
// attempts already made (>= 1).
type BackoffFunc func(attempt int) time.Duration

// ExponentialBackoff doubles from base up to max and applies "equal jitter"
// (a random value in [d/2, d]) so a burst of failures doesn't retry in lockstep.
func ExponentialBackoff(base, max time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := base
		for i := 1; i < attempt && d < max; i++ {
			d *= 2
		}
		if d > max {
			d = max
		}
		half := d / 2
		return half + time.Duration(rand.Int64N(int64(half)+1))
	}
}

// DefaultBackoff: ~1s, 2s, 4s ... capped at 1h.
var DefaultBackoff = ExponentialBackoff(time.Second, time.Hour)
