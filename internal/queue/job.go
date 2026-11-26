// Package queue implements a durable, Postgres-backed job queue.
//
// Guarantees:
//   - At-least-once execution. A job is retried with exponential backoff until
//     it succeeds or exhausts max_attempts, then moved to the dead state.
//   - Exactly one worker owns a running job at a time. Ownership is a lease
//     (locked_by + locked_until) kept alive by heartbeats; every state change a
//     worker makes is fenced on (locked_by, attempts) so a worker whose lease
//     was reclaimed can never overwrite the newer owner's result.
//   - Crashed workers are recovered by the reaper once their lease expires.
//   - Enqueue can share a transaction with your business writes (EnqueueTx),
//     so a job exists if and only if the surrounding transaction commits.
package queue

import (
	"database/sql"
	"encoding/json"
	"time"
)

type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

func (s State) Valid() bool {
	switch s {
	case StatePending, StateRunning, StateSucceeded, StateDead, StateCancelled:
		return true
	}
	return false
}

// Job is a row of conveyor_jobs.
type Job struct {
	ID          int64           `json:"id"`
	Queue       string          `json:"queue"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	State       State           `json:"state"`
	Priority    int             `json:"priority"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	RunAt       time.Time       `json:"run_at"`
	TimeoutMS   int64           `json:"timeout_ms"`
	UniqueKey   *string         `json:"unique_key,omitempty"`
	LockedBy    *string         `json:"locked_by,omitempty"`
	LockedUntil *time.Time      `json:"locked_until,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	LastError   *string         `json:"last_error,omitempty"`
	Errors      json.RawMessage `json:"errors"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`

	leaseToken string // set only on jobs handed to a worker
}

// Timeout is the per-attempt execution limit.
func (j *Job) Timeout() time.Duration { return time.Duration(j.TimeoutMS) * time.Millisecond }

// DecodePayload unmarshals the payload into v.
func (j *Job) DecodePayload(v any) error { return json.Unmarshal(j.Payload, v) }

const jobColumns = `id, queue, kind, payload, state, priority, attempts, max_attempts, run_at,
	timeout_ms, unique_key, locked_by, locked_until, result, last_error, errors,
	created_at, updated_at, started_at, finished_at`

type scanner interface{ Scan(dest ...any) error }

func scanJob(s scanner) (*Job, error) {
	var (
		j                                  Job
		payload, result, errs              []byte
		uniqueKey, lockedBy, lastError     sql.NullString
		lockedUntil, startedAt, finishedAt sql.NullTime
		state                              string
	)
	err := s.Scan(&j.ID, &j.Queue, &j.Kind, &payload, &state, &j.Priority, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.TimeoutMS, &uniqueKey, &lockedBy, &lockedUntil, &result, &lastError, &errs,
		&j.CreatedAt, &j.UpdatedAt, &startedAt, &finishedAt)
	if err != nil {
		return nil, err
	}
	j.State = State(state)
	j.Payload = payload
	j.Errors = errs
	if result != nil {
		j.Result = result
	}
	j.UniqueKey = nullStr(uniqueKey)
	j.LockedBy = nullStr(lockedBy)
	j.LastError = nullStr(lastError)
	j.LockedUntil = nullTime(lockedUntil)
	j.StartedAt = nullTime(startedAt)
	j.FinishedAt = nullTime(finishedAt)
	return &j, nil
}

func nullStr(n sql.NullString) *string {
	if !n.Valid {
		return nil
	}
	return &n.String
}

func nullTime(n sql.NullTime) *time.Time {
	if !n.Valid {
		return nil
	}
	return &n.Time
}
