// Package handlers contains example job handlers that demonstrate the
// patterns a real service needs: transient vs permanent failures, honouring
// cancellation, outbound HTTP with timeouts, and returning results.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/reshard-porter/conveyor/internal/queue"
)

// Register attaches every example handler to w.
func Register(w *queue.Worker, client *http.Client) {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	w.Handle("email.send", SendEmail)
	w.Handle("webhook.deliver", DeliverWebhook(client))
	w.Handle("report.generate", GenerateReport)
	w.Handle("demo.flaky", Flaky)
	w.Handle("bench.noop", Noop)
}

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// SendEmail simulates an email provider: bad addresses fail permanently,
// and ~10% of sends hit a transient error that will be retried.
func SendEmail(ctx context.Context, job *queue.Job) (any, error) {
	var p EmailPayload
	if err := job.DecodePayload(&p); err != nil {
		return nil, queue.Permanent(fmt.Errorf("decode payload: %w", err))
	}
	if !strings.Contains(p.To, "@") {
		return nil, queue.Permanent(fmt.Errorf("invalid recipient %q", p.To))
	}
	if err := sleepCtx(ctx, time.Duration(20+rand.IntN(80))*time.Millisecond); err != nil {
		return nil, err
	}
	if rand.Float64() < 0.10 {
		return nil, errors.New("smtp: 421 service temporarily unavailable")
	}
	return map[string]any{"message_id": fmt.Sprintf("msg-%d-%d", job.ID, job.Attempts)}, nil
}

type WebhookPayload struct {
	URL     string            `json:"url"`
	Body    json.RawMessage   `json:"body"`
	Headers map[string]string `json:"headers"`
}

// DeliverWebhook POSTs a JSON body. 5xx, 429 and network errors are retried;
// other 4xx responses are permanent; 429 honours Retry-After via Snooze.
func DeliverWebhook(client *http.Client) queue.Handler {
	return func(ctx context.Context, job *queue.Job) (any, error) {
		var p WebhookPayload
		if err := job.DecodePayload(&p); err != nil {
			return nil, queue.Permanent(fmt.Errorf("decode payload: %w", err))
		}
		u, err := url.Parse(p.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, queue.Permanent(fmt.Errorf("invalid url %q", p.URL))
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL, bytes.NewReader(p.Body))
		if err != nil {
			return nil, queue.Permanent(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("conveyor-%d", job.ID))
		for k, v := range p.Headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("deliver: %w", err)
		}
		defer resp.Body.Close()
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		switch {
		case resp.StatusCode < 300:
			return map[string]any{"status": resp.StatusCode}, nil
		case resp.StatusCode == http.StatusTooManyRequests:
			if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs > 0 {
				return nil, queue.Snooze(time.Duration(secs) * time.Second)
			}
			return nil, fmt.Errorf("rate limited (429)")
		case resp.StatusCode >= 500:
			return nil, fmt.Errorf("server error %d: %s", resp.StatusCode, snippet)
		default:
			return nil, queue.Permanent(fmt.Errorf("rejected %d: %s", resp.StatusCode, snippet))
		}
	}
}

type ReportPayload struct {
	Rows       int `json:"rows"`
	MsPerChunk int `json:"ms_per_chunk"`
}

// GenerateReport is a long-running job that checks ctx between chunks, so
// cancellation, timeouts and graceful shutdown interrupt it promptly.
func GenerateReport(ctx context.Context, job *queue.Job) (any, error) {
	p := ReportPayload{Rows: 10_000, MsPerChunk: 50}
	_ = job.DecodePayload(&p)
	const chunk = 1000
	processed := 0
	for processed < p.Rows {
		if err := sleepCtx(ctx, time.Duration(p.MsPerChunk)*time.Millisecond); err != nil {
			return nil, fmt.Errorf("interrupted after %d rows: %w", processed, err)
		}
		processed = min(processed+chunk, p.Rows)
	}
	return map[string]any{"rows": processed}, nil
}

// Flaky fails the first fail_times attempts, then succeeds. Useful for
// watching retries and backoff in action.
func Flaky(_ context.Context, job *queue.Job) (any, error) {
	var p struct {
		FailTimes int `json:"fail_times"`
	}
	_ = job.DecodePayload(&p)
	if job.Attempts <= p.FailTimes {
		return nil, fmt.Errorf("simulated failure %d/%d", job.Attempts, p.FailTimes)
	}
	return map[string]any{"succeeded_on_attempt": job.Attempts}, nil
}

func Noop(context.Context, *queue.Job) (any, error) { return nil, nil }

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}
