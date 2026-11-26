package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/reshard-porter/conveyor/internal/queue"
)

func job(t *testing.T, payload any) *queue.Job {
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &queue.Job{ID: 7, Attempts: 1, Payload: b}
}

func TestWebhookClassifiesResponses(t *testing.T) {
	var gotKey, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(204)
		case "/500":
			w.WriteHeader(500)
		case "/429":
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(429)
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()
	h := DeliverWebhook(srv.Client())
	run := func(path string) error {
		_, err := h(context.Background(), job(t, map[string]any{"url": srv.URL + path, "body": map[string]int{"x": 1}}))
		return err
	}

	if err := run("/ok"); err != nil {
		t.Fatalf("ok: %v", err)
	}
	if gotKey != "conveyor-7" || gotBody != `{"x":1}` {
		t.Fatalf("key=%q body=%q", gotKey, gotBody)
	}
	if err := run("/500"); err == nil || queue.IsPermanent(err) {
		t.Fatalf("500 should be retryable: %v", err)
	}
	if err := run("/404"); !queue.IsPermanent(err) {
		t.Fatalf("404 should be permanent: %v", err)
	}
	if err := run("/429"); err == nil || err.Error() != "snoozed for 7s" {
		t.Fatalf("429 should snooze: %v", err)
	}
	if _, err := h(context.Background(), job(t, map[string]any{"url": "ftp://nope"})); !queue.IsPermanent(err) {
		t.Fatalf("bad url should be permanent: %v", err)
	}
}

func TestEmailRejectsBadRecipient(t *testing.T) {
	_, err := SendEmail(context.Background(), job(t, map[string]string{"to": "not-an-email"}))
	if !queue.IsPermanent(err) {
		t.Fatalf("got %v", err)
	}
}

func TestReportHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := GenerateReport(ctx, job(t, map[string]int{"rows": 5000, "ms_per_chunk": 1000}))
	if err == nil {
		t.Fatal("expected interruption")
	}
}
