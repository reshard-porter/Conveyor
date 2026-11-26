package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/reshard-porter/conveyor/internal/metrics"
	"github.com/reshard-porter/conveyor/internal/migrate"
	"github.com/reshard-porter/conveyor/internal/pgwire"
)

func setup(t *testing.T, apiKey string) (*httptest.Server, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("CONVEYOR_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CONVEYOR_TEST_DATABASE_URL not set")
	}
	db, err := sql.Open(pgwire.DriverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if _, err := migrate.Up(context.Background(), db, log); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`TRUNCATE conveyor_jobs, conveyor_paused_queues RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(Config{DB: db, Logger: log, APIKey: apiKey, Registry: metrics.NewRegistry()}).Handler())
	t.Cleanup(srv.Close)
	return srv, db
}

type client struct {
	t    *testing.T
	base string
	key  string
}

func (c client) do(method, path, body string) (int, map[string]any) {
	c.t.Helper()
	req, _ := http.NewRequest(method, c.base+path, strings.NewReader(body))
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func TestJobLifecycleOverHTTP(t *testing.T) {
	srv, _ := setup(t, "")
	c := client{t: t, base: srv.URL}

	code, body := c.do("POST", "/v1/jobs", `{"kind":"email.send","queue":"emails","payload":{"to":"a@b.c"},"unique_key":"welcome:1","delay_seconds":3600}`)
	if code != 201 || body["created"] != true {
		t.Fatalf("create: %d %v", code, body)
	}
	job := body["job"].(map[string]any)
	id := int64(job["id"].(float64))
	if job["state"] != "pending" || job["queue"] != "emails" {
		t.Fatalf("job = %v", job)
	}

	code, body = c.do("POST", "/v1/jobs", `{"kind":"email.send","queue":"emails","unique_key":"welcome:1"}`)
	if code != 200 || body["created"] != false {
		t.Fatalf("dedupe: %d %v", code, body)
	}

	code, body = c.do("GET", fmt.Sprintf("/v1/jobs/%d", id), "")
	if code != 200 || body["job"].(map[string]any)["kind"] != "email.send" {
		t.Fatalf("get: %d %v", code, body)
	}

	code, body = c.do("GET", "/v1/queues", "")
	queues := body["queues"].([]any)
	if code != 200 || len(queues) != 1 || queues[0].(map[string]any)["scheduled"].(float64) != 1 {
		t.Fatalf("stats: %d %v", code, body)
	}

	code, _ = c.do("POST", fmt.Sprintf("/v1/jobs/%d/cancel", id), "")
	if code != 200 {
		t.Fatalf("cancel: %d", code)
	}
	code, _ = c.do("POST", fmt.Sprintf("/v1/jobs/%d/cancel", id), "")
	if code != 409 {
		t.Fatalf("double cancel should conflict, got %d", code)
	}
	code, body = c.do("POST", fmt.Sprintf("/v1/jobs/%d/retry", id), "")
	if code != 200 || body["job"].(map[string]any)["state"] != "pending" {
		t.Fatalf("retry: %d %v", code, body)
	}
}

func TestValidationAndErrors(t *testing.T) {
	srv, _ := setup(t, "")
	c := client{t: t, base: srv.URL}
	for _, tc := range []struct {
		method, path, body string
		want               int
	}{
		{"POST", "/v1/jobs", `{"kind":""}`, 400},
		{"POST", "/v1/jobs", `{"kind":"x","priority":500}`, 400},
		{"POST", "/v1/jobs", `{"kind":"x","unknown_field":1}`, 400},
		{"POST", "/v1/jobs", `not json`, 400},
		{"GET", "/v1/jobs/999999", "", 404},
		{"GET", "/v1/jobs/abc", "", 400},
		{"GET", "/v1/jobs?state=nope", "", 400},
		{"POST", "/v1/jobs/batch", `{"jobs":[]}`, 400},
		{"DELETE", "/v1/jobs/1", "", 405},
	} {
		if code, body := c.do(tc.method, tc.path, tc.body); code != tc.want {
			t.Errorf("%s %s %s: got %d want %d (%v)", tc.method, tc.path, tc.body, code, tc.want, body)
		}
	}
}

func TestBatchListAndPagination(t *testing.T) {
	srv, _ := setup(t, "")
	c := client{t: t, base: srv.URL}
	var jobs []string
	for i := range 5 {
		jobs = append(jobs, fmt.Sprintf(`{"kind":"demo.flaky","payload":{"n":%d}}`, i))
	}
	code, body := c.do("POST", "/v1/jobs/batch", `{"jobs":[`+strings.Join(jobs, ",")+`]}`)
	if code != 201 || body["inserted"].(float64) != 5 {
		t.Fatalf("batch: %d %v", code, body)
	}
	code, body = c.do("GET", "/v1/jobs?limit=2", "")
	if code != 200 || len(body["jobs"].([]any)) != 2 || body["next_before"].(float64) != 4 {
		t.Fatalf("page 1: %d %v", code, body)
	}
	code, body = c.do("GET", "/v1/jobs?limit=2&before=2", "")
	if code != 200 || len(body["jobs"].([]any)) != 1 || body["next_before"] != nil {
		t.Fatalf("last page: %d %v", code, body)
	}
}

func TestAPIKeyAuth(t *testing.T) {
	srv, _ := setup(t, "s3cret")
	if code, _ := (client{t: t, base: srv.URL}).do("GET", "/v1/queues", ""); code != 401 {
		t.Fatalf("no key: %d", code)
	}
	if code, _ := (client{t: t, base: srv.URL, key: "wrong"}).do("GET", "/v1/queues", ""); code != 401 {
		t.Fatalf("wrong key: %d", code)
	}
	if code, _ := (client{t: t, base: srv.URL, key: "s3cret"}).do("GET", "/v1/queues", ""); code != 200 {
		t.Fatalf("right key: %d", code)
	}
	// Health checks stay open for load balancers.
	if code, _ := (client{t: t, base: srv.URL}).do("GET", "/healthz", ""); code != 200 {
		t.Fatalf("healthz: %d", code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv, _ := setup(t, "")
	c := client{t: t, base: srv.URL}
	c.do("POST", "/v1/jobs", `{"kind":"x","queue":"metrics-q"}`)
	c.do("POST", "/v1/queues/metrics-q/pause", "")
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	for _, want := range []string{
		`conveyor_http_requests_total{route="/v1/jobs",status="201"} 1`,
		`conveyor_queue_jobs{queue="metrics-q",state="ready"} 1`,
		`conveyor_queue_oldest_ready_seconds{queue="metrics-q"}`,
		`conveyor_http_request_duration_seconds_bucket{route="/v1/jobs",le="+Inf"} 1`,
	} {
		if !bytes.Contains(raw, []byte(want)) {
			t.Errorf("metrics missing %q\n%s", want, raw)
		}
	}
	if resp.Header.Get("X-Request-Id") == "" {
		t.Error("missing X-Request-Id")
	}
}

func TestDashboardIsServed(t *testing.T) {
	srv, _ := setup(t, "s3cret")
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), "text/html") ||
		!bytes.Contains(body, []byte("Conveyor")) {
		t.Fatalf("dashboard: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if r, _ := http.Get(srv.URL + "/nope"); r.StatusCode != 404 {
		t.Fatalf("unknown path should 404, got %d", r.StatusCode)
	}
}
