package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("jobs_total", "Jobs.", "queue", "outcome")
	c.Inc("default", "ok")
	c.Add(2, "default", "ok")
	c.Inc(`we"ird`, "fail")
	h := r.NewHistogramVec("latency_seconds", "Latency.", []float64{0.1, 1}, "queue")
	h.Observe(0.05, "default")
	h.Observe(0.5, "default")
	h.Observe(5, "default")

	var buf bytes.Buffer
	r.Write(&buf)
	out := buf.String()
	for _, want := range []string{
		"# TYPE jobs_total counter",
		`jobs_total{queue="default",outcome="ok"} 3`,
		`jobs_total{queue="we\"ird",outcome="fail"} 1`,
		`latency_seconds_bucket{queue="default",le="0.1"} 1`,
		`latency_seconds_bucket{queue="default",le="1"} 2`,
		`latency_seconds_bucket{queue="default",le="+Inf"} 3`,
		`latency_seconds_sum{queue="default"} 5.55`,
		`latency_seconds_count{queue="default"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}
