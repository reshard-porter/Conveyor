// Package metrics is a tiny Prometheus text-format exporter: labelled
// counters, gauges and histograms, plus on-scrape collector callbacks.
// It covers what this service needs without pulling in client_golang.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Registry struct {
	mu         sync.Mutex
	families   []family
	collectors []func(w io.Writer)
}

type family interface {
	write(w io.Writer)
}

func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) register(f family) {
	r.mu.Lock()
	r.families = append(r.families, f)
	r.mu.Unlock()
}

// Collect registers a callback invoked on every scrape, for values that are
// cheaper to compute on demand (like queue depth) than to track continuously.
func (r *Registry) Collect(fn func(w io.Writer)) {
	r.mu.Lock()
	r.collectors = append(r.collectors, fn)
	r.mu.Unlock()
}

func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	fams := append([]family(nil), r.families...)
	cols := append([]func(w io.Writer){}, r.collectors...)
	r.mu.Unlock()
	for _, f := range fams {
		f.write(w)
	}
	for _, c := range cols {
		c(w)
	}
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.Write(w)
	})
}

// ---- vectors keyed by label values ----

type vec[T any] struct {
	name, help, typ string
	labels          []string
	mu              sync.Mutex
	series          map[string]*T
	newT            func() *T
}

func (v *vec[T]) get(values []string) *T {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d labels, got %d", v.name, len(v.labels), len(values)))
	}
	key := strings.Join(values, "\xff")
	v.mu.Lock()
	defer v.mu.Unlock()
	s, ok := v.series[key]
	if !ok {
		s = v.newT()
		v.series[key] = s
	}
	return s
}

func (v *vec[T]) sortedKeys() []string {
	keys := make([]string, 0, len(v.series))
	for k := range v.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func labelString(names []string, key string, extra ...string) string {
	var values []string
	if len(names) > 0 {
		values = strings.Split(key, "\xff")
	}
	pairs := make([]string, 0, len(names)+1)
	for i, n := range names {
		pairs = append(pairs, n+`="`+escape(values[i])+`"`)
	}
	for i := 0; i+1 < len(extra); i += 2 {
		pairs = append(pairs, extra[i]+`="`+escape(extra[i+1])+`"`)
	}
	if len(pairs) == 0 {
		return ""
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

// ---- counter / gauge ----

type value struct {
	mu sync.Mutex
	v  float64
}

type CounterVec struct{ vec[value] }

func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{vec[value]{name: name, help: help, typ: "counter", labels: labels,
		series: map[string]*value{}, newT: func() *value { return &value{} }}}
	r.register(c)
	return c
}

func (c *CounterVec) Add(delta float64, labels ...string) {
	s := c.get(labels)
	s.mu.Lock()
	s.v += delta
	s.mu.Unlock()
}

func (c *CounterVec) Inc(labels ...string) { c.Add(1, labels...) }

func (c *CounterVec) write(w io.Writer) { writeValues(w, &c.vec) }

type GaugeVec struct{ vec[value] }

func (r *Registry) NewGaugeVec(name, help string, labels ...string) *GaugeVec {
	g := &GaugeVec{vec[value]{name: name, help: help, typ: "gauge", labels: labels,
		series: map[string]*value{}, newT: func() *value { return &value{} }}}
	r.register(g)
	return g
}

func (g *GaugeVec) Add(delta float64, labels ...string) {
	s := g.get(labels)
	s.mu.Lock()
	s.v += delta
	s.mu.Unlock()
}

func (g *GaugeVec) Set(v float64, labels ...string) {
	s := g.get(labels)
	s.mu.Lock()
	s.v = v
	s.mu.Unlock()
}

func (g *GaugeVec) write(w io.Writer) { writeValues(w, &g.vec) }

func writeValues(w io.Writer, v *vec[value]) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", v.name, v.help, v.name, v.typ)
	for _, k := range v.sortedKeys() {
		s := v.series[k]
		s.mu.Lock()
		fmt.Fprintf(w, "%s%s %s\n", v.name, labelString(v.labels, k), fmtFloat(s.v))
		s.mu.Unlock()
	}
}

// ---- histogram ----

type histo struct {
	mu      sync.Mutex
	buckets []float64
	counts  []uint64
	sum     float64
	count   uint64
}

type HistogramVec struct{ vec[histo] }

var DefaultBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if buckets == nil {
		buckets = DefaultBuckets
	}
	h := &HistogramVec{vec[histo]{name: name, help: help, typ: "histogram", labels: labels,
		series: map[string]*histo{},
		newT:   func() *histo { return &histo{buckets: buckets, counts: make([]uint64, len(buckets))} }}}
	r.register(h)
	return h
}

func (h *HistogramVec) Observe(v float64, labels ...string) {
	s := h.get(labels)
	s.mu.Lock()
	for i, b := range s.buckets {
		if v <= b {
			s.counts[i]++
		}
	}
	s.sum += v
	s.count++
	s.mu.Unlock()
}

func (h *HistogramVec) write(w io.Writer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
	for _, k := range h.sortedKeys() {
		s := h.series[k]
		s.mu.Lock()
		for i, b := range s.buckets {
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelString(h.labels, k, "le", fmtFloat(b)), s.counts[i])
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelString(h.labels, k, "le", "+Inf"), s.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, labelString(h.labels, k), fmtFloat(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labelString(h.labels, k), s.count)
		s.mu.Unlock()
	}
}

func fmtFloat(f float64) string {
	if math.IsInf(f, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// WriteGauge is a helper for collectors emitting a single gauge sample.
func WriteGauge(w io.Writer, name string, v float64, labelPairs ...string) {
	fmt.Fprintf(w, "%s%s %s\n", name, labelString(nil, "", labelPairs...), fmtFloat(v))
}
