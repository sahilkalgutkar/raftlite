// Package metrics is a small Prometheus-compatible registry.
//
// raftlite has no third-party dependencies, and I would rather keep it that
// way than take one for a text format that is a dozen lines to emit. The
// exposition format is stable, documented and trivial: a help line, a type
// line, and a value per series. What a client library adds beyond that --
// histograms with configurable buckets, exemplars, a push gateway -- is more
// than this project needs, and the cost would be paid by everyone building it.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// kind is the Prometheus metric type.
type kind string

const (
	counterKind kind = "counter"
	gaugeKind   kind = "gauge"
)

// Labels are a metric's dimensions.
type Labels map[string]string

// render formats labels in the exposition format, sorted so the output of two
// scrapes of identical state is byte-identical.
func (l Labels) render() string {
	if len(l) == 0 {
		return ""
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		// The quoting is done by hand rather than with %q, which would
		// escape the escapes.
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, escape(l[k])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// escape handles the three characters the format reserves inside a label
// value.
func escape(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return v
}

// Counter only ever goes up.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add increases the counter by n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value reads the counter.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Gauge can move in either direction.
type Gauge struct{ v atomic.Int64 }

// Set replaces the value.
func (g *Gauge) Set(v int64) { g.v.Store(v) }

// Add changes the value by delta.
func (g *Gauge) Add(delta int64) { g.v.Add(delta) }

// Value reads the gauge.
func (g *Gauge) Value() int64 { return g.v.Load() }

// series is one exported time series.
type series struct {
	name   string
	help   string
	kind   kind
	labels Labels
	read   func() float64
}

// Registry holds the metrics a process exports.
type Registry struct {
	mu     sync.Mutex
	series []series
	seen   map[string]string // name -> help, so duplicates stay consistent
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{seen: make(map[string]string)}
}

// Counter registers and returns a counter.
func (r *Registry) Counter(name, help string, labels Labels) *Counter {
	c := &Counter{}
	r.register(series{name: name, help: help, kind: counterKind, labels: labels,
		read: func() float64 { return float64(c.Value()) }})
	return c
}

// Gauge registers and returns a gauge.
func (r *Registry) Gauge(name, help string, labels Labels) *Gauge {
	g := &Gauge{}
	r.register(series{name: name, help: help, kind: gaugeKind, labels: labels,
		read: func() float64 { return float64(g.Value()) }})
	return g
}

// GaugeFunc registers a gauge whose value is read at scrape time.
//
// Most of raftlite's interesting numbers -- the term, the commit index, how
// many keys the state machine holds -- already exist somewhere authoritative.
// Mirroring them into a gauge on every change would mean two sources of truth
// that can disagree; reading them when asked cannot.
func (r *Registry) GaugeFunc(name, help string, labels Labels, fn func() float64) {
	r.register(series{name: name, help: help, kind: gaugeKind, labels: labels, read: fn})
}

// CounterFunc registers a counter whose value is read at scrape time, for a
// total that some other component already maintains. It exists so a value that
// only ever increases is typed as a counter rather than a gauge -- the _total
// suffix and the declared type have to agree, or every rate() over it is
// quietly wrong.
func (r *Registry) CounterFunc(name, help string, labels Labels, fn func() float64) {
	r.register(series{name: name, help: help, kind: counterKind, labels: labels, read: fn})
}

func (r *Registry) register(s series) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen[s.name] = s.help
	r.series = append(r.series, s)
}

// WriteTo renders the registry in the Prometheus text exposition format.
// Series are grouped by name with a single HELP and TYPE line each, which the
// format requires, and sorted so two scrapes of identical state produce
// identical bytes.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	snapshot := make([]series, len(r.series))
	copy(snapshot, r.series)
	r.mu.Unlock()

	sort.SliceStable(snapshot, func(i, j int) bool {
		if snapshot[i].name != snapshot[j].name {
			return snapshot[i].name < snapshot[j].name
		}
		return snapshot[i].labels.render() < snapshot[j].labels.render()
	})

	var (
		buf      strings.Builder
		lastName string
	)
	for _, s := range snapshot {
		if s.name != lastName {
			fmt.Fprintf(&buf, "# HELP %s %s\n", s.name, s.help)
			fmt.Fprintf(&buf, "# TYPE %s %s\n", s.name, s.kind)
			lastName = s.name
		}
		fmt.Fprintf(&buf, "%s%s %s\n", s.name, s.labels.render(), formatValue(s.read()))
	}

	n, err := io.WriteString(w, buf.String())
	return int64(n), err
}

// formatValue prints whole numbers without a decimal point, which is what
// almost every raftlite metric is.
func formatValue(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
