package metrics

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var sb strings.Builder
	n, err := r.WriteTo(&sb)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if int(n) != len(sb.String()) {
		t.Fatalf("WriteTo reported %d bytes but wrote %d", n, len(sb.String()))
	}
	return sb.String()
}

func TestCounterAndGauge(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("requests_total", "Requests handled.", nil)
	g := r.Gauge("queue_depth", "Items waiting.", nil)

	c.Inc()
	c.Add(4)
	g.Set(10)
	g.Add(-3)

	if c.Value() != 5 || g.Value() != 7 {
		t.Fatalf("counter=%d gauge=%d", c.Value(), g.Value())
	}

	out := render(t, r)
	for _, want := range []string{
		"# HELP requests_total Requests handled.",
		"# TYPE requests_total counter",
		"requests_total 5",
		"# TYPE queue_depth gauge",
		"queue_depth 7",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestGaugeFuncIsReadAtScrapeTime(t *testing.T) {
	// The point of GaugeFunc is that there is one source of truth. A value
	// that changes between scrapes must be reflected without anyone having
	// remembered to update a mirror of it.
	r := NewRegistry()
	value := 1.0
	r.GaugeFunc("term", "Current term.", nil, func() float64 { return value })

	if !strings.Contains(render(t, r), "term 1") {
		t.Fatal("first scrape did not read the function")
	}
	value = 42
	if !strings.Contains(render(t, r), "term 42") {
		t.Fatal("second scrape returned a stale value")
	}
}

func TestSeriesWithLabelsShareOneHelpBlock(t *testing.T) {
	// The exposition format allows one HELP and TYPE line per metric name, not
	// per series, so several labelled series have to be grouped.
	r := NewRegistry()
	r.Counter("messages_total", "Messages.", Labels{"peer": "2", "direction": "out"}).Add(3)
	r.Counter("messages_total", "Messages.", Labels{"peer": "3", "direction": "out"}).Add(7)

	out := render(t, r)
	if strings.Count(out, "# HELP messages_total") != 1 {
		t.Fatalf("HELP repeated:\n%s", out)
	}
	if strings.Count(out, "# TYPE messages_total") != 1 {
		t.Fatalf("TYPE repeated:\n%s", out)
	}
	if !strings.Contains(out, `messages_total{direction="out",peer="2"} 3`) {
		t.Fatalf("missing the first series:\n%s", out)
	}
	if !strings.Contains(out, `messages_total{direction="out",peer="3"} 7`) {
		t.Fatalf("missing the second series:\n%s", out)
	}
}

func TestOutputIsStable(t *testing.T) {
	// Labels come from a map, and two scrapes of identical state have to
	// produce identical bytes or every diff of a metrics dump is noise.
	r := NewRegistry()
	r.Counter("b_total", "B.", Labels{"z": "1", "a": "2", "m": "3"}).Inc()
	r.Counter("a_total", "A.", nil).Inc()

	first := render(t, r)
	for i := 0; i < 20; i++ {
		if got := render(t, r); got != first {
			t.Fatalf("scrape %d differs:\n%s\n---\n%s", i, first, got)
		}
	}
	// Names are sorted, so a_total comes first.
	if strings.Index(first, "a_total") > strings.Index(first, "b_total") {
		t.Fatalf("series are not sorted by name:\n%s", first)
	}
	// And labels within a series are sorted too.
	if !strings.Contains(first, `b_total{a="2",m="3",z="1"} 1`) {
		t.Fatalf("labels are not sorted:\n%s", first)
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	r := NewRegistry()
	r.Gauge("thing", "Thing.", Labels{"path": `C:\a "quoted"` + "\nvalue"}).Set(1)

	out := render(t, r)
	if !strings.Contains(out, `path="C:\\a \"quoted\"\nvalue"`) {
		t.Fatalf("label was not escaped:\n%s", out)
	}
}

func TestFractionalValuesKeepTheirPrecision(t *testing.T) {
	r := NewRegistry()
	r.GaugeFunc("ratio", "A ratio.", nil, func() float64 { return 0.25 })
	r.GaugeFunc("whole", "A whole number.", nil, func() float64 { return 7 })

	out := render(t, r)
	if !strings.Contains(out, "ratio 0.25") {
		t.Fatalf("fractional value:\n%s", out)
	}
	// Whole numbers print without a decimal point, which is what almost every
	// metric here is.
	if !strings.Contains(out, "whole 7\n") {
		t.Fatalf("whole value:\n%s", out)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("no space left") }

func TestWriteErrorsSurface(t *testing.T) {
	r := NewRegistry()
	r.Counter("x_total", "X.", nil).Inc()
	if _, err := r.WriteTo(failingWriter{}); err == nil {
		t.Fatal("a failing writer produced no error")
	}
}

func TestEmptyRegistry(t *testing.T) {
	if got := render(t, NewRegistry()); got != "" {
		t.Fatalf("empty registry rendered %q", got)
	}
}

func TestConcurrentUse(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("hits_total", "Hits.", nil)
	g := r.Gauge("depth", "Depth.", nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				c.Inc()
				g.Add(1)
				if j%50 == 0 {
					var sb strings.Builder
					if _, err := r.WriteTo(&sb); err != nil {
						t.Errorf("WriteTo: %v", err)
						return
					}
				}
			}
		}()
	}
	// Registering while scraping happens whenever a member joins mid-flight.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			r.Counter("late_total", "Registered late.", Labels{"n": "x"}).Inc()
		}
	}()
	wg.Wait()

	if c.Value() != 1600 {
		t.Fatalf("counter = %d, want 1600", c.Value())
	}
}

func TestCounterFuncIsTypedAsACounter(t *testing.T) {
	// A _total that declares itself a gauge makes every rate() over it quietly
	// wrong, so a total maintained elsewhere still has to be typed correctly.
	r := NewRegistry()
	total := uint64(0)
	r.CounterFunc("bytes_sent_total", "Bytes sent.", nil, func() float64 { return float64(total) })

	total = 900
	out := render(t, r)
	if !strings.Contains(out, "# TYPE bytes_sent_total counter") {
		t.Fatalf("wrong type:\n%s", out)
	}
	if !strings.Contains(out, "bytes_sent_total 900") {
		t.Fatalf("wrong value:\n%s", out)
	}
}
