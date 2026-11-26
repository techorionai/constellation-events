package lag_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/techorionai/constellation-platform-go/executor"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/techorionai/constellation-events/internal/app/queries"
	"github.com/techorionai/constellation-events/internal/handlers/worker/lag"
	"github.com/techorionai/constellation-events/resources/test/mock"
)

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) count(msg string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range strings.Split(l.b.String(), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["msg"] == msg {
			n++
		}
	}
	return n
}

type harness struct {
	w      *lag.Worker
	reader *sdkmetric.ManualReader
	logs   *logBuf
	now    time.Time
	mu     sync.Mutex
	res    queries.CollectConsumerLagResult
	calls  int
}

func newHarness(t *testing.T, interval time.Duration) *harness {
	t.Helper()
	h := &harness{reader: sdkmetric.NewManualReader(), logs: &logBuf{}, now: time.Unix(1000, 0)}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(h.reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	collect := executor.FuncWithReturn[queries.CollectConsumerLag, queries.CollectConsumerLagResult](
		func(_ context.Context, _ queries.CollectConsumerLag) (queries.CollectConsumerLagResult, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.calls++
			return h.res, nil
		})
	w, err := lag.New(lag.Config{
		Collect: collect, Interval: interval, Meter: mp.Meter("events"),
		Log: slog.New(slog.NewJSONHandler(h.logs, nil)),
		Now: func() time.Time { return h.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.w = w
	return h
}

func (h *harness) set(res queries.CollectConsumerLagResult) {
	h.mu.Lock()
	h.res = res
	h.mu.Unlock()
}

func (h *harness) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func values(stream string, groups ...string) []queries.LagValue {
	var out []queries.LagValue
	for _, g := range groups {
		out = append(out, queries.LagValue{Stream: stream, Group: g, Lag: 2 * time.Second})
	}
	return out
}

func fail(op, stream, group string) queries.LagFailure {
	return queries.LagFailure{Operation: op, Stream: stream, Group: group, Err: errors.New("boom")}
}

// snap is the content of the instruments.
type snap struct {
	lags      map[string]float64
	collected *int64
	groups    *int64
	errors    map[string]int64
}

func readMetrics(t *testing.T, reader *sdkmetric.ManualReader) snap {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	s := snap{lags: map[string]float64{}, errors: map[string]int64{}}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[float64]:
				for _, p := range d.DataPoints {
					st, _ := p.Attributes.Value("stream")
					gr, _ := p.Attributes.Value("group")
					s.lags[st.AsString()+"/"+gr.AsString()] = p.Value
				}
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					v := p.Value
					if m.Name == lag.MetricCollected {
						s.collected = &v
					} else {
						s.groups = &v
					}
				}
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					op, _ := p.Attributes.Value("operation")
					s.errors[op.AsString()] += p.Value
				}
			}
		}
	}
	return s
}

func (h *harness) read(t *testing.T) snap { return readMetrics(t, h.reader) }

// U6: a failed Groups keeps the last values of the stream for 5 collections.
func TestU6_GroupsFailure(t *testing.T) {
	h := newHarness(t, time.Second)
	ctx := context.Background()
	h.set(queries.CollectConsumerLagResult{Values: append(values("alpha", "a1", "a2"), values("beta", "b1")...), GroupsFound: 3})
	h.w.Collect(ctx)
	h.set(queries.CollectConsumerLagResult{Values: values("beta", "b1"), GroupsFound: 1, Failures: []queries.LagFailure{fail("groups", "alpha", "")}})
	for i := 1; i <= 5; i++ {
		h.w.Collect(ctx)
		s := h.read(t)
		if len(s.lags) != 3 {
			t.Fatalf("collection %d: lags = %v, want 3 values", i, s.lags)
		}
		if s.errors["groups"] != int64(i) {
			t.Fatalf("collection %d: errors = %v", i, s.errors)
		}
	}
	h.w.Collect(ctx)
	s := h.read(t)
	if len(s.lags) != 1 || s.lags["beta/b1"] != 2 || s.errors["groups"] != 6 {
		t.Fatalf("sixth collection: lags = %v, errors = %v", s.lags, s.errors)
	}
	if n := h.logs.count("events.lag.collect_failed"); n != 6 {
		t.Fatalf("warn logs = %d, want 6", n)
	}
}

// U7: a failed ListApps keeps all values for 5 collections. The groups gauge
// keeps its value and the collection time does not change.
func TestU7_AppsFailure(t *testing.T) {
	h := newHarness(t, time.Second)
	ctx := context.Background()
	h.set(queries.CollectConsumerLagResult{Values: values("alpha", "a1", "a2"), GroupsFound: 2})
	h.w.Collect(ctx)
	first := h.read(t)
	h.now = h.now.Add(time.Hour)
	h.set(queries.CollectConsumerLagResult{Failures: []queries.LagFailure{fail("apps", "", "")}})
	for i := 1; i <= 5; i++ {
		h.w.Collect(ctx)
		if s := h.read(t); len(s.lags) != 2 {
			t.Fatalf("collection %d: lags = %v", i, s.lags)
		}
	}
	h.w.Collect(ctx)
	s := h.read(t)
	if len(s.lags) != 0 || s.groups == nil || *s.groups != 2 || s.errors["apps"] != 6 {
		t.Fatalf("lags = %v, groups = %v, errors = %v", s.lags, s.groups, s.errors)
	}
	if s.collected == nil || *s.collected != *first.collected {
		t.Fatalf("collected changed: %v, was %v", s.collected, first.collected)
	}
}

// U8: the gauges have no value before the first collection. The collection time
// is the time of the first full collection, and a failure does not change it.
func TestU8_Collected(t *testing.T) {
	h := newHarness(t, time.Second)
	ctx := context.Background()
	s := h.read(t)
	if s.collected != nil || s.groups != nil || len(s.lags) != 0 {
		t.Fatalf("before the first collection: %+v", s)
	}
	h.set(queries.CollectConsumerLagResult{Values: values("alpha", "a1"), GroupsFound: 1})
	h.w.Collect(ctx)
	s = h.read(t)
	if s.collected == nil || *s.collected != 1000 {
		t.Fatalf("collected = %v", s.collected)
	}
	h.now = h.now.Add(time.Minute)
	h.set(queries.CollectConsumerLagResult{GroupsFound: 1, Failures: []queries.LagFailure{fail("lag", "alpha", "a1")}})
	h.w.Collect(ctx)
	s = h.read(t)
	if *s.collected != 1000 || s.errors["lag"] != 1 {
		t.Fatalf("collected = %d, errors = %v", *s.collected, s.errors)
	}
}

// U8 (start): Run does the first collection with no tick.
func TestU8_RunCollectsAtStart(t *testing.T) {
	h := newHarness(t, time.Hour)
	h.set(queries.CollectConsumerLagResult{Values: values("alpha", "a1"), GroupsFound: 1})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.w.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for h.read(t).collected == nil {
		if time.Now().After(deadline) {
			t.Fatal("no collection at start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

// U9: the callbacks read the snapshot and make no call to a port.
func TestU9_CallbacksReadSnapshot(t *testing.T) {
	h := newHarness(t, time.Second)
	h.set(queries.CollectConsumerLagResult{Values: values("alpha", "a1"), GroupsFound: 1})
	h.w.Collect(context.Background())
	before := h.callCount()
	for range 3 {
		_ = h.read(t)
	}
	if got := h.callCount(); got != before {
		t.Fatalf("collector calls changed from %d to %d during the callbacks", before, got)
	}
}

// U10: the cap log record is written when the count of dropped groups changes.
func TestU10_CapLog(t *testing.T) {
	h := newHarness(t, time.Second)
	ctx := context.Background()
	step := func(found int) int64 {
		h.set(queries.CollectConsumerLagResult{GroupsFound: found, GroupsCapped: max(found-1000, 0)})
		h.w.Collect(ctx)
		return *h.read(t).groups
	}
	for range 3 {
		if got := step(1001); got != 1001 {
			t.Fatalf("groups = %d", got)
		}
	}
	if n := h.logs.count("events.lag.groups_capped"); n != 1 {
		t.Fatalf("logs after 3 collections = %d, want 1", n)
	}
	if got := step(1002); got != 1002 || h.logs.count("events.lag.groups_capped") != 2 {
		t.Fatalf("groups = %d, logs = %d", got, h.logs.count("events.lag.groups_capped"))
	}
	if got := step(900); got != 900 || h.logs.count("events.lag.groups_capped") != 2 {
		t.Fatalf("groups = %d, logs = %d", got, h.logs.count("events.lag.groups_capped"))
	}
}

func newRealWorker(t *testing.T, apps *mock.Registrations, st *mock.Streams, interval time.Duration) (*lag.Worker, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	w, err := lag.New(lag.Config{
		Collect:  queries.NewCollectConsumerLag(apps, st),
		Interval: interval, Meter: mp.Meter("events"),
		Log: slog.New(slog.NewJSONHandler(&logBuf{}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return w, reader
}

// U11: a blocked Lag call ends at the interval with one failure, and the next
// collection starts.
func TestU11_CollectionTimeLimit(t *testing.T) {
	apps := &mock.Registrations{ListAppsFunc: func(context.Context) ([]string, error) { return []string{"alpha"}, nil }}
	var mu sync.Mutex
	lagCalls := 0
	st := &mock.Streams{
		GroupsFunc: func(context.Context, string) ([]string, error) { return []string{"a1"}, nil },
		LagFunc: func(ctx context.Context, _, _ string) (time.Duration, error) {
			mu.Lock()
			lagCalls++
			mu.Unlock()
			<-ctx.Done()
			return 0, ctx.Err()
		},
	}
	w, reader := newRealWorker(t, apps, st, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := lagCalls
		mu.Unlock()
		if n >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lag calls = %d, want 2 or more", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if s := readMetrics(t, reader); s.errors["lag"] < 1 || s.collected != nil {
		t.Fatalf("errors = %v, collected = %v", s.errors, s.collected)
	}
}

// U12: Run returns when the context ends, and the mocks get no call after it.
func TestU12_Stop(t *testing.T) {
	apps := &mock.Registrations{ListAppsFunc: func(context.Context) ([]string, error) { return []string{"alpha"}, nil }}
	st := &mock.Streams{
		GroupsFunc: func(context.Context, string) ([]string, error) { return []string{"a1"}, nil },
		LagFunc:    func(context.Context, string, string) (time.Duration, error) { return time.Second, nil },
	}
	w, _ := newRealWorker(t, apps, st, 10*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return in 5 s")
	}
	n := apps.Calls() + st.Calls()
	time.Sleep(60 * time.Millisecond)
	if got := apps.Calls() + st.Calls(); got != n {
		t.Fatalf("calls after Run returned: %d, was %d", got, n)
	}
}

func TestNewRejectsBadConfig(t *testing.T) {
	mp := sdkmetric.NewMeterProvider()
	c := executor.FuncWithReturn[queries.CollectConsumerLag, queries.CollectConsumerLagResult](
		func(context.Context, queries.CollectConsumerLag) (queries.CollectConsumerLagResult, error) {
			return queries.CollectConsumerLagResult{}, nil
		})
	for name, cfg := range map[string]lag.Config{
		"no interval":  {Collect: c, Meter: mp.Meter("x")},
		"no collector": {Interval: time.Second, Meter: mp.Meter("x")},
		"no meter":     {Collect: c, Interval: time.Second},
	} {
		if _, err := lag.New(cfg); err == nil {
			t.Fatalf("%s: want an error", name)
		}
	}
}

// U15: a collection that lost a stream keeps the old count and the old cap log state.
func TestU15_PartialGroupsFailureKeepsCount(t *testing.T) {
	h := newHarness(t, time.Second)
	ctx := context.Background()
	h.set(queries.CollectConsumerLagResult{GroupsFound: 1200, GroupsCapped: 200, Values: values("alpha", "a1")})
	h.w.Collect(ctx)
	h.set(queries.CollectConsumerLagResult{
		GroupsFound: 10, Values: values("alpha", "a1"),
		Failures: []queries.LagFailure{fail(queries.OperationGroups, "beta", "")},
	})
	h.w.Collect(ctx)
	if got := *h.read(t).groups; got != 1200 {
		t.Fatalf("groups = %d, want 1200", got)
	}
	h.set(queries.CollectConsumerLagResult{GroupsFound: 1200, GroupsCapped: 200, Values: values("alpha", "a1")})
	h.w.Collect(ctx)
	if n := h.logs.count("events.lag.groups_capped"); n != 1 {
		t.Fatalf("cap logs = %d, want 1", n)
	}
}

// U16: a value with a name that is not valid UTF-8 is not in the instruments.
func TestU16_InvalidUTF8Name(t *testing.T) {
	h := newHarness(t, time.Second)
	h.set(queries.CollectConsumerLagResult{Values: values("alpha", "good", "bad\xff")})
	h.w.Collect(context.Background())
	s := h.read(t)
	if len(s.lags) != 1 || s.lags["alpha/good"] == 0 {
		t.Fatalf("lags = %v", s.lags)
	}
	if n := h.logs.count("events.lag.groups_skipped"); n != 1 {
		t.Fatalf("skip logs = %d, want 1", n)
	}
}

// U17: a failure that comes from a cancel of the parent context is not counted or logged.
func TestU17_ShutdownIsNotAFailure(t *testing.T) {
	h := newHarness(t, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.set(queries.CollectConsumerLagResult{Failures: []queries.LagFailure{{Operation: queries.OperationLag, Stream: "alpha", Group: "a1", Err: context.Canceled}}})
	h.w.Collect(ctx)
	if n := h.logs.count("events.lag.collect_failed"); n != 0 {
		t.Fatalf("failure logs = %d, want 0", n)
	}
	if e := h.read(t).errors; len(e) != 0 {
		t.Fatalf("errors = %v", e)
	}
}

// U18: parallel calls of Collect have no data race (run with -race in CI).
func TestU18_ParallelCollect(t *testing.T) {
	h := newHarness(t, time.Second)
	h.set(queries.CollectConsumerLagResult{GroupsFound: 1100, GroupsCapped: 100, Values: values("alpha", "a1")})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); h.w.Collect(context.Background()) }()
	}
	wg.Wait()
}

// U19: the worker refuses an interval of 0.
func TestU19_IntervalRequired(t *testing.T) {
	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	collect := executor.FuncWithReturn[queries.CollectConsumerLag, queries.CollectConsumerLagResult](
		func(context.Context, queries.CollectConsumerLag) (queries.CollectConsumerLagResult, error) {
			return queries.CollectConsumerLagResult{}, nil
		})
	if _, err := lag.New(lag.Config{Collect: collect, Meter: mp.Meter("events")}); err == nil {
		t.Fatal("want an error")
	}
}
