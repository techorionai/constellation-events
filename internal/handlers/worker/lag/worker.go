// Package lag is the worker that collects the consumer lag. It runs the
// collection query at a fixed interval, keeps the last result as a snapshot,
// and reports the snapshot with OpenTelemetry instruments.
package lag

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/techorionai/constellation-platform-go/executor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/techorionai/constellation-events/internal/app/queries"
)

// Names of the instruments.
const (
	MetricLag       = "constellation.events.consumer.lag"
	MetricCollected = "constellation.events.lag.collected"
	MetricGroups    = "constellation.events.lag.groups"
	MetricErrors    = "constellation.events.lag.errors"
)

// MaxStale is the count of collections for which the snapshot keeps the last
// value of a group after an operation that affects the group failed.
const MaxStale = 5

// Collector runs one collection.
type Collector = executor.ExecutorWithReturn[queries.CollectConsumerLag, queries.CollectConsumerLagResult]

// Config is the config of the worker.
type Config struct {
	Collect  Collector
	Interval time.Duration
	Meter    metric.Meter
	Log      *slog.Logger
	// Now gives the time. It is time.Now if nil.
	Now func() time.Time
}

type groupKey struct{ stream, group string }

type entry struct {
	lag   float64
	stale int
}

// snapshot is the state that the instruments report. It does not change after
// the worker stores it.
type snapshot struct {
	values       map[groupKey]entry
	order        []groupKey
	groupsFound  int
	groupsKnown  bool
	collected    int64
	collectedSet bool
}

// Worker collects the consumer lag.
type Worker struct {
	collect  Collector
	interval time.Duration
	log      *slog.Logger
	now      func() time.Time

	snap   atomic.Pointer[snapshot]
	errors metric.Int64Counter
	reg    metric.Registration

	// mu makes the collections run one after the other. It guards dropped
	// and skipped.
	mu      sync.Mutex
	dropped int
	skipped int
}

// New makes the worker and registers the instruments. It does not start the
// collection. The interval must be more than 0.
func New(cfg Config) (*Worker, error) {
	if cfg.Collect == nil || cfg.Meter == nil {
		return nil, errors.New("lag: the collector and the meter are required")
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("lag: the interval must be more than 0")
	}
	w := &Worker{collect: cfg.Collect, interval: cfg.Interval, log: cfg.Log, now: cfg.Now}
	if w.log == nil {
		w.log = slog.Default()
	}
	if w.now == nil {
		w.now = time.Now
	}
	w.snap.Store(&snapshot{})

	lagGauge, err := cfg.Meter.Float64ObservableGauge(MetricLag, metric.WithUnit("s"),
		metric.WithDescription("The age of the oldest unacknowledged event of a consumer group."))
	if err != nil {
		return nil, err
	}
	collected, err := cfg.Meter.Int64ObservableGauge(MetricCollected, metric.WithUnit("s"),
		metric.WithDescription("The Unix time of the end of the last collection with no failure."))
	if err != nil {
		return nil, err
	}
	groups, err := cfg.Meter.Int64ObservableGauge(MetricGroups, metric.WithUnit("{group}"),
		metric.WithDescription("The count of consumer groups that the last collection found, before the cap."))
	if err != nil {
		return nil, err
	}
	if w.errors, err = cfg.Meter.Int64Counter(MetricErrors, metric.WithUnit("{error}"),
		metric.WithDescription("The count of failed operations of the lag collection.")); err != nil {
		return nil, err
	}
	// The callback reads the snapshot only. It makes no call to a port.
	w.reg, err = cfg.Meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s := w.snap.Load()
		for _, k := range s.order {
			o.ObserveFloat64(lagGauge, s.values[k].lag,
				metric.WithAttributes(attribute.String("stream", k.stream), attribute.String("group", k.group)))
		}
		if s.collectedSet {
			o.ObserveInt64(collected, s.collected)
		}
		if s.groupsKnown {
			o.ObserveInt64(groups, int64(s.groupsFound))
		}
		return nil
	}, lagGauge, collected, groups)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Close removes the callback of the instruments.
func (w *Worker) Close() error { return w.reg.Unregister() }

// Run runs one collection at once, then one collection each interval. It
// returns when the context ends, after the collection in progress.
func (w *Worker) Run(ctx context.Context) {
	w.runOnce(ctx)
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.runOnce(ctx)
		}
	}
}

// runOnce runs one collection with the interval as time limit.
func (w *Worker) runOnce(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, w.interval)
	defer cancel()
	w.Collect(cctx)
}

// Collect runs one collection and stores the new snapshot. A parallel call
// waits for the call in progress.
func (w *Worker) Collect(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	res, err := w.collect.Execute(ctx, queries.CollectConsumerLag{})
	if err != nil {
		// The query reports failures in its result. An error is a defect.
		res = queries.CollectConsumerLagResult{Failures: []queries.LagFailure{{Operation: queries.OperationApps, Err: err}}}
	}
	old := w.snap.Load()
	next := &snapshot{
		values:       map[groupKey]entry{},
		groupsFound:  old.groupsFound,
		groupsKnown:  old.groupsKnown,
		collected:    old.collected,
		collectedSet: old.collectedSet,
	}

	appsFailed := false
	failedStreams := map[string]bool{}
	failedGroups := map[groupKey]bool{}
	for _, f := range res.Failures {
		switch f.Operation {
		case queries.OperationApps:
			appsFailed = true
		case queries.OperationGroups:
			failedStreams[f.Stream] = true
		default:
			failedGroups[groupKey{f.Stream, f.Group}] = true
		}
		// A cancel of the parent context is a shutdown, not a failure. A time
		// limit of the collection is a failure.
		if f.Skipped || errors.Is(ctx.Err(), context.Canceled) {
			continue
		}
		w.errors.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("operation", f.Operation)))
		w.log.LogAttrs(ctx, slog.LevelWarn, "events.lag.collect_failed",
			slog.String("action", "events.lag.collect_failed"),
			slog.String("operation", f.Operation), slog.String("stream", f.Stream),
			slog.String("group", f.Group), slog.Any("error", f.Err))
	}

	for _, v := range res.Values {
		// An attribute with a value that is not valid UTF-8 makes the export
		// of all metrics fail. The query skips such names. This is a second guard.
		if !utf8.ValidString(v.Stream) || !utf8.ValidString(v.Group) {
			res.GroupsSkipped++
			continue
		}
		k := groupKey{v.Stream, v.Group}
		next.values[k] = entry{lag: v.Lag.Seconds()}
		next.order = append(next.order, k)
	}
	// An affected group keeps its last value for MaxStale collections.
	for _, k := range old.order {
		if _, ok := next.values[k]; ok {
			continue
		}
		if !appsFailed && !failedStreams[k.stream] && !failedGroups[k] {
			continue
		}
		e := old.values[k]
		e.stale++
		if e.stale > MaxStale {
			continue
		}
		next.values[k] = e
		next.order = append(next.order, k)
	}
	sortKeys(next.order)

	// A count from a collection that lost a stream is too low. Keep the old
	// count and the old log state in that case.
	if !appsFailed && len(failedStreams) == 0 {
		next.groupsFound, next.groupsKnown = res.GroupsFound, true
		w.logCap(ctx, res)
	}
	w.logSkipped(ctx, res.GroupsSkipped)
	if len(res.Failures) == 0 {
		next.collected, next.collectedSet = w.now().Unix(), true
	}
	w.snap.Store(next)
}

// logCap writes the warn log when the count of dropped groups changes.
func (w *Worker) logCap(ctx context.Context, res queries.CollectConsumerLagResult) {
	dropped := res.GroupsCapped
	if dropped != w.dropped && dropped > 0 {
		w.log.LogAttrs(ctx, slog.LevelWarn, "events.lag.groups_capped",
			slog.String("action", "events.lag.groups_capped"),
			slog.Int("found", res.GroupsFound), slog.Int("dropped", dropped))
	}
	w.dropped = dropped
}

// logSkipped writes the warn log when the count of skipped groups changes.
func (w *Worker) logSkipped(ctx context.Context, skipped int) {
	if skipped != w.skipped && skipped > 0 {
		w.log.LogAttrs(ctx, slog.LevelWarn, "events.lag.groups_skipped",
			slog.String("action", "events.lag.groups_skipped"), slog.Int("skipped", skipped))
	}
	w.skipped = skipped
}
