package queries

import (
	"context"
	"errors"
	"time"

	"github.com/techorionai/constellation-events/internal/ports/registrations"
	"github.com/techorionai/constellation-events/internal/ports/streams"
)

// MaxLagGroups is the largest count of groups that one collection reads. It is
// less than the default limit of 2000 series for each instrument of the OTel
// Go SDK.
const MaxLagGroups = 1000

// Operations of a collection.
const (
	OperationApps   = "apps"
	OperationGroups = "groups"
	OperationLag    = "lag"
)

// CollectConsumerLag collects the lag of all consumer groups of the streams of
// the registered apps.
type CollectConsumerLag struct{}

// LagValue is the lag of one group.
type LagValue struct {
	// Stream is the app of the stream, not the key.
	Stream string
	Group  string
	Lag    time.Duration
}

// LagFailure is one operation of a collection that did not work.
type LagFailure struct {
	// Operation is OperationApps, OperationGroups, or OperationLag.
	Operation string
	// Stream is empty for OperationApps. Group is empty for OperationApps and
	// OperationGroups.
	Stream string
	Group  string
	Err    error
	// Skipped is true when the collection did not try the operation, because
	// the time limit of the collection ended before. A skipped operation
	// affects the last values in the same way as a failed operation.
	Skipped bool
}

// CollectConsumerLagResult is the result of one collection.
type CollectConsumerLagResult struct {
	// Values has the lag of each group, in the order of stream and group. It has
	// at most MaxLagGroups values.
	Values []LagValue
	// GroupsFound is the count of groups before the cap.
	GroupsFound int
	Failures    []LagFailure
}

// CollectConsumerLagHandler runs CollectConsumerLag.
type CollectConsumerLagHandler struct {
	apps    registrations.Repository
	streams streams.Streams
}

// NewCollectConsumerLag makes the handler.
func NewCollectConsumerLag(apps registrations.Repository, s streams.Streams) *CollectConsumerLagHandler {
	return &CollectConsumerLagHandler{apps: apps, streams: s}
}

// Execute lists the apps, then the groups of each stream, then the lag of each
// group. The calls are sequential. A failed operation is in Failures, and the
// collection goes on. The error result is always nil.
func (h *CollectConsumerLagHandler) Execute(ctx context.Context, _ CollectConsumerLag) (CollectConsumerLagResult, error) {
	var res CollectConsumerLagResult
	apps, err := h.apps.ListApps(ctx)
	if err != nil {
		res.Failures = append(res.Failures, LagFailure{Operation: OperationApps, Err: err})
		return res, nil
	}
	read := 0
	for _, app := range apps {
		if ctx.Err() != nil {
			res.Failures = append(res.Failures, LagFailure{Operation: OperationGroups, Stream: app, Err: ctx.Err(), Skipped: true})
			continue
		}
		groups, err := h.streams.Groups(ctx, app)
		if err != nil {
			res.Failures = append(res.Failures, LagFailure{Operation: OperationGroups, Stream: app, Err: err})
			continue
		}
		res.GroupsFound += len(groups)
		for _, group := range groups {
			if read >= MaxLagGroups {
				break
			}
			read++
			if ctx.Err() != nil {
				res.Failures = append(res.Failures, LagFailure{Operation: OperationLag, Stream: app, Group: group, Err: ctx.Err(), Skipped: true})
				continue
			}
			lag, err := h.streams.Lag(ctx, app, group)
			switch {
			case errors.Is(err, streams.ErrNoGroup):
				// The consumer deleted the group after the list. It is a normal case.
			case err != nil:
				res.Failures = append(res.Failures, LagFailure{Operation: OperationLag, Stream: app, Group: group, Err: err})
			default:
				res.Values = append(res.Values, LagValue{Stream: app, Group: group, Lag: lag})
			}
		}
	}
	return res, nil
}
