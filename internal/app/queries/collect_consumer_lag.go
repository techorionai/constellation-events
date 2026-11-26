package queries

import (
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/techorionai/constellation-events/internal/ports/registrations"
	"github.com/techorionai/constellation-events/internal/ports/streams"
)

// MaxLagGroups is the largest count of groups that one collection reads. It is
// less than the default limit of 2000 series for each instrument of the OTel
// Go SDK.
const MaxLagGroups = 1000

// MaxLagGroupsPerStream is the largest count of groups that one collection
// reads for one stream. The limit stops one stream from using the full cap.
const MaxLagGroupsPerStream = 100

// MaxGroupNameBytes is the longest group name that the collection reads. The
// collection skips a longer name, and a name that is not valid UTF-8.
const MaxGroupNameBytes = 256

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
	// GroupsSkipped is the count of groups that the collection did not read,
	// because the name is too long or is not valid UTF-8.
	GroupsSkipped int
	// GroupsCapped is the count of valid groups that the collection did not
	// read, because of MaxLagGroups or MaxLagGroupsPerStream.
	GroupsCapped int
	Failures     []LagFailure
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
		valid := make([]string, 0, len(groups))
		for _, group := range groups {
			if len(group) > MaxGroupNameBytes || !utf8.ValidString(group) {
				res.GroupsSkipped++
				continue
			}
			valid = append(valid, group)
		}
		keep := min(len(valid), MaxLagGroupsPerStream, MaxLagGroups-read)
		res.GroupsCapped += len(valid) - keep
		for _, group := range valid[:keep] {
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
