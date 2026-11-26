// Package streams is the port of the event streams. The events service reads
// the state of the consumer groups. It does not add or acknowledge events.
package streams

import (
	"context"
	"errors"
	"time"
)

// ErrNoGroup means that the consumer group does not exist.
var ErrNoGroup = errors.New("the consumer group does not exist")

// Streams reads the consumer groups of the event streams.
type Streams interface {
	// Groups returns the sorted names of the consumer groups of the stream of
	// app. The list is empty if the stream does not exist.
	Groups(ctx context.Context, app string) ([]string, error)
	// Lag returns the age of the oldest event that the group did not
	// acknowledge, or 0 if there is none. It returns ErrNoGroup if the group
	// does not exist.
	Lag(ctx context.Context, app, group string) (time.Duration, error)
}
