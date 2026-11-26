// Package valkey is the Valkey adapter of the event streams port.
package valkey

import (
	"context"
	"errors"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"

	streamsvalkey "github.com/techorionai/constellation-platform-go/streams/valkey"

	"github.com/techorionai/constellation-events/internal/ports/streams"
)

// Streams reads the consumer groups of the event streams with the helpers of
// platform-go. The client is the raw valkey-go client. The stream keys do not
// use the key prefix of the service.
type Streams struct {
	c valkeygo.Client
}

var _ streams.Streams = (*Streams)(nil)

// NewStreams makes the adapter.
func NewStreams(c valkeygo.Client) *Streams { return &Streams{c: c} }

// Groups returns the sorted names of the consumer groups of the stream of app.
func (s *Streams) Groups(ctx context.Context, app string) ([]string, error) {
	return streamsvalkey.Groups(ctx, s.c, app)
}

// Lag returns the age of the oldest event that the group did not acknowledge.
func (s *Streams) Lag(ctx context.Context, app, group string) (time.Duration, error) {
	d, err := streamsvalkey.Lag(ctx, s.c, app, group)
	if errors.Is(err, streamsvalkey.ErrNoGroup) {
		return 0, streams.ErrNoGroup
	}
	return d, err
}
