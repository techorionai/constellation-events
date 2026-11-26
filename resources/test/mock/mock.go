// Package mock has the mocks of the ports for unit tests. Integration tests
// do not use it.
package mock

import (
	"context"
	"sync"
	"time"

	"github.com/techorionai/constellation-events/internal/domain"
	"github.com/techorionai/constellation-events/internal/ports/registrations"
	"github.com/techorionai/constellation-events/internal/ports/streams"
)

var (
	_ registrations.Repository = (*Registrations)(nil)
	_ streams.Streams          = (*Streams)(nil)
)

// Registrations is a mock of the registrations port. Only ListApps has a
// function. The other methods return no value.
type Registrations struct {
	ListAppsFunc func(ctx context.Context) ([]string, error)

	mu    sync.Mutex
	calls int
}

// Calls returns the count of calls of ListApps.
func (m *Registrations) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// ListApps calls ListAppsFunc.
func (m *Registrations) ListApps(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	return m.ListAppsFunc(ctx)
}

// Register is not used.
func (m *Registrations) Register(context.Context, domain.Registration, []byte, string, time.Time) (domain.Result, error) {
	return domain.Result{}, nil
}

// ListRegistrations is not used.
func (m *Registrations) ListRegistrations(context.Context, string, int) ([]domain.StoredRegistration, error) {
	return nil, nil
}

// GetRegistration is not used.
func (m *Registrations) GetRegistration(context.Context, string) (domain.StoredRegistration, error) {
	return domain.StoredRegistration{}, nil
}

// ListTypes is not used.
func (m *Registrations) ListTypes(context.Context, registrations.TypeFilter, int) ([]domain.StoredType, error) {
	return nil, nil
}

// GetType is not used.
func (m *Registrations) GetType(context.Context, string) (domain.StoredType, error) {
	return domain.StoredType{}, nil
}

// Streams is a mock of the streams port.
type Streams struct {
	GroupsFunc func(ctx context.Context, app string) ([]string, error)
	LagFunc    func(ctx context.Context, app, group string) (time.Duration, error)

	mu    sync.Mutex
	calls int
}

// Calls returns the count of calls of Groups and Lag.
func (m *Streams) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *Streams) count() {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
}

// Groups calls GroupsFunc.
func (m *Streams) Groups(ctx context.Context, app string) ([]string, error) {
	m.count()
	return m.GroupsFunc(ctx, app)
}

// Lag calls LagFunc.
func (m *Streams) Lag(ctx context.Context, app, group string) (time.Duration, error) {
	m.count()
	return m.LagFunc(ctx, app, group)
}
