// Package registrations is the port of the event catalog store.
package registrations

import (
	"context"
	"errors"
	"time"

	"github.com/techorionai/constellation-events/internal/domain"
)

// Errors of the store.
var (
	// ErrNotFound means that the registration or the type does not exist.
	ErrNotFound = errors.New("not found")
	// ErrUnavailable means that the store did not answer.
	ErrUnavailable = errors.New("the store is not available")
	// ErrTooManyTypes means that the registration makes more than
	// domain.MaxTypesPerApp types of the app in the catalog.
	ErrTooManyTypes = errors.New("too many event types of the app")
)

// TypeFilter selects the event types of a list.
type TypeFilter struct {
	// App is the app key, or empty for all apps.
	App string
	// IncludeRemoved includes the types that have a removal time.
	IncludeRemoved bool
	// After is the last type of the previous page, or empty.
	After string
}

// Repository stores the registrations and the event types.
type Repository interface {
	// Register stores a registration. It runs in one transaction under a row
	// lock of the app. An equal hash gives the stored result and no write.
	Register(ctx context.Context, r domain.Registration, hash []byte, registeredBy string, now time.Time) (domain.Result, error)
	// ListRegistrations returns at most limit registrations after the app
	// "after", in the order of the app. Each has the types with no removal time.
	ListRegistrations(ctx context.Context, after string, limit int) ([]domain.StoredRegistration, error)
	// ListApps returns the app of each registration, sorted. It has no limit,
	// because the lag collector needs all streams.
	ListApps(ctx context.Context) ([]string, error)
	// GetRegistration returns the registration of one app, or ErrNotFound.
	GetRegistration(ctx context.Context, app string) (domain.StoredRegistration, error)
	// ListTypes returns at most limit types in the order of the type.
	ListTypes(ctx context.Context, f TypeFilter, limit int) ([]domain.StoredType, error)
	// GetType returns one type, also a removed one, or ErrNotFound.
	GetType(ctx context.Context, typ string) (domain.StoredType, error)
}
