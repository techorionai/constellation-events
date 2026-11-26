// Package queries holds the use cases that read the catalog.
package queries

import (
	"context"
	"errors"

	apperrors "github.com/techorionai/constellation-events/internal/common/errors"
	"github.com/techorionai/constellation-events/internal/domain"
	"github.com/techorionai/constellation-events/internal/ports/registrations"
)

// Page limits.
const (
	// MaxPageBytes is the largest encoded size of a page. It is the default
	// response limit of the events client of platform-go.
	MaxPageBytes = 1 << 20
	// envelopeBytes is the space that a page keeps for the parts of the
	// response that are not items.
	envelopeBytes = 1 << 10
	// DefaultTypePageSize and MaxTypePageSize are the limits of EventTypes_list.
	DefaultTypePageSize = 50
	MaxTypePageSize     = 200
	// DefaultRegistrationPageSize and MaxRegistrationPageSize are the limits of
	// EventTypeRegistrations_list.
	DefaultRegistrationPageSize = 10
	MaxRegistrationPageSize     = 10
)

// BuildPage takes the items of one page from items, which has at most
// pageSize+1 entries. The page ends before the item that makes the encoded
// size larger than MaxPageBytes. A page always has the first item. more is true
// when items has an entry that is not in the page.
func BuildPage[T any](items []T, pageSize int, size func(T) int) (page []T, more bool) {
	total := envelopeBytes
	for i, it := range items {
		if i >= pageSize {
			break
		}
		n := size(it)
		if i > 0 && total+n > MaxPageBytes {
			break
		}
		total += n
		page = append(page, it)
	}
	return page, len(page) < len(items)
}

// mapStoreError maps an error of the store.
func mapStoreError(err error) error {
	if errors.Is(err, registrations.ErrUnavailable) {
		return apperrors.NewUnavailable()
	}
	return err
}

// ListEventTypes lists the event types of the catalog.
type ListEventTypes struct {
	App            string
	IncludeRemoved bool
	After          string
	PageSize       int
}

// ListEventTypesResult is a page of event types. Next is the last type of the
// page when more types follow, and empty on the last page.
type ListEventTypesResult struct {
	Items []domain.StoredType
	Next  string
}

// ListEventTypesHandler runs ListEventTypes.
type ListEventTypesHandler struct{ repo registrations.Repository }

// NewListEventTypes makes the handler.
func NewListEventTypes(repo registrations.Repository) *ListEventTypesHandler {
	return &ListEventTypesHandler{repo: repo}
}

// Execute returns one page.
func (h *ListEventTypesHandler) Execute(ctx context.Context, q ListEventTypes) (ListEventTypesResult, error) {
	size := q.PageSize
	if size <= 0 {
		size = DefaultTypePageSize
	}
	items, err := h.repo.ListTypes(ctx, registrations.TypeFilter{App: q.App, IncludeRemoved: q.IncludeRemoved, After: q.After}, size+1)
	if err != nil {
		return ListEventTypesResult{}, mapStoreError(err)
	}
	page, more := BuildPage(items, size, domain.StoredType.ApproxSize)
	res := ListEventTypesResult{Items: page}
	if more && len(page) > 0 {
		res.Next = page[len(page)-1].Type
	}
	return res, nil
}

// GetEventType reads one event type.
type GetEventType struct{ Type string }

// GetEventTypeHandler runs GetEventType.
type GetEventTypeHandler struct{ repo registrations.Repository }

// NewGetEventType makes the handler.
func NewGetEventType(repo registrations.Repository) *GetEventTypeHandler {
	return &GetEventTypeHandler{repo: repo}
}

// Execute returns the type, also a removed one. A type that does not exist
// gives the 404 problem event_type_not_found.
func (h *GetEventTypeHandler) Execute(ctx context.Context, q GetEventType) (domain.StoredType, error) {
	if !domain.ValidEventType(q.Type) {
		return domain.StoredType{}, errEventTypeNotFound()
	}
	t, err := h.repo.GetType(ctx, q.Type)
	if errors.Is(err, registrations.ErrNotFound) {
		return domain.StoredType{}, errEventTypeNotFound()
	}
	if err != nil {
		return domain.StoredType{}, mapStoreError(err)
	}
	return t, nil
}

func errEventTypeNotFound() error {
	return apperrors.NewNotFound("event_type_not_found", "The event type does not exist.")
}

func errRegistrationNotFound() error {
	return apperrors.NewNotFound("registration_not_found", "The registration does not exist.")
}

// ListRegistrations lists the registrations.
type ListRegistrations struct {
	After    string
	PageSize int
}

// ListRegistrationsResult is a page of registrations. Next is the last app of
// the page when more follow.
type ListRegistrationsResult struct {
	Items []domain.StoredRegistration
	Next  string
}

// ListRegistrationsHandler runs ListRegistrations.
type ListRegistrationsHandler struct{ repo registrations.Repository }

// NewListRegistrations makes the handler.
func NewListRegistrations(repo registrations.Repository) *ListRegistrationsHandler {
	return &ListRegistrationsHandler{repo: repo}
}

// Execute returns one page.
func (h *ListRegistrationsHandler) Execute(ctx context.Context, q ListRegistrations) (ListRegistrationsResult, error) {
	size := q.PageSize
	if size <= 0 {
		size = DefaultRegistrationPageSize
	}
	items, err := h.repo.ListRegistrations(ctx, q.After, size+1)
	if err != nil {
		return ListRegistrationsResult{}, mapStoreError(err)
	}
	page, more := BuildPage(items, size, domain.StoredRegistration.ApproxSize)
	res := ListRegistrationsResult{Items: page}
	if more && len(page) > 0 {
		res.Next = page[len(page)-1].App
	}
	return res, nil
}

// GetRegistration reads the registration of one app.
type GetRegistration struct{ App string }

// GetRegistrationHandler runs GetRegistration.
type GetRegistrationHandler struct{ repo registrations.Repository }

// NewGetRegistration makes the handler.
func NewGetRegistration(repo registrations.Repository) *GetRegistrationHandler {
	return &GetRegistrationHandler{repo: repo}
}

// Execute returns the registration. An app that does not exist gives the 404
// problem registration_not_found.
func (h *GetRegistrationHandler) Execute(ctx context.Context, q GetRegistration) (domain.StoredRegistration, error) {
	if !domain.ValidAppKey(q.App) {
		return domain.StoredRegistration{}, errRegistrationNotFound()
	}
	s, err := h.repo.GetRegistration(ctx, q.App)
	if errors.Is(err, registrations.ErrNotFound) {
		return domain.StoredRegistration{}, errRegistrationNotFound()
	}
	if err != nil {
		return domain.StoredRegistration{}, mapStoreError(err)
	}
	return s, nil
}
