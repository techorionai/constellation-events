package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/techorionai/constellation-events/internal/app/queries"
	apperrors "github.com/techorionai/constellation-events/internal/common/errors"
	"github.com/techorionai/constellation-events/internal/domain"
	"github.com/techorionai/constellation-events/internal/handlers/http/gen"
)

// Operations of a page token.
const (
	tokenOpTypes         = "types"
	tokenOpRegistrations = "registrations"
	maxTokenLength       = 1024
)

// pageToken is the content of a page token. It has no secret, and the service
// checks each value, so it needs no signature.
type pageToken struct {
	Op             string `json:"o"`
	App            string `json:"a,omitempty"`
	IncludeRemoved bool   `json:"r,omitempty"`
	Last           string `json:"l"`
}

var errInvalidToken = apperrors.NewProblem(http.StatusBadRequest, "invalid_page_token", "The page token is not valid.")

// EncodePageToken makes a page token from the operation, the filters, and the
// last key of the page.
func EncodePageToken(op, app string, includeRemoved bool, last string) string {
	b, _ := json.Marshal(pageToken{Op: op, App: app, IncludeRemoved: includeRemoved, Last: last})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodePageToken returns the last key of a page token. The token must have the
// given operation and the given filters. An empty token gives an empty key.
func DecodePageToken(token, op, app string, includeRemoved bool) (string, error) {
	if token == "" {
		return "", nil
	}
	if len(token) > maxTokenLength {
		return "", errInvalidToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", errInvalidToken
	}
	var t pageToken
	if err := json.Unmarshal(raw, &t); err != nil {
		return "", errInvalidToken
	}
	if t.Op != op || t.App != app || t.IncludeRemoved != includeRemoved || t.Last == "" {
		return "", errInvalidToken
	}
	switch op {
	case tokenOpTypes:
		if !domain.ValidEventType(t.Last) {
			return "", errInvalidToken
		}
	case tokenOpRegistrations:
		if !domain.ValidAppKey(t.Last) {
			return "", errInvalidToken
		}
	}
	return t.Last, nil
}

func toCatalogType(t domain.StoredType) gen.CatalogEventType {
	return gen.CatalogEventType{
		Type:        t.Type,
		App:         t.App,
		Source:      t.Source,
		Description: t.Description,
		DataSchema:  t.DataSchema,
		Deprecated:  t.Deprecated,
		CreatedAt:   t.CreatedAt,
		UpdatedAt:   t.UpdatedAt,
		RemovedAt:   t.RemovedAt,
	}
}

func toRegistered(s domain.StoredRegistration) gen.RegisteredEventTypes {
	defs := make([]gen.EventTypeDefinition, 0, len(s.Types))
	for _, t := range s.Types {
		deprecated := t.Deprecated
		defs = append(defs, gen.EventTypeDefinition{
			Type: t.Type, Description: t.Description, DataSchema: t.DataSchema, Deprecated: &deprecated,
		})
	}
	return gen.RegisteredEventTypes{
		Registration: gen.EventTypeRegistration{Source: s.Source, App: s.App, EventTypes: defs},
		Version:      int32(s.Version), //nolint:gosec // A small count.
		RegisteredAt: s.RegisteredAt,
	}
}

// EventTypesList lists the event types of the catalog.
func (s *Server) EventTypesList(ctx context.Context, req gen.EventTypesListRequestObject) (gen.EventTypesListResponseObject, error) {
	var app string
	if req.Params.App != nil {
		app = *req.Params.App
		if !domain.ValidAppKey(app) {
			return nil, apperrors.NewProblem(http.StatusBadRequest, "invalid_parameter", "The app filter is not valid.")
		}
	}
	includeRemoved := req.Params.IncludeRemoved != nil && *req.Params.IncludeRemoved
	size, err := pageSize(req.Params.PageSize, queries.DefaultTypePageSize, queries.MaxTypePageSize)
	if err != nil {
		return nil, err
	}
	var token string
	if req.Params.PageToken != nil {
		token = *req.Params.PageToken
	}
	after, err := DecodePageToken(token, tokenOpTypes, app, includeRemoved)
	if err != nil {
		return nil, err
	}
	res, err := s.d.ListEventTypes.Execute(ctx, queries.ListEventTypes{App: app, IncludeRemoved: includeRemoved, After: after, PageSize: size})
	if err != nil {
		return nil, err
	}
	page := gen.CatalogEventTypePage{Items: make([]gen.CatalogEventType, 0, len(res.Items))}
	for _, t := range res.Items {
		page.Items = append(page.Items, toCatalogType(t))
	}
	if res.Next != "" {
		next := EncodePageToken(tokenOpTypes, app, includeRemoved, res.Next)
		page.NextPageToken = &next
	}
	return gen.EventTypesList200JSONResponse(page), nil
}

// EventTypesGet returns one event type, also a removed one.
func (s *Server) EventTypesGet(ctx context.Context, req gen.EventTypesGetRequestObject) (gen.EventTypesGetResponseObject, error) {
	t, err := s.d.GetEventType.Execute(ctx, queries.GetEventType{Type: req.Type})
	if err != nil {
		return nil, err
	}
	return gen.EventTypesGet200JSONResponse(toCatalogType(t)), nil
}

// EventTypeRegistrationsList lists the registrations.
func (s *Server) EventTypeRegistrationsList(ctx context.Context, req gen.EventTypeRegistrationsListRequestObject) (gen.EventTypeRegistrationsListResponseObject, error) {
	size, err := pageSize(req.Params.PageSize, queries.DefaultRegistrationPageSize, queries.MaxRegistrationPageSize)
	if err != nil {
		return nil, err
	}
	var token string
	if req.Params.PageToken != nil {
		token = *req.Params.PageToken
	}
	after, err := DecodePageToken(token, tokenOpRegistrations, "", false)
	if err != nil {
		return nil, err
	}
	res, err := s.d.ListRegistrations.Execute(ctx, queries.ListRegistrations{After: after, PageSize: size})
	if err != nil {
		return nil, err
	}
	page := gen.RegisteredEventTypesPage{Items: make([]gen.RegisteredEventTypes, 0, len(res.Items))}
	for _, reg := range res.Items {
		page.Items = append(page.Items, toRegistered(reg))
	}
	if res.Next != "" {
		next := EncodePageToken(tokenOpRegistrations, "", false, res.Next)
		page.NextPageToken = &next
	}
	return gen.EventTypeRegistrationsList200JSONResponse(page), nil
}

// EventTypeRegistrationsGet returns the registration of one app.
func (s *Server) EventTypeRegistrationsGet(ctx context.Context, req gen.EventTypeRegistrationsGetRequestObject) (gen.EventTypeRegistrationsGetResponseObject, error) {
	reg, err := s.d.GetRegistration.Execute(ctx, queries.GetRegistration{App: req.App})
	if err != nil {
		return nil, err
	}
	return gen.EventTypeRegistrationsGet200JSONResponse(toRegistered(reg)), nil
}
