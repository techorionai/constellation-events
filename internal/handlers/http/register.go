package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/techorionai/constellation-platform-go/auth"

	"github.com/techorionai/constellation-events/internal/app/commands"
	"github.com/techorionai/constellation-events/internal/domain"
	"github.com/techorionai/constellation-events/internal/handlers/http/gen"
)

// registerBody is the decoded body of a registration. A pointer or a nil raw
// value marks a member that is missing.
type registerBody struct {
	Source     *string         `json:"source"`
	App        *string         `json:"app"`
	EventTypes *[]registerType `json:"eventTypes"`
}

type registerType struct {
	Type        *string         `json:"type"`
	Description *string         `json:"description"`
	DataSchema  json.RawMessage `json:"dataSchema"`
	Deprecated  *bool           `json:"deprecated"`
}

type registerBodyKey struct{}

// errInvalidBody is the error of a body that breaks the shape of the contract.
var errInvalidBody = errors.New("the body is not valid")

// DecodeRegisterBody decodes the body of a registration. It refuses an unknown
// field at the top level or in an event type, data after the object, a
// missing required member, and a member of the wrong JSON type. A member
// inside a dataSchema is not checked. The text of each dataSchema stays as it
// is, so a number keeps its text.
func DecodeRegisterBody(data []byte) (domain.Registration, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var b registerBody
	if err := dec.Decode(&b); err != nil {
		return domain.Registration{}, errInvalidBody
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return domain.Registration{}, errInvalidBody
	}
	if b.Source == nil || b.App == nil || b.EventTypes == nil {
		return domain.Registration{}, errInvalidBody
	}
	reg := domain.Registration{Source: *b.Source, App: *b.App, EventTypes: make([]domain.EventTypeDefinition, 0, len(*b.EventTypes))}
	for _, t := range *b.EventTypes {
		if t.Type == nil || t.Description == nil || len(t.DataSchema) == 0 {
			return domain.Registration{}, errInvalidBody
		}
		def := domain.EventTypeDefinition{Type: *t.Type, Description: *t.Description, DataSchema: t.DataSchema}
		if t.Deprecated != nil {
			def.Deprecated = *t.Deprecated
		}
		reg.EventTypes = append(reg.EventTypes, def)
	}
	return reg, nil
}

// decodeRegisterBody reads the body with the size limit, decodes it with
// DecodeRegisterBody, and puts the result in the request context. The
// generated code then reads an empty object, because it accepts unknown fields
// and changes each number to float64. It runs after the token check and the
// scope check.
func (s *Server) decodeRegisterBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.d.MaxBodyBytes))
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			s.writeProblem(w, r, http.StatusRequestEntityTooLarge, "content_too_large", "Content too large", "The request body is larger than the limit of the service.")
			return
		case err != nil:
			s.writeProblem(w, r, http.StatusBadRequest, "invalid_request", "Invalid request", "The request is not valid.")
			return
		}
		reg, err := DecodeRegisterBody(data)
		if err != nil {
			s.writeProblem(w, r, http.StatusBadRequest, "invalid_request", "Invalid request", "The request is not valid.")
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), registerBodyKey{}, reg))
		r.Body = io.NopCloser(strings.NewReader("{}"))
		r.ContentLength = 2
		next.ServeHTTP(w, r)
	})
}

// EventTypeRegistrationsRegister registers the event types of an app.
func (s *Server) EventTypeRegistrationsRegister(ctx context.Context, req gen.EventTypeRegistrationsRegisterRequestObject) (gen.EventTypeRegistrationsRegisterResponseObject, error) {
	reg, ok := ctx.Value(registerBodyKey{}).(domain.Registration)
	if !ok {
		return nil, errors.New("the register body is not in the context")
	}
	p, _ := auth.PrincipalFromContext(ctx)
	res, err := s.d.Register.Execute(ctx, commands.RegisterEventTypes{PathApp: req.App, Registration: reg, Principal: p})
	if err != nil {
		return nil, err
	}
	return gen.EventTypeRegistrationsRegister200JSONResponse{
		App:            res.App,
		Version:        int32(res.Version), //nolint:gosec // A small count.
		RegisteredAt:   res.RegisteredAt,
		EventTypeCount: int32(res.EventTypeCount), //nolint:gosec // At most 1000.
	}, nil
}
