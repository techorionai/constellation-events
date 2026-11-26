// Package commands holds the use cases that change the catalog.
package commands

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/techorionai/constellation-platform-go/auth"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	apperrors "github.com/techorionai/constellation-events/internal/common/errors"
	"github.com/techorionai/constellation-events/internal/domain"
	"github.com/techorionai/constellation-events/internal/ports/registrations"
)

// Codes of the caller check.
const CodeRegistrationAppMismatch = "registration_app_mismatch"

// AuditAction is the action of the audit log record of a registration.
const AuditAction = "events:event_type:register"

// Values of the attribute outcome of the metric and the result of the audit record.
const (
	OutcomeChanged   = "changed"
	OutcomeUnchanged = "unchanged"
	OutcomeInvalid   = "invalid"
	OutcomeError     = "error"
	ResultDenied     = "denied"
)

// RegisterEventTypes is the command that registers the event types of an app.
type RegisterEventTypes struct {
	// PathApp is the app of the path.
	PathApp string
	// Registration is the body.
	Registration domain.Registration
	// Principal is the caller.
	Principal auth.Principal
}

// RegisterEventTypesHandler runs RegisterEventTypes.
type RegisterEventTypesHandler struct {
	repo    registrations.Repository
	log     *slog.Logger
	now     func() time.Time
	counter metric.Int64Counter
}

// NewRegisterEventTypes makes the handler. A nil log means slog.Default. A nil
// now means time.Now.
func NewRegisterEventTypes(repo registrations.Repository, log *slog.Logger, now func() time.Time) (*RegisterEventTypesHandler, error) {
	if log == nil {
		log = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	counter, err := otel.Meter("events").Int64Counter("constellation.events.registration",
		metric.WithUnit("{request}"),
		metric.WithDescription("Registrations of event types, by outcome."))
	if err != nil {
		return nil, fmt.Errorf("make the registration counter: %w", err)
	}
	return &RegisterEventTypesHandler{repo: repo, log: log, now: now, counter: counter}, nil
}

// Execute checks the caller and the content, then stores the registration.
func (h *RegisterEventTypesHandler) Execute(ctx context.Context, cmd RegisterEventTypes) (domain.Result, error) {
	p := cmd.Principal
	reg := cmd.Registration
	if p.Type != auth.PrincipalService || p.App == "" || p.App != cmd.PathApp || reg.App != cmd.PathApp {
		h.audit(ctx, p, cmd.PathApp, OutcomeInvalid, nil)
		h.count(ctx, OutcomeInvalid)
		return domain.Result{}, &apperrors.ValidationError{Code: CodeRegistrationAppMismatch}
	}
	if errs := reg.Validate(); len(errs) > 0 {
		fields := make([]apperrors.FieldError, 0, len(errs))
		for _, e := range errs {
			fields = append(fields, apperrors.FieldError{Field: e.Field, Code: e.Code, Message: e.Message})
		}
		h.audit(ctx, p, reg.App, OutcomeInvalid, nil)
		h.count(ctx, OutcomeInvalid)
		return domain.Result{}, &apperrors.ValidationError{Code: errs[0].Code, Fields: fields}
	}
	hash, err := reg.Hash()
	if err != nil {
		h.count(ctx, OutcomeError)
		return domain.Result{}, fmt.Errorf("hash the registration: %w", err)
	}
	res, err := h.repo.Register(ctx, reg, hash, p.ID, h.now())
	switch {
	case errors.Is(err, registrations.ErrTooManyTypes):
		h.audit(ctx, p, reg.App, OutcomeInvalid, nil)
		h.count(ctx, OutcomeInvalid)
		return domain.Result{}, &apperrors.ValidationError{Code: domain.CodeLimitExceeded, Fields: []apperrors.FieldError{{
			Field: "/eventTypes", Code: domain.CodeLimitExceeded,
			Message: fmt.Sprintf("An app has a maximum of %d event types in the catalog, removed types included.", domain.MaxTypesPerApp),
		}}}
	case errors.Is(err, registrations.ErrUnavailable):
		h.count(ctx, OutcomeError)
		return domain.Result{}, apperrors.NewUnavailable()
	case err != nil:
		h.count(ctx, OutcomeError)
		return domain.Result{}, fmt.Errorf("register: %w", err)
	}
	if res.Changed {
		h.audit(ctx, p, reg.App, OutcomeChanged, &res)
		h.count(ctx, OutcomeChanged)
	} else {
		h.count(ctx, OutcomeUnchanged)
	}
	return res, nil
}

func (h *RegisterEventTypesHandler) count(ctx context.Context, outcome string) {
	h.counter.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// audit writes the audit record of a change or of a registration that failed.
// The record has no JSON Schema, no description, and no text of the compiler.
// After R09, an audit entry through the outbox replaces this record.
func (h *RegisterEventTypesHandler) audit(ctx context.Context, p auth.Principal, app, result string, res *domain.Result) {
	attrs := []any{
		"action", AuditAction, "principal", p.ID, "principal_type", string(p.Type), "app", app, "result", result,
	}
	if res != nil {
		attrs = append(attrs, "version", res.Version, "event_type_count", res.EventTypeCount,
			"added_count", res.Added, "removed_count", res.Removed, "modified_count", res.Modified)
	}
	h.log.InfoContext(ctx, "events.event_type.register", attrs...)
}

// LogDenied writes the audit record of a registration that gave 403. It uses
// the logger of the handler, so all audit records of a registration come from
// one place.
func (h *RegisterEventTypesHandler) LogDenied(ctx context.Context, p auth.Principal, app string) {
	h.audit(ctx, p, app, ResultDenied, nil)
}
