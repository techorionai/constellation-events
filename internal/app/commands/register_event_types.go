// Package commands holds the use cases that change the catalog.
package commands

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/techorionai/constellation-platform-go/audit"
	"github.com/techorionai/constellation-platform-go/auth"
	"github.com/techorionai/constellation-platform-go/transaction"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	apperrors "github.com/techorionai/constellation-events/internal/common/errors"
	"github.com/techorionai/constellation-events/internal/domain"
	"github.com/techorionai/constellation-events/internal/ports/registrations"
)

// Codes of the caller check.
const (
	CodeRegistrationAppMismatch = "registration_app_mismatch"
	// CodeForbidden is the code in the details of a denied entry.
	CodeForbidden = "forbidden"
)

// Values of the audit entry of a registration.
const (
	// AuditAction is the action of the audit entry of a registration.
	AuditAction = "events:event_type:register"
	// AuditTargetType is the target type of the audit entry of a registration.
	AuditTargetType = "registration"
	// MaxAuditNames is the largest number of type names in one list of the
	// details of an audit entry.
	MaxAuditNames = 30
	// RefusedWriteTimeout is the time limit of the write of a refused entry.
	RefusedWriteTimeout = 2 * time.Second
)

// Values of the attribute outcome of the metric.
const (
	OutcomeChanged   = "changed"
	OutcomeUnchanged = "unchanged"
	OutcomeInvalid   = "invalid"
	OutcomeError     = "error"
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
	tx      transaction.Transactor
	writer  audit.Writer
	limiter *RefusedLimiter
	notify  func()
	log     *slog.Logger
	now     func() time.Time
	counter metric.Int64Counter
	dropped metric.Int64Counter
}

// Deps are the dependencies of the handler.
type Deps struct {
	Repo registrations.Repository
	// Tx runs a change and a refused entry in a transaction.
	Tx transaction.Transactor
	// Writer writes the audit entries in the transaction of the context.
	Writer audit.Writer
	// Limiter limits the entries of refused registrations. It is required.
	Limiter *RefusedLimiter
	// Notify tells the audit relay that a new entry exists. It runs after the
	// commit. It can be nil.
	Notify func()
	// Log is the logger. A nil value means slog.Default.
	Log *slog.Logger
	// Now is the clock. A nil value means time.Now.
	Now func() time.Time
}

// NewRegisterEventTypes makes the handler.
func NewRegisterEventTypes(d Deps) (*RegisterEventTypesHandler, error) {
	if d.Repo == nil || d.Tx == nil || d.Writer == nil || d.Limiter == nil {
		return nil, errors.New("make the registration handler: the repository, the transactor, the audit writer, and the limiter are required")
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Notify == nil {
		d.Notify = func() {}
	}
	meter := otel.Meter("events")
	counter, err := meter.Int64Counter("constellation.events.registration",
		metric.WithUnit("{request}"),
		metric.WithDescription("Registrations of event types, by outcome."))
	if err != nil {
		return nil, fmt.Errorf("make the registration counter: %w", err)
	}
	dropped, err := meter.Int64Counter("constellation.events.audit.refused_dropped",
		metric.WithUnit("{entry}"),
		metric.WithDescription("Audit entries of refused registrations that the limit of each principal dropped."))
	if err != nil {
		return nil, fmt.Errorf("make the dropped counter: %w", err)
	}
	return &RegisterEventTypesHandler{
		repo: d.Repo, tx: d.Tx, writer: d.Writer, limiter: d.Limiter, notify: d.Notify,
		log: d.Log, now: d.Now, counter: counter, dropped: dropped,
	}, nil
}

// auditWriteError marks an error of the audit writer in the transaction of a
// change.
type auditWriteError struct{ err error }

func (e *auditWriteError) Error() string { return "write the audit entry: " + e.err.Error() }
func (e *auditWriteError) Unwrap() error { return e.err }

// Execute checks the caller and the content, then stores the registration and
// its audit entry in one transaction.
func (h *RegisterEventTypesHandler) Execute(ctx context.Context, cmd RegisterEventTypes) (domain.Result, error) {
	p := cmd.Principal
	reg := cmd.Registration
	if p.Type != auth.PrincipalService || p.App == "" || p.App != cmd.PathApp || reg.App != cmd.PathApp {
		h.refuse(ctx, p, audit.Invalid, cmd.PathApp, CodeRegistrationAppMismatch)
		h.count(ctx, OutcomeInvalid)
		return domain.Result{}, &apperrors.ValidationError{Code: CodeRegistrationAppMismatch}
	}
	if errs := reg.Validate(); len(errs) > 0 {
		fields := make([]apperrors.FieldError, 0, len(errs))
		for _, e := range errs {
			fields = append(fields, apperrors.FieldError{Field: e.Field, Code: e.Code, Message: e.Message})
		}
		h.refuse(ctx, p, audit.Invalid, cmd.PathApp, errs[0].Code)
		h.count(ctx, OutcomeInvalid)
		return domain.Result{}, &apperrors.ValidationError{Code: errs[0].Code, Fields: fields}
	}
	hash, err := reg.Hash()
	if err != nil {
		h.count(ctx, OutcomeError)
		return domain.Result{}, fmt.Errorf("hash the registration: %w", err)
	}
	var res domain.Result
	err = h.tx.WithinTx(ctx, func(ctx context.Context) error {
		r, err := h.repo.Register(ctx, reg, hash, p.ID, h.now())
		if err != nil {
			return err
		}
		if r.Changed {
			if err := h.writer.Write(ctx, successEntry(reg.App, r, h.now())); err != nil {
				return &auditWriteError{err: err}
			}
			h.tx.AfterCommit(ctx, func(context.Context) { h.notify() })
		}
		res = r
		return nil
	}, transaction.Name("register_event_types"))
	switch {
	case errors.Is(err, registrations.ErrTooManyTypes):
		h.refuse(ctx, p, audit.Invalid, cmd.PathApp, domain.CodeLimitExceeded)
		h.count(ctx, OutcomeInvalid)
		return domain.Result{}, &apperrors.ValidationError{Code: domain.CodeLimitExceeded, Fields: []apperrors.FieldError{{
			Field: "/eventTypes", Code: domain.CodeLimitExceeded,
			Message: fmt.Sprintf("An app has a maximum of %d event types in the catalog, removed types included.", domain.MaxTypesPerApp),
		}}}
	case err != nil:
		h.count(ctx, OutcomeError)
		return domain.Result{}, h.failure(ctx, err)
	}
	if res.Changed {
		h.count(ctx, OutcomeChanged)
	} else {
		h.count(ctx, OutcomeUnchanged)
	}
	return res, nil
}

// failure maps the error of the transaction of a change. A database error
// gives 503. A rule error of the audit writer gives a plain error that the
// server maps to 500 internal_error.
func (h *RegisterEventTypesHandler) failure(ctx context.Context, err error) error {
	var aw *auditWriteError
	if errors.As(err, &aw) {
		if name, rule := auditRuleError(aw.err); rule {
			h.log.ErrorContext(ctx, "the audit entry of a registration is not valid", "error_type", name)
			return fmt.Errorf("write the audit entry: %s", name)
		}
		h.log.ErrorContext(ctx, "the audit entry of a registration is not stored", "error_type", "database")
		return apperrors.NewUnavailable()
	}
	if errors.Is(err, registrations.ErrUnavailable) || h.repo.Unavailable(err) {
		return apperrors.NewUnavailable()
	}
	return fmt.Errorf("register: %w", err)
}

// auditRuleError tells whether err is a rule error of the audit writer. It
// returns the name of the rule error. It never returns the text of err,
// because the text can have data of the entry.
func auditRuleError(err error) (string, bool) {
	for _, r := range []struct {
		err  error
		name string
	}{
		{audit.ErrInvalidDetails, "ErrInvalidDetails"},
		{audit.ErrInvalidTarget, "ErrInvalidTarget"},
		{audit.ErrEntryTooLarge, "ErrEntryTooLarge"},
		{audit.ErrInvalidText, "ErrInvalidText"},
		{audit.ErrNoTransaction, "ErrNoTransaction"},
		{audit.ErrInvalidAction, "ErrInvalidAction"},
		{audit.ErrInvalidResult, "ErrInvalidResult"},
		{audit.ErrInvalidSummary, "ErrInvalidSummary"},
		{audit.ErrInvalidActor, "ErrInvalidActor"},
	} {
		if errors.Is(err, r.err) {
			return r.name, true
		}
	}
	return "", false
}

func (h *RegisterEventTypesHandler) count(ctx context.Context, outcome string) {
	h.counter.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// nameList returns at most MaxAuditNames names and tells whether it cut the
// list. The result is never nil, so the JSON has an empty list.
func nameList(names []string) ([]string, bool) {
	if len(names) > MaxAuditNames {
		return append([]string{}, names[:MaxAuditNames]...), true
	}
	return append([]string{}, names...), false
}

// successEntry makes the audit entry of a change.
func successEntry(app string, r domain.Result, at time.Time) audit.Entry {
	added, addedCut := nameList(r.AddedTypes)
	removed, removedCut := nameList(r.RemovedTypes)
	modified, modifiedCut := nameList(r.ModifiedTypes)
	return audit.Entry{
		Action:  AuditAction,
		Result:  audit.Success,
		Target:  &audit.Target{Type: AuditTargetType, ID: app},
		Summary: fmt.Sprintf("Event types of app %s registered, version %d", app, r.Version),
		Details: map[string]any{
			"version":           r.Version,
			"eventTypeCount":    r.EventTypeCount,
			"addedCount":        r.Added,
			"removedCount":      r.Removed,
			"modifiedCount":     r.Modified,
			"added":             added,
			"removed":           removed,
			"modified":          modified,
			"addedTruncated":    addedCut,
			"removedTruncated":  removedCut,
			"modifiedTruncated": modifiedCut,
		},
		Time: at,
	}
}

// refusedEntry makes the audit entry of a refused request. The target is the
// path app only when it is an app key.
func refusedEntry(result audit.Result, pathApp, code string, at time.Time) audit.Entry {
	e := audit.Entry{Action: AuditAction, Result: result, Time: at, Summary: "Registration of event types refused"}
	if result == audit.Denied {
		e.Summary = "Registration of event types denied"
	}
	details := map[string]any{"code": code}
	if domain.ValidAppKey(pathApp) {
		e.Target = &audit.Target{Type: AuditTargetType, ID: pathApp}
	} else {
		details["appValid"] = false
	}
	e.Details = details
	return e
}

// refuse writes the audit entry of a refused or denied request in a short
// transaction of its own. The write does not depend on the cancel of the
// request. A request over the limit of its principal writes no entry. A failed
// write gives an error log record, and the response of the request does not
// change.
func (h *RegisterEventTypesHandler) refuse(ctx context.Context, p auth.Principal, result audit.Result, pathApp, code string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), RefusedWriteTimeout)
	defer cancel()
	if !h.limiter.Allow(p.ID) {
		h.dropped.Add(ctx, 1)
		return
	}
	entry := refusedEntry(result, pathApp, code, h.now())
	err := h.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := h.writer.Write(ctx, entry); err != nil {
			return err
		}
		h.tx.AfterCommit(ctx, func(context.Context) { h.notify() })
		return nil
	}, transaction.Name("audit_refused_registration"))
	if err != nil {
		name, rule := auditRuleError(err)
		if !rule {
			name = "database"
		}
		h.log.ErrorContext(ctx, "the audit entry of a refused registration is not written", "error_type", name, "result", string(result))
	}
}

// AuditDenied writes the audit entry of a registration that the scope check
// denies. The server calls it before it sends the 403. The actor comes from
// the principal in the context.
func (h *RegisterEventTypesHandler) AuditDenied(ctx context.Context, p auth.Principal, pathApp string) {
	h.refuse(ctx, p, audit.Denied, pathApp, CodeForbidden)
}
