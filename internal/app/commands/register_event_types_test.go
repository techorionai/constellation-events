package commands_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/techorionai/constellation-platform-go/audit"
	"github.com/techorionai/constellation-platform-go/auth"
	"github.com/techorionai/constellation-platform-go/outbox"

	"github.com/techorionai/constellation-events/internal/app/commands"
	apperrors "github.com/techorionai/constellation-events/internal/common/errors"
	"github.com/techorionai/constellation-events/internal/domain"
	"github.com/techorionai/constellation-events/internal/ports/registrations"
	"github.com/techorionai/constellation-events/resources/test/mock"
)

// fakeRepo returns a scripted result of Register.
type fakeRepo struct {
	registrations.Repository
	result domain.Result
	err    error
}

func (f *fakeRepo) Register(context.Context, domain.Registration, []byte, string, time.Time) (domain.Result, error) {
	return f.result, f.err
}

func (f *fakeRepo) Unavailable(error) bool { return false }

// logBuffer keeps the text of the log records.
type logBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type fixture struct {
	h      *commands.RegisterEventTypesHandler
	repo   *fakeRepo
	writer *mock.AuditWriter
	logs   *logBuffer
}

func newFixture(t *testing.T, limit int) *fixture {
	t.Helper()
	f := &fixture{repo: &fakeRepo{}, writer: &mock.AuditWriter{}, logs: &logBuffer{}}
	var err error
	f.h, err = commands.NewRegisterEventTypes(commands.Deps{
		Repo: f.repo, Tx: &mock.Transactor{}, Writer: f.writer,
		Limiter: commands.NewRefusedLimiter(limit, nil),
		Log:     slog.New(slog.NewJSONHandler(io.Writer(f.logs), nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

var servicePrincipal = auth.Principal{ID: "helpdesk-svc", Type: auth.PrincipalService, App: "helpdesk"}

func cmdFor(app string) commands.RegisterEventTypes {
	return commands.RegisterEventTypes{
		PathApp: app,
		Registration: domain.Registration{
			Source: "/" + app, App: app,
			EventTypes: []domain.EventTypeDefinition{{
				Type: app + ".ticket.created", Description: "d", DataSchema: json.RawMessage(`{"type":"object"}`),
			}},
		},
		Principal: servicePrincipal,
	}
}

// names makes n names that have exactly length characters.
func names(n, length int, prefix string) []string {
	out := make([]string, n)
	for i := range out {
		base := fmt.Sprintf("helpdesk.%s.%03d", prefix, i)
		out[i] = base + strings.Repeat("x", length-len(base))
	}
	return out
}

func details(t *testing.T, e audit.Entry) map[string]any {
	t.Helper()
	b, err := json.Marshal(e.Details)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func stringList(t *testing.T, v any) []string {
	t.Helper()
	raw, ok := v.([]any)
	if !ok {
		t.Fatalf("value %v is not a list", v)
	}
	out := make([]string, len(raw))
	for i, x := range raw {
		out[i], _ = x.(string)
	}
	return out
}

// U1: a change writes one success entry with the version, the counts, the
// three lists, the flags, and the summary.
func TestU1_SuccessEntry(t *testing.T) {
	f := newFixture(t, 30)
	f.repo.result = domain.Result{
		App: "helpdesk", Version: 4, Changed: true, EventTypeCount: 5,
		Added: 3, Removed: 1, Modified: 2,
		AddedTypes:    []string{"helpdesk.a.one", "helpdesk.a.three", "helpdesk.a.two"},
		RemovedTypes:  []string{"helpdesk.r.one"},
		ModifiedTypes: []string{"helpdesk.m.one", "helpdesk.m.two"},
	}
	if _, err := f.h.Execute(context.Background(), cmdFor("helpdesk")); err != nil {
		t.Fatal(err)
	}
	entries := f.writer.Entries()
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	e := entries[0]
	if e.Action != "events:event_type:register" || e.Result != audit.Success {
		t.Fatalf("action %q, result %q", e.Action, e.Result)
	}
	if e.Target == nil || e.Target.Type != "registration" || e.Target.ID != "helpdesk" {
		t.Fatalf("target = %+v", e.Target)
	}
	if e.Summary != "Event types of app helpdesk registered, version 4" {
		t.Fatalf("summary = %q", e.Summary)
	}
	d := details(t, e)
	for k, want := range map[string]float64{"version": 4, "eventTypeCount": 5, "addedCount": 3, "removedCount": 1, "modifiedCount": 2} {
		if d[k] != want {
			t.Fatalf("details[%s] = %v, want %v", k, d[k], want)
		}
	}
	if got := strings.Join(stringList(t, d["added"]), ","); got != "helpdesk.a.one,helpdesk.a.three,helpdesk.a.two" {
		t.Fatalf("added = %v", got)
	}
	if len(stringList(t, d["removed"])) != 1 || len(stringList(t, d["modified"])) != 2 {
		t.Fatalf("details = %v", d)
	}
	for _, k := range []string{"addedTruncated", "removedTruncated", "modifiedTruncated"} {
		if d[k] != false {
			t.Fatalf("%s = %v, want false", k, d[k])
		}
	}
}

// An unchanged registration writes no entry.
func TestUnchangedWritesNoEntry(t *testing.T) {
	f := newFixture(t, 30)
	f.repo.result = domain.Result{App: "helpdesk", Version: 4}
	if _, err := f.h.Execute(context.Background(), cmdFor("helpdesk")); err != nil {
		t.Fatal(err)
	}
	if n := len(f.writer.Entries()); n != 0 {
		t.Fatalf("entries = %d, want 0", n)
	}
}

// fakeInserter accepts all rows. The test context has no transaction, so it
// says yes always.
type fakeInserter struct{}

func (*fakeInserter) InTransaction(context.Context) bool         { return true }
func (*fakeInserter) Insert(context.Context, []outbox.Row) error { return nil }

// U2: lists of 40 names of 128 characters get cut to 30 names. The counts stay
// complete. The details have 16 KiB or less, and the real writer accepts the
// entry.
func TestU2_ListLimit(t *testing.T) {
	f := newFixture(t, 30)
	f.repo.result = domain.Result{
		App: "helpdesk", Version: 2, Changed: true, EventTypeCount: 80,
		Added: 40, Removed: 40, Modified: 40,
		AddedTypes: names(40, 128, "added"), RemovedTypes: names(40, 128, "removed"), ModifiedTypes: names(40, 128, "modified"),
	}
	if _, err := f.h.Execute(context.Background(), cmdFor("helpdesk")); err != nil {
		t.Fatal(err)
	}
	e := f.writer.Entries()[0]
	d := details(t, e)
	for _, k := range []string{"added", "removed", "modified"} {
		if n := len(stringList(t, d[k])); n != 30 {
			t.Fatalf("%s has %d names", k, n)
		}
		if d[k+"Truncated"] != true {
			t.Fatalf("%sTruncated = %v", k, d[k+"Truncated"])
		}
		if d[k+"Count"] != float64(40) {
			t.Fatalf("%sCount = %v", k, d[k+"Count"])
		}
	}
	if got := stringList(t, d["added"])[0]; got != f.repo.result.AddedTypes[0] {
		t.Fatalf("first added = %q", got)
	}
	b, err := json.Marshal(e.Details)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 16<<10 {
		t.Fatalf("details have %d bytes", len(b))
	}
	w, err := audit.New(&fakeInserter{}, audit.Config{App: "events", ClientID: "events"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(context.Background(), e); err != nil {
		t.Fatalf("the writer refuses the entry: %v", err)
	}
}

// U3: errors of the audit writer.
func TestU3_WriterErrors(t *testing.T) {
	change := domain.Result{App: "helpdesk", Version: 2, Changed: true, EventTypeCount: 1, Added: 1, AddedTypes: []string{"helpdesk.ticket.created"}}

	t.Run("database error gives 503", func(t *testing.T) {
		f := newFixture(t, 30)
		f.repo.result = change
		f.writer.Err = errors.New("audit: insert: connection reset")
		_, err := f.h.Execute(context.Background(), cmdFor("helpdesk"))
		pe, ok := apperrors.AsProblem(err)
		if !ok || pe.Status != 503 || pe.RetryAfter != 5*time.Second {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("rule error gives a plain error and an error log record", func(t *testing.T) {
		f := newFixture(t, 30)
		f.repo.result = change
		f.writer.Err = fmt.Errorf("entry 0: %w: secret-value", audit.ErrInvalidDetails)
		_, err := f.h.Execute(context.Background(), cmdFor("helpdesk"))
		if err == nil {
			t.Fatal("no error")
		}
		if _, ok := apperrors.AsProblem(err); ok {
			t.Fatalf("a rule error must not be a Problem: %v", err)
		}
		if _, ok := apperrors.AsValidationError(err); ok {
			t.Fatalf("a rule error must not be a validation error: %v", err)
		}
		logs := f.logs.String()
		if !strings.Contains(logs, `"level":"ERROR"`) || !strings.Contains(logs, "ErrInvalidDetails") || strings.Contains(logs, "secret-value") {
			t.Fatalf("logs = %s", logs)
		}
	})
	t.Run("writer error on a validation failure keeps 422", func(t *testing.T) {
		f := newFixture(t, 30)
		f.writer.Err = errors.New("audit: insert: connection reset")
		c := cmdFor("helpdesk")
		c.Registration.EventTypes[0].Type = "other.ticket.created"
		_, err := f.h.Execute(context.Background(), c)
		if _, ok := apperrors.AsValidationError(err); !ok {
			t.Fatalf("error = %v", err)
		}
		if logs := f.logs.String(); !strings.Contains(logs, `"level":"ERROR"`) {
			t.Fatalf("logs = %s", logs)
		}
	})
}

// U5: a path app that is not an app key gives an entry with no target and
// appValid false.
func TestU5_InvalidPathApp(t *testing.T) {
	for _, app := range []string{strings.Repeat("a", 300), "app\x00x", "Bad-App"} {
		// An app mismatch.
		f := newFixture(t, 30)
		c := cmdFor("helpdesk")
		c.PathApp = app
		if _, err := f.h.Execute(context.Background(), c); err == nil {
			t.Fatal("no error")
		}
		entries := f.writer.Entries()
		if len(entries) != 1 {
			t.Fatalf("entries = %d", len(entries))
		}
		if e := entries[0]; e.Result != audit.Invalid || e.Target != nil || details(t, e)["appValid"] != false {
			t.Fatalf("entry = %+v, details = %v", e, e.Details)
		}
		// A validation failure with the same path app.
		f2 := newFixture(t, 30)
		c2 := cmdFor("helpdesk")
		c2.PathApp = app
		c2.Registration.App = app
		c2.Principal = auth.Principal{ID: "s", Type: auth.PrincipalService, App: app}
		_, _ = f2.h.Execute(context.Background(), c2)
		if got := f2.writer.Entries(); len(got) != 1 || got[0].Target != nil || details(t, got[0])["appValid"] != false {
			t.Fatalf("validation failure entries = %+v", got)
		}
	}
}

// U6: the details code of an invalid entry.
func TestU6_InvalidCodes(t *testing.T) {
	cases := []struct {
		name string
		prep func(f *fixture, c *commands.RegisterEventTypes)
		want string
	}{
		{"app mismatch", func(_ *fixture, c *commands.RegisterEventTypes) { c.Registration.App = "crm" }, "registration_app_mismatch"},
		{"too many types", func(f *fixture, _ *commands.RegisterEventTypes) { f.repo.err = registrations.ErrTooManyTypes }, "limit_exceeded"},
		{"validation", func(_ *fixture, c *commands.RegisterEventTypes) {
			c.Registration.EventTypes[0].Type = "other.ticket.created"
		}, "event_type_app_mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, 30)
			c := cmdFor("helpdesk")
			tc.prep(f, &c)
			if _, err := f.h.Execute(context.Background(), c); err == nil {
				t.Fatal("no error")
			}
			entries := f.writer.Entries()
			if len(entries) != 1 {
				t.Fatalf("entries = %d", len(entries))
			}
			e := entries[0]
			d := details(t, e)
			if e.Result != audit.Invalid || d["code"] != tc.want || e.Summary != "Registration of event types refused" {
				t.Fatalf("entry = %+v, details = %v", e, d)
			}
			if e.Target == nil || e.Target.ID != "helpdesk" {
				t.Fatalf("target = %+v", e.Target)
			}
			if _, has := d["schema"]; has {
				t.Fatal("the entry has a schema")
			}
		})
	}
}

// U7: the limiter, with a limit of 2.
func TestU7_RefusedLimiter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := commands.NewRefusedLimiter(2, func() time.Time { return now })
	for i, want := range []bool{true, true, false} {
		if got := l.Allow("a"); got != want {
			t.Fatalf("a call %d = %v, want %v", i, got, want)
		}
	}
	if !l.Allow("b") {
		t.Fatal("the second principal has its own window")
	}
	now = now.Add(commands.RefusedWindow)
	if !l.Allow("a") {
		t.Fatal("the next window allows again")
	}

	// A handler with the limit drops the entry and keeps the 4xx result.
	f := newFixture(t, 2)
	c := cmdFor("helpdesk")
	c.Registration.App = "crm"
	for range 3 {
		_, err := f.h.Execute(context.Background(), c)
		if _, ok := apperrors.AsValidationError(err); !ok {
			t.Fatalf("error = %v", err)
		}
	}
	if n := len(f.writer.Entries()); n != 2 {
		t.Fatalf("entries = %d, want 2", n)
	}
	// R3: one warn record names the principal for the window.
	logs := f.logs.String()
	if n := strings.Count(logs, `"level":"WARN"`); n != 1 || !strings.Contains(logs, `"principal_id":"helpdesk-svc"`) {
		t.Fatalf("want one warn record with the principal, logs = %s", logs)
	}
	for range 3 {
		_, _ = f.h.Execute(context.Background(), c)
	}
	if n := strings.Count(f.logs.String(), `"level":"WARN"`); n != 1 {
		t.Fatalf("warn records = %d, want 1 for each window", n)
	}
}

// The limiter keeps at most MaxRefusedPrincipals windows, and it removes the
// oldest first.
func TestRefusedLimiterMemory(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := commands.NewRefusedLimiter(1, func() time.Time { return now })
	l.Allow("first")
	for i := range commands.MaxRefusedPrincipals {
		now = now.Add(time.Millisecond)
		l.Allow(fmt.Sprintf("p%d", i))
	}
	if !l.Allow("first") {
		t.Fatal("the oldest window must be gone")
	}
}

// U8 (command side): AuditDenied writes a denied entry, also when the context
// is canceled.
func TestU8_AuditDeniedCanceledContext(t *testing.T) {
	f := newFixture(t, 30)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.h.AuditDenied(ctx, auth.Principal{ID: "reader-svc", Type: auth.PrincipalService, App: "reader"}, "reader")
	entries := f.writer.Entries()
	if len(entries) != 1 {
		t.Fatalf("entries = %d", len(entries))
	}
	e := entries[0]
	if e.Result != audit.Denied || details(t, e)["code"] != "forbidden" || e.Target == nil || e.Target.ID != "reader" ||
		e.Summary != "Registration of event types denied" {
		t.Fatalf("entry = %+v", e)
	}
}
