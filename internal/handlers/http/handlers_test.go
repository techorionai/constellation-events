package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/techorionai/constellation-platform-go/auth"

	"github.com/techorionai/constellation-events/api/openapi"
	"github.com/techorionai/constellation-events/internal/app/commands"
	httpapi "github.com/techorionai/constellation-events/internal/handlers/http"
	"github.com/techorionai/constellation-events/internal/ports/registrations"
	"github.com/techorionai/constellation-events/resources/test/mock"
)

// fakeVerifier gives the principal of the bearer token value.
type fakeVerifier map[string]auth.Principal

func (v fakeVerifier) Verify(_ context.Context, token string) (auth.Principal, error) {
	p, ok := v[token]
	if !ok {
		return auth.Principal{}, auth.ErrInvalidToken
	}
	return p, nil
}

// panicRepo has no store. A call of it panics, so a test fails when a request reaches the store.
type panicRepo struct{ registrations.Repository }

const registerScope = "events:event_type:register"

func newRegister() (*commands.RegisterEventTypesHandler, error) {
	return commands.NewRegisterEventTypes(commands.Deps{
		Repo: panicRepo{}, Tx: &mock.Transactor{}, Writer: &mock.AuditWriter{},
		Limiter: commands.NewRefusedLimiter(30, nil),
	})
}

func newHandler(t *testing.T) http.Handler {
	t.Helper()
	scopes, err := httpapi.NewScopeTable(openapi.Spec)
	if err != nil {
		t.Fatal(err)
	}
	register, err := newRegister()
	if err != nil {
		t.Fatal(err)
	}
	svc := auth.Principal{ID: "client-1", Type: auth.PrincipalService, App: "helpdesk", Scopes: []string{registerScope}}
	return httpapi.New(httpapi.Deps{
		Register:     register,
		MaxBodyBytes: 4 << 20,
		Scopes:       scopes,
		Verifier: fakeVerifier{
			"user":       {ID: "u1", Type: auth.PrincipalUser, Scopes: []string{registerScope}},
			"noapp":      {ID: "s2", Type: auth.PrincipalService, Scopes: []string{registerScope}},
			"service":    svc,
			"otherapp":   {ID: "s3", Type: auth.PrincipalService, App: "crm", Scopes: []string{registerScope}},
			"noscope":    {ID: "s4", Type: auth.PrincipalService, App: "helpdesk"},
			"readerOnly": {ID: "s5", Type: auth.PrincipalService, App: "reader", Scopes: []string{"events:catalog:read"}},
		},
	}).Handler()
}

func body(app string) string {
	return `{"source":"/` + app + `","app":"` + app + `","eventTypes":[{"type":"` + app + `.ticket.created","description":"d","dataSchema":{"type":"object"}}]}`
}

func do(h http.Handler, method, path, token, payload string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func code(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var p struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return p.Code
}

// U4: a caller that is not a service client of the app gives 422
// registration_app_mismatch.
func TestU4_CallerCheck(t *testing.T) {
	h := newHandler(t)
	cases := []struct {
		name, token, path, payload string
	}{
		{"user principal", "user", "/v1/event-type-registrations/helpdesk", body("helpdesk")},
		{"service with empty app", "noapp", "/v1/event-type-registrations/helpdesk", body("helpdesk")},
		{"app of the token is not the path", "otherapp", "/v1/event-type-registrations/helpdesk", body("helpdesk")},
		{"body app is not the path", "service", "/v1/event-type-registrations/helpdesk", body("crm")},
		{"path app is not the token app", "service", "/v1/event-type-registrations/crm", body("crm")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(h, http.MethodPut, tc.path, tc.token, tc.payload)
			if rec.Code != http.StatusUnprocessableEntity || code(t, rec) != "registration_app_mismatch" {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
		})
	}
	rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "noscope", body("helpdesk"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("no scope: status = %d", rec.Code)
	}
	if rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "", body("helpdesk")); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d", rec.Code)
	}
	// The order of the checks: 403 comes before 400 and 413.
	if rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "noscope", "not json"); rec.Code != http.StatusForbidden {
		t.Fatalf("403 before 400: status = %d", rec.Code)
	}
	if rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "service", "not json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("400 before 422: status = %d", rec.Code)
	}
}

// U5: each of the 5 routes has its scope from x-required-scopes. A route with
// no entry gives 500.
func TestU5_ScopeTable(t *testing.T) {
	table, err := httpapi.NewScopeTable(openapi.Spec)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"GET /v1/event-type-registrations":       "events:catalog:read",
		"PUT /v1/event-type-registrations/{app}": "events:event_type:register",
		"GET /v1/event-type-registrations/{app}": "events:catalog:read",
		"GET /v1/event-types":                    "events:catalog:read",
		"GET /v1/event-types/{type}":             "events:catalog:read",
	}
	if len(table) != len(want) {
		t.Fatalf("table = %v", table)
	}
	for route, scope := range want {
		if got := table[route]; len(got) != 1 || got[0] != scope {
			t.Fatalf("%s: scopes = %v, want %s", route, got, scope)
		}
	}

	// A table with no entry makes each /v1 route fail closed.
	h := httpapi.New(httpapi.Deps{
		Scopes:   httpapi.ScopeTable{},
		Verifier: fakeVerifier{"service": {ID: "s", Type: auth.PrincipalService, App: "helpdesk"}},
	}).Handler()
	for route := range want {
		method, path, _ := strings.Cut(route, " ")
		path = strings.NewReplacer("{app}", "helpdesk", "{type}", "helpdesk.ticket.created").Replace(path)
		if rec := do(h, method, path, "service", ""); rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s: status = %d, want 500", route, rec.Code)
		}
	}

	// A spec with an operation that has no scope list is an error.
	if _, err := httpapi.NewScopeTable([]byte("paths:\n  /v1/x:\n    get:\n      operationId: X_get\n")); err == nil {
		t.Fatal("an operation with no x-required-scopes must be an error")
	}
	if _, err := httpapi.NewScopeTable([]byte("paths:\n  /v1/x:\n    get:\n      operationId: X_get\n      x-required-scopes: []\n")); err == nil {
		t.Fatal("an operation with an empty x-required-scopes must be an error")
	}
}

// U7: the page token holds the operation, the filters, and the last key.
func TestU7_PageToken(t *testing.T) {
	tok := httpapi.EncodePageToken("types", "helpdesk", true, "helpdesk.ticket.created")
	last, err := httpapi.DecodePageToken(tok, "types", "helpdesk", true)
	if err != nil || last != "helpdesk.ticket.created" {
		t.Fatalf("last = %q, err = %v", last, err)
	}
	if last, err := httpapi.DecodePageToken("", "types", "", false); err != nil || last != "" {
		t.Fatalf("empty token: %q, %v", last, err)
	}
	regTok := httpapi.EncodePageToken("registrations", "", false, "helpdesk")
	if last, err := httpapi.DecodePageToken(regTok, "registrations", "", false); err != nil || last != "helpdesk" {
		t.Fatalf("registrations token: %q, %v", last, err)
	}

	bad := map[string]func() (string, error){
		"not base64":   func() (string, error) { return httpapi.DecodePageToken("!!!", "types", "", false) },
		"not json":     func() (string, error) { return httpapi.DecodePageToken("YWJj", "types", "", false) },
		"too long":     func() (string, error) { return httpapi.DecodePageToken(strings.Repeat("a", 1025), "types", "", false) },
		"other app":    func() (string, error) { return httpapi.DecodePageToken(tok, "types", "crm", true) },
		"other remove": func() (string, error) { return httpapi.DecodePageToken(tok, "types", "helpdesk", false) },
		"other op":     func() (string, error) { return httpapi.DecodePageToken(tok, "registrations", "helpdesk", true) },
		"types token on registrations": func() (string, error) {
			return httpapi.DecodePageToken(httpapi.EncodePageToken("types", "", false, "helpdesk.ticket.created"), "registrations", "", false)
		},
		"bad last key": func() (string, error) {
			return httpapi.DecodePageToken(httpapi.EncodePageToken("types", "", false, "Bad.Key"), "types", "", false)
		},
	}
	for name, f := range bad {
		if _, err := f(); err == nil {
			t.Fatalf("%s: want an error", name)
		}
	}

	// Through the router: 400 invalid_page_token.
	h := httpapi.New(httpapi.Deps{
		Scopes:   mustTable(t),
		Verifier: fakeVerifier{"reader": {ID: "r", Type: auth.PrincipalService, App: "reader", Scopes: []string{"events:catalog:read"}}},
	}).Handler()
	rec := do(h, http.MethodGet, "/v1/event-type-registrations?pageToken="+tok, "reader", "")
	if rec.Code != http.StatusBadRequest || code(t, rec) != "invalid_page_token" {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	rec = do(h, http.MethodGet, "/v1/event-types?app=Bad-App", "reader", "")
	if rec.Code != http.StatusBadRequest || code(t, rec) != "invalid_parameter" {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func mustTable(t *testing.T) httpapi.ScopeTable {
	t.Helper()
	table, err := httpapi.NewScopeTable(openapi.Spec)
	if err != nil {
		t.Fatal(err)
	}
	return table
}

// U10: the decode of the register body.
func TestU10_DecodeRegisterBody(t *testing.T) {
	et := `{"type":"helpdesk.ticket.created","description":"d","dataSchema":{"type":"object"}}`
	cases := []struct {
		name, body string
		wantErr    bool
	}{
		{"not JSON", `not json`, true},
		{"data after the object", body("helpdesk") + ` {}`, true},
		{"unknown field at the top level", `{"source":"/helpdesk","app":"helpdesk","eventTypes":[` + et + `],"extra":1}`, true},
		{"unknown field in an event type", `{"source":"/helpdesk","app":"helpdesk","eventTypes":[{"type":"helpdesk.ticket.created","description":"d","dataSchema":{},"deprecatd":true}]}`, true},
		{"no dataSchema", `{"source":"/helpdesk","app":"helpdesk","eventTypes":[{"type":"helpdesk.ticket.created","description":"d"}]}`, true},
		{"eventTypes as a string", `{"source":"/helpdesk","app":"helpdesk","eventTypes":"x"}`, true},
		{"no source", `{"app":"helpdesk","eventTypes":[` + et + `]}`, true},
		{"description as a number", `{"source":"/helpdesk","app":"helpdesk","eventTypes":[{"type":"helpdesk.ticket.created","description":1,"dataSchema":{}}]}`, true},
		{"unknown member inside a dataSchema", `{"source":"/helpdesk","app":"helpdesk","eventTypes":[{"type":"helpdesk.ticket.created","description":"d","dataSchema":{"x-unknown":{"a":1}}}]}`, false},
		{"dataSchema as an array passes the decode", `{"source":"/helpdesk","app":"helpdesk","eventTypes":[{"type":"helpdesk.ticket.created","description":"d","dataSchema":[]}]}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := httpapi.DecodeRegisterBody([]byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error = %v", err, tc.wantErr)
			}
		})
	}

	// Through the router: 400 for the first six, 422 for the array.
	h := newHandler(t)
	for _, tc := range cases[:6] {
		rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "service", tc.body)
		if rec.Code != http.StatusBadRequest || code(t, rec) != "invalid_request" {
			t.Fatalf("%s: status = %d, body = %s", tc.name, rec.Code, rec.Body.String())
		}
	}
	rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "service", cases[9].body)
	if rec.Code != http.StatusUnprocessableEntity || code(t, rec) != "invalid_data_schema" {
		t.Fatalf("array: status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var p struct {
		Errors []struct{ Field, Code, Message string } `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || len(p.Errors) != 1 || p.Errors[0].Field != "/eventTypes/0/dataSchema" {
		t.Fatalf("errors = %+v, err = %v", p, err)
	}
}

// The body limit gives 413 after the scope check.
func TestBodyLimit(t *testing.T) {
	scopes := mustTable(t)
	register, err := newRegister()
	if err != nil {
		t.Fatal(err)
	}
	h := httpapi.New(httpapi.Deps{
		Register: register, MaxBodyBytes: 1 << 10, Scopes: scopes,
		Verifier: fakeVerifier{
			"service": {ID: "s", Type: auth.PrincipalService, App: "helpdesk", Scopes: []string{registerScope}},
			"noscope": {ID: "s", Type: auth.PrincipalService, App: "helpdesk"},
		},
	}).Handler()
	big := strings.Repeat("a", 2<<10)
	rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "service", big)
	if rec.Code != http.StatusRequestEntityTooLarge || code(t, rec) != "content_too_large" {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "noscope", big); rec.Code != http.StatusForbidden {
		t.Fatalf("403 before 413: status = %d", rec.Code)
	}
}

// U8: the denied step writes the entry before RequireScopes sends the 403, also
// when the client has canceled the request. A principal with the scope writes
// no denied entry.
func TestU8_DeniedStep(t *testing.T) {
	writer := &mock.AuditWriter{}
	register, err := commands.NewRegisterEventTypes(commands.Deps{
		Repo: panicRepo{}, Tx: &mock.Transactor{}, Writer: writer, Limiter: commands.NewRefusedLimiter(30, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	h := httpapi.New(httpapi.Deps{
		Register: register, MaxBodyBytes: 4 << 20, Scopes: mustTable(t), AuditDenied: register.AuditDenied,
		Verifier: fakeVerifier{
			"reader-svc": {ID: "reader-svc", Type: auth.PrincipalService, App: "reader", Scopes: []string{"events:catalog:read"}},
			"service":    {ID: "helpdesk-svc", Type: auth.PrincipalService, App: "helpdesk", Scopes: []string{registerScope}},
		},
	}).Handler()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPut, "/v1/event-type-registrations/reader", strings.NewReader(body("reader")))
	req.Header.Set("Authorization", "Bearer reader-svc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	entries := writer.Entries()
	if len(entries) != 1 || entries[0].Result != "denied" || entries[0].Target == nil || entries[0].Target.ID != "reader" {
		t.Fatalf("entries = %+v", entries)
	}

	// A principal with the scope writes no denied entry. The store has no data,
	// so the handler stops with a panic of panicRepo. A bad body stops earlier.
	if rec := do(h, http.MethodPut, "/v1/event-type-registrations/helpdesk", "service", "not json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
	if n := len(writer.Entries()); n != 1 {
		t.Fatalf("entries = %d, want 1", n)
	}
}
