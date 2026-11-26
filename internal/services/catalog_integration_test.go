//go:build integration

package services_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/techorionai/constellation-platform-go/eventtypes"

	"github.com/techorionai/constellation-events/resources/test/testserver"
)

// I9: the list of the types has pages in the order of the type, and the filter works.
func TestI9_ListPages(t *testing.T) {
	srv := started(t)
	var helpdesk, crm []typ
	for i := range 120 {
		helpdesk = append(helpdesk, newType(fmt.Sprintf("helpdesk.item.n%03d", i)))
	}
	for i := range 10 {
		crm = append(crm, newType(fmt.Sprintf("crm.item.n%03d", i)))
	}
	registerOK(t, srv, "helpdesk-svc", "helpdesk", helpdesk...)
	registerOK(t, srv, "crm-svc", "crm", crm...)

	var sizes []int
	var all []string
	query := "?pageSize=50&app=helpdesk"
	for range 5 {
		page := listTypes(t, srv, query)
		sizes = append(sizes, len(page.Items))
		for _, it := range page.Items {
			if it.App != "helpdesk" {
				t.Fatalf("type of app %s in a helpdesk list", it.App)
			}
			all = append(all, it.Type)
		}
		if page.NextPageToken == "" {
			break
		}
		query = "?pageSize=50&app=helpdesk&pageToken=" + page.NextPageToken
	}
	if fmt.Sprint(sizes) != "[50 50 20]" {
		t.Fatalf("page sizes = %v, want [50 50 20]", sizes)
	}
	for i := 1; i < len(all); i++ {
		if all[i-1] >= all[i] {
			t.Fatalf("the types are not in order: %s, %s", all[i-1], all[i])
		}
	}
	if n := len(listTypes(t, srv, "?app=crm").Items); n != 10 {
		t.Fatalf("crm filter: %d types", n)
	}
}

// I10: a removed type has removedAt. An unknown type gives 404.
func TestI10_GetRemovedAndUnknown(t *testing.T) {
	srv := started(t)
	a, b := newType("helpdesk.ticket.created"), newType("helpdesk.ticket.closed")
	registerOK(t, srv, "helpdesk-svc", "helpdesk", a, b)
	registerOK(t, srv, "helpdesk-svc", "helpdesk", a)

	resp := read(t, srv, "reader-svc", "/v1/event-types/"+b.name)
	var ct catalogType
	resp.JSON(t, &ct)
	if resp.Status != http.StatusOK || ct.RemovedAt == nil || ct.Source != "/helpdesk" || ct.App != "helpdesk" {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	resp = read(t, srv, "reader-svc", "/v1/event-types/helpdesk.ticket.unknown")
	if resp.Status != http.StatusNotFound || resp.Code(t) != "event_type_not_found" {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
}

// I11: with PostgreSQL stopped, a registration gives 503 with Retry-After.
func TestI11_RegisterWithoutDatabase(t *testing.T) {
	db, stop := env.NewIsolatedDB(t)
	srv := started(t, testserver.WithDB(db))
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	stop()
	resp := srv.Do(t, http.MethodPut, registerPath("helpdesk"), token, registration("helpdesk", newType("helpdesk.ticket.created")))
	if resp.Status != http.StatusServiceUnavailable || resp.Code(t) != "unavailable" || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status = %d, Retry-After = %q, body = %s", resp.Status, resp.Header.Get("Retry-After"), resp.Body)
	}
}

// I12: two instances that start at the same time on one database run the
// migration one time, and both are ready.
func TestI12_ParallelStart(t *testing.T) {
	db := env.NewDB(t)
	var wg sync.WaitGroup
	servers := make([]*testserver.Server, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			servers[i], errs[i] = env.StartE(t, testserver.WithDB(db))
		}()
	}
	wg.Wait()
	for i := range 2 {
		if errs[i] != nil {
			t.Fatalf("instance %d: %v", i, errs[i])
		}
		t.Cleanup(servers[i].Stop)
		servers[i].WaitReady(t)
	}
	conn, err := pgx.Connect(context.Background(), db.URL.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var rows, version int
	var dirty bool
	if err := conn.QueryRow(context.Background(), `SELECT count(*), max(version), bool_or(dirty) FROM schema_migrations`).Scan(&rows, &version, &dirty); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || version != 1 || dirty {
		t.Fatalf("schema_migrations: rows = %d, version = %d, dirty = %v", rows, version, dirty)
	}
}

// I14: no log record has a JSON Schema, a description, a compiler text, a
// token, or a private key. A change or a refused registration writes one
// record with the action. An unchanged registration writes none.
func TestI14_Logs(t *testing.T) {
	logger, capture := testserver.NewLogCapture()
	srv := started(t, testserver.WithLogger(logger))
	const schemaMarker, descMarker = "SCHEMA_MARKER_7731", "DESCRIPTION_MARKER_5519"
	a := typ{name: "helpdesk.ticket.created", description: descMarker, schema: `{"type":"object","title":"` + schemaMarker + `"}`}
	b := newType("helpdesk.ticket.closed")
	count := func() int { return len(capture.Records("events:event_type:register")) }

	// I2: the first registration, with the publisher of platform-go.
	reg, err := eventtypes.Load(fsOf("event-types.json", string(registration("helpdesk", a, b))), "event-types.json")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client := eventsClient(t, srv, "helpdesk-svc")
	if _, err := client.Register(ctx, reg); err != nil {
		t.Fatal(err)
	}
	if count() != 1 {
		t.Fatalf("after the first registration: %d records, want 1", count())
	}
	// I3: the same registration writes no record.
	if _, err := client.Register(ctx, reg); err != nil {
		t.Fatal(err)
	}
	if count() != 1 {
		t.Fatalf("after the same registration: %d records, want 1", count())
	}
	// I4: a change writes one record.
	registerOK(t, srv, "helpdesk-svc", "helpdesk", a)
	if count() != 2 {
		t.Fatalf("after a change: %d records, want 2", count())
	}
	// I5: a registration for another app writes one record.
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	if resp := srv.Do(t, http.MethodPut, registerPath("crm"), token, registration("crm", newType("crm.ticket.created"))); resp.Status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", resp.Status)
	}
	if count() != 3 {
		t.Fatalf("after the app mismatch: %d records, want 3", count())
	}
	// I18: a reference to a file writes one record.
	bad := typ{name: "helpdesk.ticket.bad", description: descMarker, schema: `{"$ref":"file:///etc/passwd"}`}
	if resp := register(t, srv, "helpdesk-svc", "helpdesk", a, bad); resp.Status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", resp.Status)
	}
	if count() != 4 {
		t.Fatalf("after the bad schema: %d records, want 4", count())
	}
	// A 403 writes one record with the result denied.
	readerToken := env.Token(t, "reader-svc", "events", readScope)
	if resp := srv.Do(t, http.MethodPut, registerPath("reader"), readerToken, registration("reader", newType("reader.ticket.created"))); resp.Status != http.StatusForbidden {
		t.Fatalf("status = %d", resp.Status)
	}
	if count() != 5 {
		t.Fatalf("after the 403: %d records, want 5", count())
	}

	text := capture.Text()
	for _, forbidden := range []string{schemaMarker, descMarker, "etc/passwd", "a reference to", "PRIVATE KEY", token, readerToken, env.Clients["helpdesk-svc"].PEM[40:80]} {
		if forbidden != "" && strings.Contains(text, forbidden) {
			t.Fatalf("the log has %q", forbidden)
		}
	}
	results := map[string]int{}
	for _, r := range capture.Records("events:event_type:register") {
		results[fmt.Sprint(r["result"])]++
	}
	if results["changed"] != 2 || results["invalid"] != 2 || results["denied"] != 1 {
		t.Fatalf("results = %v", results)
	}
}

// I15: a body of 5 MiB gives 413 and changes no row.
func TestI15_BodyTooLarge(t *testing.T) {
	srv := started(t)
	big := newType("helpdesk.ticket.created")
	big.description = strings.Repeat("a", 5<<20)
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	resp := srv.Do(t, http.MethodPut, registerPath("helpdesk"), token, registration("helpdesk", big))
	if resp.Status != http.StatusRequestEntityTooLarge || resp.Code(t) != "content_too_large" {
		t.Fatalf("status = %d, body = %.200s", resp.Status, resp.Body)
	}
	assertEmptyCatalog(t, srv)
}

func assertEmptyCatalog(t *testing.T, srv *testserver.Server) {
	t.Helper()
	if n := len(listTypes(t, srv, "?includeRemoved=true").Items); n != 0 {
		t.Fatalf("the catalog has %d types, want 0", n)
	}
	var page struct {
		Items []json.RawMessage `json:"items"`
	}
	read(t, srv, "reader-svc", "/v1/event-type-registrations").JSON(t, &page)
	if len(page.Items) != 0 {
		t.Fatalf("the catalog has %d registrations, want 0", len(page.Items))
	}
}

// I16: a value that does not match its pattern gives 404 or 400.
func TestI16_BadPatterns(t *testing.T) {
	srv := started(t)
	cases := []struct {
		path   string
		status int
		code   string
	}{
		{"/v1/event-types/Bad.Type", http.StatusNotFound, "event_type_not_found"},
		{"/v1/event-type-registrations/Bad-App", http.StatusNotFound, "registration_not_found"},
		{"/v1/event-types?app=Bad-App", http.StatusBadRequest, "invalid_parameter"},
		{"/v1/event-types?pageSize=201", http.StatusBadRequest, "invalid_parameter"},
		{"/v1/event-type-registrations?pageSize=11", http.StatusBadRequest, "invalid_parameter"},
	}
	for _, tc := range cases {
		resp := read(t, srv, "reader-svc", tc.path)
		if resp.Status != tc.status || resp.Code(t) != tc.code {
			t.Fatalf("%s: status = %d, body = %s", tc.path, resp.Status, resp.Body)
		}
	}
}

// I17: with PostgreSQL stopped, a list gives 503 with Retry-After.
func TestI17_ListWithoutDatabase(t *testing.T) {
	db, stop := env.NewIsolatedDB(t)
	srv := started(t, testserver.WithDB(db))
	token := env.Token(t, "reader-svc", "events", readScope)
	stop()
	resp := srv.Do(t, http.MethodGet, "/v1/event-types", token, nil)
	if resp.Status != http.StatusServiceUnavailable || resp.Code(t) != "unavailable" || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("status = %d, Retry-After = %q, body = %s", resp.Status, resp.Header.Get("Retry-After"), resp.Body)
	}
}

// I18: a reference to a file gives the same 422 for a file that exists and
// for a file that does not exist. No row changes.
func TestI18_FileReference(t *testing.T) {
	srv := started(t)
	answer := func(ref string) map[string]any {
		bad := typ{name: "helpdesk.ticket.created", description: "d", schema: `{"$ref":"` + ref + `"}`}
		resp := register(t, srv, "helpdesk-svc", "helpdesk", bad)
		if resp.Status != http.StatusUnprocessableEntity || resp.Code(t) != "invalid_data_schema" {
			t.Fatalf("%s: status = %d, body = %s", ref, resp.Status, resp.Body)
		}
		var m map[string]any
		resp.JSON(t, &m)
		delete(m, "requestId")
		delete(m, "traceId")
		return m
	}
	first, second := answer("file:///etc/passwd"), answer("file:///does/not/exist")
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if string(a) != string(b) {
		t.Fatalf("the answers differ:\n%s\n%s", a, b)
	}
	assertEmptyCatalog(t, srv)
}

// I19: the read permission and the register permission are separate.
func TestI19_Permissions(t *testing.T) {
	srv := started(t)
	registerOK(t, srv, "helpdesk-svc", "helpdesk", newType("helpdesk.ticket.created"))

	if resp := read(t, srv, "reader-svc", "/v1/event-types"); resp.Status != http.StatusOK {
		t.Fatalf("reader list: status = %d, body = %s", resp.Status, resp.Body)
	}
	if resp := read(t, srv, "reader-svc", "/v1/event-type-registrations/helpdesk"); resp.Status != http.StatusOK {
		t.Fatalf("reader get: status = %d, body = %s", resp.Status, resp.Body)
	}
	readerToken := env.Token(t, "reader-svc", "events", readScope)
	resp := srv.Do(t, http.MethodPut, registerPath("reader"), readerToken, registration("reader", newType("reader.ticket.created")))
	assertForbidden(t, resp, registerScope)

	// A token with the register scope only has no read scope.
	registerOnly := env.Token(t, "helpdesk-svc", "events", registerScope)
	assertForbidden(t, srv.Do(t, http.MethodGet, "/v1/event-types", registerOnly, nil), readScope)
	assertForbidden(t, srv.Do(t, http.MethodGet, "/v1/event-type-registrations", registerOnly, nil), readScope)
}

// I20: a number of a schema keeps its text.
func TestI20_NumberText(t *testing.T) {
	srv := started(t)
	n := newType("helpdesk.ticket.created")
	n.schema = `{"type":"object","properties":{"a":{"type":"integer","maximum":9007199254740993},"b":{"type":"number","multipleOf":0.1}}}`
	registerOK(t, srv, "helpdesk-svc", "helpdesk", n)
	resp := read(t, srv, "reader-svc", "/v1/event-types/"+n.name)
	body := string(resp.Body)
	if resp.Status != http.StatusOK || !strings.Contains(body, "9007199254740993") || !strings.Contains(body, "0.1") {
		t.Fatalf("status = %d, body = %s", resp.Status, body)
	}
	if strings.Contains(body, "9.007199254740992e+15") {
		t.Fatalf("the number lost its text: %s", body)
	}
}

// I21: an unknown field of an event type gives 400 and changes no row.
func TestI21_UnknownField(t *testing.T) {
	srv := started(t)
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	body := `{"source":"/helpdesk","app":"helpdesk","eventTypes":[{"type":"helpdesk.ticket.created","description":"d","dataSchema":{},"deprecatd":true}]}`
	resp := srv.Do(t, http.MethodPut, registerPath("helpdesk"), token, []byte(body))
	if resp.Status != http.StatusBadRequest || resp.Code(t) != "invalid_request" {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	assertEmptyCatalog(t, srv)
}

// I22: a page of registrations is not larger than 1 MiB, except a page of one
// registration. The token of one operation does not work on the other.
func TestI22_RegistrationPages(t *testing.T) {
	srv := started(t)
	big := func(app string) []typ {
		var types []typ
		for i := range 10 {
			schema := `{"description":"` + strings.Repeat("x", 60<<10) + `"}`
			types = append(types, typ{name: fmt.Sprintf("%s.item.n%d", app, i), description: "d", schema: schema})
		}
		return types
	}
	registerOK(t, srv, "helpdesk-svc", "helpdesk", big("helpdesk")...)
	registerOK(t, srv, "crm-svc", "crm", big("crm")...)
	registerOK(t, srv, "sales-svc", "sales", big("sales")...)

	var apps []string
	path := "/v1/event-type-registrations?pageSize=10"
	var firstToken string
	for range 5 {
		resp := read(t, srv, "reader-svc", path)
		if resp.Status != http.StatusOK {
			t.Fatalf("status = %d, body = %.200s", resp.Status, resp.Body)
		}
		var page struct {
			Items []struct {
				Registration struct {
					App string `json:"app"`
				} `json:"registration"`
			} `json:"items"`
			NextPageToken string `json:"nextPageToken"`
		}
		resp.JSON(t, &page)
		if len(resp.Body) > 1<<20 && len(page.Items) != 1 {
			t.Fatalf("a page of %d registrations has %d bytes", len(page.Items), len(resp.Body))
		}
		for _, it := range page.Items {
			apps = append(apps, it.Registration.App)
		}
		if page.NextPageToken == "" {
			break
		}
		if firstToken == "" {
			firstToken = page.NextPageToken
		}
		path = "/v1/event-type-registrations?pageSize=10&pageToken=" + page.NextPageToken
	}
	if fmt.Sprint(apps) != "[crm helpdesk sales]" {
		t.Fatalf("apps = %v, want [crm helpdesk sales]", apps)
	}
	resp := read(t, srv, "reader-svc", "/v1/event-types?pageToken="+firstToken)
	if resp.Status != http.StatusBadRequest || resp.Code(t) != "invalid_page_token" {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
}

// An app has a maximum of 1000 types in the catalog, removed types included.
func TestTypesPerAppLimit(t *testing.T) {
	srv := started(t)
	set := func(n int) []typ {
		var types []typ
		for i := range 200 {
			types = append(types, newType(fmt.Sprintf("helpdesk.set%d.n%03d", n, i)))
		}
		return types
	}
	for n := range 5 {
		registerOK(t, srv, "helpdesk-svc", "helpdesk", set(n)...)
	}
	resp := register(t, srv, "helpdesk-svc", "helpdesk", set(5)...)
	if resp.Status != http.StatusUnprocessableEntity || resp.Code(t) != "limit_exceeded" {
		t.Fatalf("status = %d, body = %.300s", resp.Status, resp.Body)
	}
	// The same set again as one of the removed sets is fine.
	registerOK(t, srv, "helpdesk-svc", "helpdesk", set(0)...)
}
