//go:build integration

package services_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/techorionai/constellation-events/resources/test/testserver"
)

const auditStream = "audit.stream:events"

// auditEvent is the CloudEvent of an audit entry.
type auditEvent struct {
	Source    string `json:"source"`
	Type      string `json:"type"`
	Actor     string `json:"actor"`
	ActorType string `json:"actortype"`
	Data      struct {
		Action  string `json:"action"`
		Result  string `json:"result"`
		Summary string `json:"summary"`
		Target  *struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		} `json:"target"`
		Details   map[string]any `json:"details"`
		RequestID string         `json:"requestId"`
		UserAgent string         `json:"userAgent"`
	} `json:"data"`
}

// auditReader reads the entries that come after its start on the audit stream
// of the events service. The stream is shared by all tests of the package, so
// each test reads only the entries after the last ID at its start.
type auditReader struct {
	t      *testing.T
	client valkeygo.Client
	after  string
}

func newAuditReader(t *testing.T) *auditReader {
	t.Helper()
	c := env.ValkeyAdmin(t)
	r := &auditReader{t: t, client: c, after: "0-0"}
	res, err := c.Do(context.Background(), c.B().Xrevrange().Key(auditStream).End("+").Start("-").Count(1).Build()).AsXRange()
	if err == nil && len(res) == 1 {
		r.after = res[0].ID
	}
	return r
}

func (r *auditReader) read() []auditEvent {
	r.t.Helper()
	entries, err := r.client.Do(context.Background(),
		r.client.B().Xrange().Key(auditStream).Start("("+r.after).End("+").Build()).AsXRange()
	if err != nil && !valkeygo.IsValkeyNil(err) {
		r.t.Fatalf("read the audit stream: %v", err)
	}
	var out []auditEvent
	for _, e := range entries {
		var ev auditEvent
		if err := json.Unmarshal([]byte(e.FieldValues["event"]), &ev); err != nil {
			r.t.Fatalf("audit entry %q is not JSON: %v", e.FieldValues["event"], err)
		}
		out = append(out, ev)
	}
	return out
}

// wait waits until the stream has at least n entries after the start, and
// returns all of them.
func (r *auditReader) wait(n int) []auditEvent {
	r.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		got := r.read()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("the audit stream has %d entries after 30 seconds, want %d", len(got), n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// settle waits a short time, then returns the entries. A test uses it to check
// that no more entries arrive.
func (r *auditReader) settle() []auditEvent {
	time.Sleep(1500 * time.Millisecond)
	return r.read()
}

func connect(t *testing.T, srv *testserver.Server) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), srv.DB.URL.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func outboxRows(t *testing.T, srv *testserver.Server) int {
	t.Helper()
	var n int
	if err := connect(t, srv).QueryRow(context.Background(), `SELECT count(*) FROM platform_audit_outbox`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func stringsOf(t *testing.T, v any) []string {
	t.Helper()
	list, ok := v.([]any)
	if !ok {
		t.Fatalf("%v is not a list", v)
	}
	out := make([]string, len(list))
	for i, x := range list {
		out[i], _ = x.(string)
	}
	return out
}

// I1: the service starts on a new database. The audit outbox table exists and
// /readyz is 200.
func TestAudit_I1_Startup(t *testing.T) {
	srv := started(t)
	var table *string
	if err := connect(t, srv).QueryRow(context.Background(), `SELECT to_regclass('platform_audit_outbox')::text`).Scan(&table); err != nil {
		t.Fatal(err)
	}
	if table == nil {
		t.Fatal("the table platform_audit_outbox does not exist")
	}
	if srv.Ready() != http.StatusOK {
		t.Fatalf("/readyz = %d", srv.Ready())
	}
}

// I2: a registration of 3 types, the same body, and a body without one type.
func TestAudit_I2_SuccessEntries(t *testing.T) {
	logger, capture := testserver.NewLogCapture()
	srv := started(t, testserver.WithLogger(logger), testserver.WithValkeyACL())
	reader := newAuditReader(t)
	a, b, c := newType("helpdesk.ticket.created"), newType("helpdesk.ticket.closed"), newType("helpdesk.ticket.moved")
	registerOK(t, srv, "helpdesk-svc", "helpdesk", a, b, c)
	registerOK(t, srv, "helpdesk-svc", "helpdesk", a, b, c)
	registerOK(t, srv, "helpdesk-svc", "helpdesk", a, b)

	events := reader.wait(2)
	if n := outboxRows(t, srv); n != 2 {
		t.Fatalf("outbox rows = %d, want 2", n)
	}
	if got := reader.settle(); len(got) != 2 {
		t.Fatalf("entries = %d, want 2 (no entry for the same body)", len(got))
	}
	first, second := events[0], events[1]
	for _, e := range events {
		if e.Actor != "helpdesk-svc" || e.ActorType != "service" || e.Data.Action != "events:event_type:register" ||
			e.Data.Result != "success" || e.Data.Target == nil || e.Data.Target.Type != "registration" || e.Data.Target.ID != "helpdesk" {
			t.Fatalf("entry = %+v", e)
		}
	}
	if first.Data.Summary != "Event types of app helpdesk registered, version 1" || first.Data.Details["version"] != float64(1) {
		t.Fatalf("first = %+v", first.Data)
	}
	added := stringsOf(t, first.Data.Details["added"])
	if first.Data.Details["addedCount"] != float64(3) || len(added) != 3 || !sort.StringsAreSorted(added) {
		t.Fatalf("added = %v, details = %v", added, first.Data.Details)
	}
	if second.Data.Details["version"] != float64(2) || second.Data.Details["removedCount"] != float64(1) ||
		strings.Join(stringsOf(t, second.Data.Details["removed"]), ",") != "helpdesk.ticket.moved" ||
		second.Data.Details["eventTypeCount"] != float64(2) {
		t.Fatalf("second = %+v", second.Data)
	}
	// The names in the lists match the catalog rows.
	var catalog []string
	for _, it := range listTypes(t, srv, "?includeRemoved=true").Items {
		catalog = append(catalog, it.Type)
	}
	sort.Strings(catalog)
	if strings.Join(catalog, ",") != strings.Join(added, ",") {
		t.Fatalf("catalog = %v, added = %v", catalog, added)
	}
	if capture.Text() == "" {
		t.Fatal("the capture has no log")
	}
}

// I3: a registration with a bad dataSchema gives 422 and one invalid entry.
func TestAudit_I3_InvalidEntry(t *testing.T) {
	srv := started(t)
	reader := newAuditReader(t)
	bad := typ{name: "helpdesk.ticket.bad", description: "d", schema: `{"$ref":"file:///etc/passwd"}`}
	resp := register(t, srv, "helpdesk-svc", "helpdesk", newType("helpdesk.ticket.created"), bad)
	if resp.Status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	events := reader.wait(1)
	e := events[0]
	if e.Data.Result != "invalid" || e.Data.Details["code"] != resp.Code(t) || e.Data.Summary != "Registration of event types refused" ||
		e.Actor != "helpdesk-svc" || e.Data.Target == nil || e.Data.Target.ID != "helpdesk" {
		t.Fatalf("entry = %+v, code = %s", e, resp.Code(t))
	}
	if text, _ := json.Marshal(e); strings.Contains(string(text), "passwd") {
		t.Fatalf("the entry has the schema: %s", text)
	}
	if got := reader.settle(); len(got) != 1 {
		t.Fatalf("entries = %d, want 1", len(got))
	}
}

// I4: a client with no register scope gets 403 and one denied entry.
func TestAudit_I4_DeniedEntry(t *testing.T) {
	srv := started(t)
	reader := newAuditReader(t)
	token := env.Token(t, "reader-svc", "events", readScope)
	resp := srv.Do(t, http.MethodPut, registerPath("reader"), token, registration("reader", newType("reader.ticket.created")))
	assertForbidden(t, resp, registerScope)
	e := reader.wait(1)[0]
	if e.Data.Result != "denied" || e.Actor != "reader-svc" || e.Data.Target == nil || e.Data.Target.ID != "reader" ||
		e.Data.Details["code"] != "forbidden" || e.Data.Summary != "Registration of event types denied" {
		t.Fatalf("entry = %+v", e)
	}
	if e.Data.RequestID == "" {
		t.Fatal("the entry has no request ID")
	}
}

// I5: when the audit insert fails, the registration does not change.
func TestAudit_I5_InsertFails(t *testing.T) {
	srv := started(t)
	registerOK(t, srv, "helpdesk-svc", "helpdesk", newType("helpdesk.ticket.created"))
	conn := connect(t, srv)
	ctx := context.Background()
	for _, q := range []string{
		`CREATE FUNCTION audit_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'refused by the test'; END $$`,
		`CREATE TRIGGER audit_refuse BEFORE INSERT ON platform_audit_outbox FOR EACH ROW EXECUTE FUNCTION audit_refuse()`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	resp := register(t, srv, "helpdesk-svc", "helpdesk", newType("helpdesk.ticket.created"), newType("helpdesk.ticket.closed"))
	if resp.Status != http.StatusServiceUnavailable || resp.Code(t) != "unavailable" || resp.Header.Get("Retry-After") != "5" {
		t.Fatalf("status = %d, Retry-After = %q, body = %s", resp.Status, resp.Header.Get("Retry-After"), resp.Body)
	}
	var reg struct {
		Version int `json:"version"`
	}
	read(t, srv, "reader-svc", "/v1/event-type-registrations/helpdesk").JSON(t, &reg)
	if reg.Version != 1 {
		t.Fatalf("version = %d, want 1", reg.Version)
	}
	if n := len(listTypes(t, srv, "?includeRemoved=true").Items); n != 1 {
		t.Fatalf("the catalog has %d types, want 1", n)
	}
	// A refused request keeps its 4xx response when the audit insert fails.
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	if resp := srv.Do(t, http.MethodPut, registerPath("crm"), token, registration("crm", newType("crm.ticket.created"))); resp.Status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d", resp.Status)
	}
}

// I6: no log record has the old action record or private data.
func TestAudit_I6_Logs(t *testing.T) {
	logger, capture := testserver.NewLogCapture()
	srv := started(t, testserver.WithLogger(logger))
	const schemaMarker, descMarker = "SCHEMA_MARKER_7731", "DESCRIPTION_MARKER_5519"
	a := typ{name: "helpdesk.ticket.created", description: descMarker, schema: `{"type":"object","title":"` + schemaMarker + `"}`}
	registerOK(t, srv, "helpdesk-svc", "helpdesk", a)
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	srv.Do(t, http.MethodPut, registerPath("crm"), token, registration("crm", newType("crm.ticket.created")))
	readerToken := env.Token(t, "reader-svc", "events", readScope)
	srv.Do(t, http.MethodPut, registerPath("reader"), readerToken, registration("reader", newType("reader.ticket.created")))
	text := capture.Text()
	for _, forbidden := range []string{"events.event_type.register", schemaMarker, descMarker, "PRIVATE KEY", token, readerToken, env.Clients["helpdesk-svc"].PEM[40:80]} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("the log has %q", forbidden)
		}
	}
}

// I7: a registration for another app gives 422 and one invalid entry.
func TestAudit_I7_AppMismatch(t *testing.T) {
	srv := started(t)
	reader := newAuditReader(t)
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	resp := srv.Do(t, http.MethodPut, registerPath("other"), token, registration("other", newType("other.ticket.created")))
	if resp.Status != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	e := reader.wait(1)[0]
	if e.Data.Result != "invalid" || e.Data.Details["code"] != "registration_app_mismatch" || e.Actor != "helpdesk-svc" {
		t.Fatalf("entry = %+v", e)
	}
}

// I8: more types than the limit of an app give 422 limit_exceeded and one
// invalid entry. The version does not change.
func TestAudit_I8_TooManyTypes(t *testing.T) {
	srv := started(t)
	reader := newAuditReader(t)
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
	events := reader.wait(6)
	last := events[5]
	if last.Data.Result != "invalid" || last.Data.Details["code"] != "limit_exceeded" {
		t.Fatalf("last = %+v", last.Data)
	}
	// The success entries have lists that the limit cut to 30 names.
	second := events[1].Data.Details
	if len(stringsOf(t, second["added"])) != 30 || second["addedTruncated"] != true || second["addedCount"] != float64(200) ||
		second["removedTruncated"] != true || len(stringsOf(t, second["removed"])) != 30 {
		t.Fatalf("details = %v", second)
	}
	var reg struct {
		Version int `json:"version"`
	}
	read(t, srv, "reader-svc", "/v1/event-type-registrations/helpdesk").JSON(t, &reg)
	if reg.Version != 5 {
		t.Fatalf("version = %d, want 5", reg.Version)
	}
}

// I9: two registrations of one app at the same time give two entries with
// different versions that agree with the stored versions.
func TestAudit_I9_Parallel(t *testing.T) {
	srv := started(t)
	reader := newAuditReader(t)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token := env.Token(t, "helpdesk-svc", "events", registerScope)
			resp := srv.Do(t, http.MethodPut, registerPath("helpdesk"), token,
				registration("helpdesk", newType(fmt.Sprintf("helpdesk.item.v%d", i))))
			if resp.Status != http.StatusOK {
				t.Errorf("status = %d, body = %s", resp.Status, resp.Body)
			}
		}()
	}
	wg.Wait()
	events := reader.wait(2)
	if got := reader.settle(); len(got) != 2 {
		t.Fatalf("entries = %d, want 2", len(got))
	}
	versions := []float64{events[0].Data.Details["version"].(float64), events[1].Data.Details["version"].(float64)}
	sort.Float64s(versions)
	if versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("versions = %v", versions)
	}
	var reg struct {
		Version int `json:"version"`
	}
	read(t, srv, "reader-svc", "/v1/event-type-registrations/helpdesk").JSON(t, &reg)
	if reg.Version != 2 {
		t.Fatalf("stored version = %d", reg.Version)
	}
}

// I10: 32 denied requests in one minute give 32 responses of 403 and 30 denied
// entries.
func TestAudit_I10_RefusedLimit(t *testing.T) {
	srv := started(t)
	reader := newAuditReader(t)
	token := env.Token(t, "reader-svc", "events", readScope)
	for i := range 32 {
		resp := srv.Do(t, http.MethodPut, registerPath("reader"), token, registration("reader", newType("reader.ticket.created")))
		if resp.Status != http.StatusForbidden {
			t.Fatalf("request %d: status = %d", i, resp.Status)
		}
	}
	events := reader.wait(30)
	if got := reader.settle(); len(got) != 30 || len(events) < 30 {
		t.Fatalf("entries = %d, want 30", len(got))
	}
	for _, e := range reader.read() {
		if e.Data.Result != "denied" {
			t.Fatalf("entry = %+v", e)
		}
	}
}

// I11: with platform-go v0.9.0, the audit publisher sends no index command. The
// ACL user of the service cannot write the old index or the trusted set
// audit.apps. No warn record comes from the publisher.
func TestAudit_I11_NoIndexCommand(t *testing.T) {
	logger, capture := testserver.NewLogCapture()
	srv := started(t, testserver.WithLogger(logger), testserver.WithValkeyACL())
	reader := newAuditReader(t)
	registerOK(t, srv, "helpdesk-svc", "helpdesk", newType("helpdesk.ticket.indexed"))
	if got := reader.wait(1); len(got) != 1 || got[0].Data.Result != "success" {
		t.Fatalf("entries = %+v", got)
	}

	admin := env.ValkeyAdmin(t)
	for _, key := range []string{"audit.streams", "audit.apps"} {
		n, err := admin.Do(context.Background(), admin.B().Exists().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("EXISTS %s = %d, want 0", key, n)
		}
	}

	cfg := srv.Config.Valkey
	user, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{cfg.Address}, Username: srv.ValkeyUser, Password: cfg.Password.Reveal(), DisableCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(user.Close)
	for _, args := range [][]string{
		{"SADD", "audit.streams", "events"},
		{"SADD", "audit.apps", "events"},
		{"SREM", "audit.apps", "events"},
	} {
		err := user.Do(context.Background(), user.B().Arbitrary(args[0]).Args(args[1:]...).Build()).Error()
		if err == nil || !strings.Contains(err.Error(), "NOPERM") {
			t.Fatalf("%s %s: error = %v, want NOPERM", args[0], args[1], err)
		}
	}
	if text := capture.Text(); strings.Contains(text, `"level":"WARN"`) {
		t.Fatalf("the capture has a warn record: %s", text)
	}
}
