//go:build integration

package services_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/techorionai/constellation-platform-go/eventtypes"

	"github.com/techorionai/constellation-events/internal/config"
	"github.com/techorionai/constellation-events/resources/test/testserver"
)

// I1: at start the events service publishes its manifest to identity. /readyz
// is 503 until the publish succeeds.
func TestI1_StartupPublishesManifest(t *testing.T) {
	identityURL, err := url.Parse(env.IssuerURL)
	if err != nil {
		t.Fatal(err)
	}
	open := make(chan struct{})
	proxy := httputil.NewSingleHostReverseProxy(identityURL)
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-open:
			proxy.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	defer gate.Close()

	srv := env.Start(t, testserver.WithConfigure(func(cfg *config.Config) { cfg.Client.TokenURL = gate.URL + "/oauth/token" }))
	for range 5 {
		if got := srv.Ready(); got != http.StatusServiceUnavailable {
			t.Fatalf("/readyz before the publish = %d, want 503", got)
		}
		time.Sleep(100 * time.Millisecond)
	}
	close(open)
	srv.WaitReady(t)

	// Identity gives a token for the audience events only when it has the manifest.
	token := env.Token(t, "reader-svc", "events", readScope)
	resp := srv.Do(t, http.MethodGet, "/v1/event-types", token, nil)
	if resp.Status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
}

// I2: a client registers with the eventtypes publisher of platform-go.
func TestI2_PublisherRegisters(t *testing.T) {
	srv := started(t)
	reg, err := eventtypes.Load(fsOf("event-types.json", string(registration("helpdesk", newType("helpdesk.ticket.created"), newType("helpdesk.ticket.closed")))), "event-types.json")
	if err != nil {
		t.Fatal(err)
	}
	pub := eventtypes.NewPublisher(eventsClient(t, srv, "helpdesk-svc"), reg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := pub.Run(ctx); err != nil {
		t.Fatalf("publisher: %v", err)
	}
	if !pub.Registered() {
		t.Fatal("the checker of the publisher must be ready")
	}
	reply, err := eventsClient(t, srv, "helpdesk-svc").Register(ctx, reg)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Version != 1 || reply.EventTypeCount != 2 || reply.App != "helpdesk" {
		t.Fatalf("result = %+v", reply)
	}
}

// I3: the same registration again, in another order, gives the stored version.
func TestI3_SameRegistrationAgain(t *testing.T) {
	srv := started(t)
	a, b := newType("helpdesk.ticket.created"), newType("helpdesk.ticket.closed")
	first := registerOK(t, srv, "helpdesk-svc", "helpdesk", a, b)
	second := registerOK(t, srv, "helpdesk-svc", "helpdesk", b, a)
	if first.Version != 1 || second.Version != 1 || !first.RegisteredAt.Equal(second.RegisteredAt) {
		t.Fatalf("first = %+v, second = %+v", first, second)
	}
}

// I4: a removed type stays in the catalog with removedAt, and it comes back.
func TestI4_RemovedTypeComesBack(t *testing.T) {
	srv := started(t)
	a, b := newType("helpdesk.ticket.created"), newType("helpdesk.ticket.closed")
	if r := registerOK(t, srv, "helpdesk-svc", "helpdesk", a, b); r.Version != 1 {
		t.Fatalf("version = %d", r.Version)
	}
	var created time.Time
	for _, ct := range listTypes(t, srv, "").Items {
		if ct.Type == b.name {
			created = ct.CreatedAt
		}
	}
	if created.IsZero() {
		t.Fatal("the type is not in the catalog")
	}

	r2 := registerOK(t, srv, "helpdesk-svc", "helpdesk", a)
	if r2.Version != 2 || r2.EventTypeCount != 1 {
		t.Fatalf("version 2 = %+v", r2)
	}
	resp := read(t, srv, "reader-svc", "/v1/event-types/"+b.name)
	var removed catalogType
	resp.JSON(t, &removed)
	if resp.Status != http.StatusOK || removed.RemovedAt == nil {
		t.Fatalf("removed type: status = %d, body = %s", resp.Status, resp.Body)
	}
	for _, ct := range listTypes(t, srv, "").Items {
		if ct.Type == b.name {
			t.Fatal("the list without includeRemoved must not have the removed type")
		}
	}
	if n := len(listTypes(t, srv, "?includeRemoved=true").Items); n != 2 {
		t.Fatalf("list with includeRemoved has %d types, want 2", n)
	}
	var regs struct {
		Registration struct {
			EventTypes []json.RawMessage `json:"eventTypes"`
		} `json:"registration"`
	}
	read(t, srv, "reader-svc", "/v1/event-type-registrations/helpdesk").JSON(t, &regs)
	if len(regs.Registration.EventTypes) != 1 {
		t.Fatalf("the registration has %d types, want 1", len(regs.Registration.EventTypes))
	}

	r3 := registerOK(t, srv, "helpdesk-svc", "helpdesk", a, b)
	if r3.Version != 3 || r3.EventTypeCount != 2 {
		t.Fatalf("version 3 = %+v", r3)
	}
	var back catalogType
	read(t, srv, "reader-svc", "/v1/event-types/"+b.name).JSON(t, &back)
	if back.RemovedAt != nil || !back.CreatedAt.Equal(created) {
		t.Fatalf("type after return: %+v, created at %s", back, created)
	}
}

// I5: a client cannot register the types of another app.
func TestI5_AppMismatch(t *testing.T) {
	srv := started(t)
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	resp := srv.Do(t, http.MethodPut, registerPath("crm"), token, registration("crm", newType("crm.ticket.created")))
	if resp.Status != http.StatusUnprocessableEntity || resp.Code(t) != "registration_app_mismatch" {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	if n := len(listTypes(t, srv, "").Items); n != 0 {
		t.Fatalf("the catalog has %d types, want 0", n)
	}
}

// I6: a token without the register scope gives 403 with missingScopes. A client
// with no role cannot get a token for the audience events at all, because
// identity gives only the scopes of the roles of the client.
func TestI6_MissingScope(t *testing.T) {
	srv := started(t)
	if _, err := env.Source(t, "noroles-svc").Token(context.Background(), "events", registerScope); err == nil {
		t.Fatal("a client with no role must not get a token with the register scope")
	}
	token := env.Token(t, "reader-svc", "events", readScope)
	resp := srv.Do(t, http.MethodPut, registerPath("reader"), token, registration("reader", newType("reader.ticket.created")))
	assertForbidden(t, resp, registerScope)
}

func assertForbidden(t *testing.T, resp testserver.Response, missing string) {
	t.Helper()
	var p struct {
		MissingScopes []string `json:"missingScopes"`
	}
	resp.JSON(t, &p)
	if resp.Status != http.StatusForbidden || !slices.Contains(p.MissingScopes, missing) {
		t.Fatalf("status = %d, body = %s, want missingScopes with %s", resp.Status, resp.Body, missing)
	}
}

// I7: a token for another audience gives 401.
func TestI7_WrongAudience(t *testing.T) {
	srv := started(t)
	token := env.Token(t, "helpdesk-svc", "identity", "identity:manifest:publish")
	resp := srv.Do(t, http.MethodPut, registerPath("helpdesk"), token, registration("helpdesk", newType("helpdesk.ticket.created")))
	if resp.Status != http.StatusUnauthorized {
		t.Fatalf("status = %d, body = %s", resp.Status, resp.Body)
	}
	if resp := srv.Do(t, http.MethodGet, "/v1/event-types", "", nil); resp.Status != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d", resp.Status)
	}
}

// I8: parallel registrations run one after the other. The first registrations
// of a new app at the same time make the versions 1 and 2.
func TestI8_ParallelRegistrations(t *testing.T) {
	srv := started(t)
	token := env.Token(t, "helpdesk-svc", "events", registerScope)
	var wg sync.WaitGroup
	statuses := make([]int, 10)
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := registration("helpdesk", newType(fmt.Sprintf("helpdesk.item.a%d", i)), newType(fmt.Sprintf("helpdesk.item.b%d", i)))
			statuses[i] = srv.Do(t, http.MethodPut, registerPath("helpdesk"), token, body).Status
		}()
	}
	wg.Wait()
	for i, s := range statuses {
		if s != http.StatusOK {
			t.Fatalf("registration %d: status = %d", i, s)
		}
	}
	var reg struct {
		Version      int `json:"version"`
		Registration struct {
			EventTypes []struct {
				Type string `json:"type"`
			} `json:"eventTypes"`
		} `json:"registration"`
	}
	read(t, srv, "reader-svc", "/v1/event-type-registrations/helpdesk").JSON(t, &reg)
	if reg.Version != 10 || len(reg.Registration.EventTypes) != 2 {
		t.Fatalf("version = %d, types = %d", reg.Version, len(reg.Registration.EventTypes))
	}
	names := []string{reg.Registration.EventTypes[0].Type, reg.Registration.EventTypes[1].Type}
	sort.Strings(names)
	idx := strings.TrimPrefix(names[0], "helpdesk.item.a")
	if names[1] != "helpdesk.item.b"+idx {
		t.Fatalf("the stored types %v are not the types of one body", names)
	}

	crm := env.Token(t, "crm-svc", "events", registerScope)
	versions := make([]int, 2)
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := srv.Do(t, http.MethodPut, registerPath("crm"), crm, registration("crm", newType(fmt.Sprintf("crm.ticket.v%d", i))))
			var r registered
			if resp.Status == http.StatusOK {
				resp.JSON(t, &r)
			}
			versions[i] = r.Version
		}()
	}
	wg.Wait()
	sort.Ints(versions)
	if versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("versions = %v, want [1 2]", versions)
	}
}
