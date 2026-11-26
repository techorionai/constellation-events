//go:build integration

package services_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/techorionai/constellation-platform-go/clients/events"
	"github.com/techorionai/constellation-platform-go/eventtypes"

	"github.com/techorionai/constellation-events/resources/test/testserver"
)

// env is the shared containers of the package.
var env *testserver.Env

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	e, err := testserver.NewEnv(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "the test environment does not start: %v\n", err)
		os.Exit(1)
	}
	env = e
	code := m.Run()
	env.Close()
	os.Exit(code)
}

const (
	registerScope = "events:event_type:register"
	readScope     = "events:catalog:read"
)

// typ is one event type of a test body.
type typ struct {
	name        string
	description string
	schema      string
	deprecated  bool
}

func newType(name string) typ {
	return typ{name: name, description: "The event " + name + ".", schema: `{"type":"object"}`}
}

// registration makes the JSON body of a registration.
func registration(app string, types ...typ) []byte {
	var items []string
	for _, t := range types {
		items = append(items, fmt.Sprintf(`{"type":%q,"description":%q,"dataSchema":%s,"deprecated":%t}`,
			t.name, t.description, t.schema, t.deprecated))
	}
	return []byte(fmt.Sprintf(`{"source":"/%s","app":%q,"eventTypes":[%s]}`, app, app, strings.Join(items, ",")))
}

func registerPath(app string) string { return "/v1/event-type-registrations/" + app }

// started starts a service and waits until it is ready.
func started(t *testing.T, opts ...testserver.Option) *testserver.Server {
	t.Helper()
	srv := env.Start(t, opts...)
	srv.WaitReady(t)
	return srv
}

// register sends a registration of the client for its own app.
func register(t *testing.T, srv *testserver.Server, client, app string, types ...typ) testserver.Response {
	t.Helper()
	token := env.Token(t, client, "events", registerScope)
	return srv.Do(t, http.MethodPut, registerPath(app), token, registration(app, types...))
}

// registerOK registers and expects 200.
func registerOK(t *testing.T, srv *testserver.Server, client, app string, types ...typ) registered {
	t.Helper()
	resp := register(t, srv, client, app, types...)
	if resp.Status != http.StatusOK {
		t.Fatalf("register: status = %d, body = %s", resp.Status, resp.Body)
	}
	var r registered
	resp.JSON(t, &r)
	return r
}

type registered struct {
	App            string    `json:"app"`
	Version        int       `json:"version"`
	RegisteredAt   time.Time `json:"registeredAt"`
	EventTypeCount int       `json:"eventTypeCount"`
}

type catalogType struct {
	Type        string          `json:"type"`
	App         string          `json:"app"`
	Source      string          `json:"source"`
	Description string          `json:"description"`
	DataSchema  json.RawMessage `json:"dataSchema"`
	Deprecated  bool            `json:"deprecated"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	RemovedAt   *time.Time      `json:"removedAt"`
}

type typePage struct {
	Items         []catalogType `json:"items"`
	NextPageToken string        `json:"nextPageToken"`
}

// read sends a GET request with a catalog read token of the client.
func read(t *testing.T, srv *testserver.Server, client, path string) testserver.Response {
	t.Helper()
	return srv.Do(t, http.MethodGet, path, env.Token(t, client, "events", readScope), nil)
}

func listTypes(t *testing.T, srv *testserver.Server, query string) typePage {
	t.Helper()
	resp := read(t, srv, "reader-svc", "/v1/event-types"+query)
	if resp.Status != http.StatusOK {
		t.Fatalf("list types: status = %d, body = %s", resp.Status, resp.Body)
	}
	var p typePage
	resp.JSON(t, &p)
	return p
}

// eventsClient makes the events client of platform-go for a service client.
func eventsClient(t *testing.T, srv *testserver.Server, client string) eventtypes.Registry {
	t.Helper()
	c, err := events.New(events.Config{URL: srv.PublicURL, Timeout: 20 * time.Second, AllowInsecureHTTP: true},
		env.Source(t, client), []string{registerScope})
	if err != nil {
		t.Fatal(err)
	}
	return events.NewEventTypeRegistry(c)
}

// fsOf makes a file system with one file.
func fsOf(name, content string) fs.FS {
	return fstest.MapFS{name: {Data: []byte(content)}}
}
