// Package testserver starts the events service in the test process, with live
// PostgreSQL and Valkey containers and the identity image of the platform.
// Import it from test code only.
package testserver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	"github.com/techorionai/constellation-platform-go/auth/clientcred"
	platformconfig "github.com/techorionai/constellation-platform-go/config"
	"github.com/techorionai/constellation-platform-go/resources/test/containers"
	streamsvalkey "github.com/techorionai/constellation-platform-go/streams/valkey"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcvalkey "github.com/testcontainers/testcontainers-go/modules/valkey"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	valkeygo "github.com/valkey-io/valkey-go"
)

// The images of the tests. PostgreSQL and Valkey are the same as in
// constellation-deploy. The identity image is the release with the app claim.
const (
	PostgresImage = "postgres:18.7-alpine"
	ValkeyImage   = "valkey/valkey:9.0.7-alpine"
	IdentityImage = "ghcr.io/techorionai/constellation-identity:0.7.1"
)

// Passwords of the throwaway containers.
const (
	pgPassword = "pgpass"
	vkPassword = "vkpass"
)

// Client is a service client of the identity fixture.
type Client struct {
	ID    string
	App   string
	Roles []string
	// KeyID and PEM are the signing key of the client.
	KeyID string
	PEM   string
	// JWK is the public key for the clients file.
	x, y string
}

// Clients of the identity fixture. The client events publishes the manifest of
// the events service. It has no role, because the implicit role
// identity:service gives it the right to publish.
var clientSpecs = []Client{
	{ID: "events", App: "events"},
	{ID: "helpdesk-svc", App: "helpdesk", Roles: []string{"events:producer"}},
	{ID: "crm-svc", App: "crm", Roles: []string{"events:producer"}},
	{ID: "sales-svc", App: "sales", Roles: []string{"events:producer"}},
	{ID: "reader-svc", App: "reader", Roles: []string{"events:reader"}},
	{ID: "noroles-svc", App: "noroles"},
}

// Env holds the containers and the identity fixture. Many servers can share one
// Env.
type Env struct {
	Network   *testcontainers.DockerNetwork
	PG        *tcpostgres.PostgresContainer
	VK        *tcvalkey.ValkeyContainer
	Identity  testcontainers.Container
	IssuerURL string
	Clients   map[string]*Client

	pgAdmin *pgxpool.Pool
	mu      sync.Mutex
	sources map[string]*clientcred.Source
	dbSeq   int
}

var clientsTemplate = template.Must(template.New("clients").Parse(`clients:
{{- range .}}
  - id: {{.ID}}
    app: {{.App}}
    name: {{.ID}}
    keys:
      - kid: {{.KeyID}}
        jwk: { kty: EC, crv: P-256, x: "{{.X}}", y: "{{.Y}}" }
{{- if .Roles}}
    roles:
{{- range .Roles}}
      - {{.}}
{{- end}}
{{- end}}
{{- end}}
`))

type clientRow struct {
	ID, App, KeyID, X, Y string
	Roles                []string
}

// freePort returns a TCP port that is free now.
func freePort() (int, error) {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// NewEnv starts the containers and the identity service. Call Close when the
// tests end. It is for TestMain.
func NewEnv(ctx context.Context) (_ *Env, err error) {
	e := &Env{Clients: map[string]*Client{}, sources: map[string]*clientcred.Source{}}
	defer func() {
		if err != nil {
			e.Close()
		}
	}()

	if e.Network, err = network.New(ctx); err != nil {
		return nil, fmt.Errorf("network: %w", err)
	}
	if e.PG, err = tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("postgres"), tcpostgres.WithUsername("postgres"), tcpostgres.WithPassword(pgPassword),
		tcpostgres.BasicWaitStrategies(), network.WithNetwork([]string{"pg"}, e.Network)); err != nil {
		return nil, fmt.Errorf("PostgreSQL: %w", err)
	}
	if e.VK, err = tcvalkey.Run(ctx, ValkeyImage,
		testcontainers.WithCmdArgs("--requirepass", vkPassword), network.WithNetwork([]string{"vk"}, e.Network)); err != nil {
		return nil, fmt.Errorf("valkey: %w", err)
	}
	pgURL, err := e.PG.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, err
	}
	if e.pgAdmin, err = pgxpool.New(ctx, pgURL); err != nil {
		return nil, fmt.Errorf("PostgreSQL admin pool: %w", err)
	}
	for _, sql := range []string{
		`CREATE ROLE identity LOGIN PASSWORD 'idpass'`,
		`CREATE DATABASE identity OWNER identity`,
		`CREATE ROLE events LOGIN PASSWORD 'evpass'`,
	} {
		if _, err := e.pgAdmin.Exec(ctx, sql); err != nil {
			return nil, fmt.Errorf("%s: %w", sql, err)
		}
	}

	rows := make([]clientRow, 0, len(clientSpecs))
	for _, spec := range clientSpecs {
		c := spec
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, err
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, err
		}
		c.PEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		c.KeyID = c.ID + "-key"
		xb, yb := make([]byte, 32), make([]byte, 32)
		key.X.FillBytes(xb)
		key.Y.FillBytes(yb)
		c.x, c.y = base64.RawURLEncoding.EncodeToString(xb), base64.RawURLEncoding.EncodeToString(yb)
		e.Clients[c.ID] = &c
		rows = append(rows, clientRow{ID: c.ID, App: c.App, KeyID: c.KeyID, X: c.x, Y: c.y, Roles: c.Roles})
	}
	var clientsFile bytes.Buffer
	if err := clientsTemplate.Execute(&clientsFile, rows); err != nil {
		return nil, err
	}

	port, err := freePort()
	if err != nil {
		return nil, err
	}
	e.IssuerURL = "http://localhost:" + strconv.Itoa(port)
	masterKey := make([]byte, 32)
	if _, err := rand.Read(masterKey); err != nil {
		return nil, err
	}
	if e.Identity, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        IdentityImage,
			ExposedPorts: []string{"8080/tcp", "9090/tcp"},
			HostConfigModifier: func(hc *mobycontainer.HostConfig) {
				hc.PortBindings = mobynetwork.PortMap{
					mobynetwork.MustParsePort("8080/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}},
				}
			},
			Networks:       []string{e.Network.Name},
			NetworkAliases: map[string][]string{e.Network.Name: {"identity"}},
			Files: []testcontainers.ContainerFile{{
				Reader: bytes.NewReader(clientsFile.Bytes()), ContainerFilePath: "/etc/identity/clients.yaml", FileMode: 0o644,
			}},
			Env: map[string]string{ //nolint:gosec // Throwaway passwords of test containers.
				"IDENTITY_ISSUER":                 e.IssuerURL,
				"IDENTITY_DATABASE__URL":          "postgres://identity:idpass@pg:5432/identity?sslmode=disable",
				"IDENTITY_DATABASE__NAME":         "identity",
				"IDENTITY_VALKEY__ADDRESS":        "vk:6379",
				"IDENTITY_VALKEY__PASSWORD":       vkPassword,
				"IDENTITY_VALKEY__KEY_PREFIX":     "identity",
				"IDENTITY_SECRETS__MASTER_KEY":    base64.StdEncoding.EncodeToString(masterKey),
				"IDENTITY_SECRETS__MASTER_KEY_ID": "test",
				"IDENTITY_CLIENTS_FILE":           "/etc/identity/clients.yaml",
				"IDENTITY_RATE_LIMIT__ENABLED":    "false",
			},
			WaitingFor: wait.ForHTTP("/readyz").WithPort("9090/tcp").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	}); err != nil {
		return nil, fmt.Errorf("identity: %w", err)
	}
	return e, nil
}

// Close removes the containers.
func (e *Env) Close() {
	if e.pgAdmin != nil {
		e.pgAdmin.Close()
	}
	if e.Identity != nil {
		_ = testcontainers.TerminateContainer(e.Identity)
	}
	if e.VK != nil {
		_ = testcontainers.TerminateContainer(e.VK)
	}
	if e.PG != nil {
		_ = testcontainers.TerminateContainer(e.PG)
	}
	if e.Network != nil {
		_ = e.Network.Remove(context.Background())
	}
}

// DB is a database for one events service.
type DB struct {
	URL  platformconfig.Secret
	Name string
}

// NewDB makes a new database that the user events owns.
func (e *Env) NewDB(t testing.TB) DB {
	t.Helper()
	e.mu.Lock()
	e.dbSeq++
	name := fmt.Sprintf("events_%d_%d", time.Now().UnixNano(), e.dbSeq)
	e.mu.Unlock()
	if _, err := e.pgAdmin.Exec(context.Background(), `CREATE DATABASE `+name+` OWNER events`); err != nil {
		t.Fatalf("testserver: create database: %v", err)
	}
	host, err := e.PG.Host(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	port, err := e.PG.MappedPort(context.Background(), "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return DB{
		URL:  platformconfig.Secret(fmt.Sprintf("postgres://events:evpass@%s:%s/%s?sslmode=disable", host, port.Port(), name)),
		Name: name,
	}
}

// ValkeyAddress returns the address of Valkey for the test process.
func (e *Env) ValkeyAddress(t testing.TB) string {
	t.Helper()
	addr, err := e.VK.PortEndpoint(context.Background(), "6379/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(addr, "tcp://")
}

// ClientConfig returns the config of the token source of a client.
func (e *Env) ClientConfig(id string) clientcred.Config {
	c, ok := e.Clients[id]
	if !ok {
		panic("testserver: unknown client " + id)
	}
	return clientcred.Config{
		ID: c.ID, KeyID: c.KeyID, PrivateKey: platformconfig.Secret(c.PEM),
		Issuer: e.IssuerURL, AllowInsecureHTTP: true, HTTPTimeout: 10 * time.Second,
	}
}

// Source returns the token source of a client. It caches the source.
func (e *Env) Source(t testing.TB, id string) *clientcred.Source {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.sources[id]; ok {
		return s
	}
	s, err := clientcred.New(e.ClientConfig(id))
	if err != nil {
		t.Fatalf("testserver: token source of %s: %v", id, err)
	}
	e.sources[id] = s
	return s
}

// Token gets an access token of a client from the identity service.
func (e *Env) Token(t testing.TB, id, audience string, scopes ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tok, err := e.Source(t, id).Token(ctx, audience, scopes...)
	if err != nil {
		t.Fatalf("testserver: token of %s for %s: %v", id, audience, err)
	}
	return tok
}

// NewIsolatedDB starts a PostgreSQL container that only one test uses, and
// returns a database in it with a function that stops the container. A test
// that stops the database uses it.
func (e *Env) NewIsolatedDB(t testing.TB) (DB, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pg, err := tcpostgres.Run(ctx, PostgresImage,
		tcpostgres.WithDatabase("events"), tcpostgres.WithUsername("events"), tcpostgres.WithPassword("evpass"),
		tcpostgres.BasicWaitStrategies())
	if pg != nil {
		t.Cleanup(func() { _ = testcontainers.TerminateContainer(pg) })
	}
	if err != nil {
		t.Fatalf("testserver: isolated PostgreSQL: %v", err)
	}
	host, err := pg.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := pg.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	db := DB{URL: platformconfig.Secret(fmt.Sprintf("postgres://events:evpass@%s:%s/events?sslmode=disable", host, port.Port())), Name: "events"}
	return db, func() { _ = testcontainers.TerminateContainer(pg) }
}

// NewContainerDB makes a new database for a container in the network of the
// Env. It returns the URL for that network and the name of the database.
func (e *Env) NewContainerDB(t testing.TB) (string, string) {
	t.Helper()
	db := e.NewDB(t)
	return "postgres://events:evpass@pg:5432/" + db.Name + "?sslmode=disable", db.Name
}

// ContainerEnv returns the environment variables that an events container in
// the network of the Env needs. The issuer stays the public URL of identity,
// because it is the iss claim of the tokens. The JWKS URL, the token URL, and
// the identity URL use the network alias of the identity container.
func (e *Env) ContainerEnv(t testing.TB) map[string]string {
	t.Helper()
	dbURL, dbName := e.NewContainerDB(t)
	return map[string]string{ //nolint:gosec // Throwaway passwords of test containers.
		"EVENTS_DATABASE__URL":                 dbURL,
		"EVENTS_DATABASE__NAME":                dbName,
		"EVENTS_VALKEY__ADDRESS":               "vk:6379",
		"EVENTS_VALKEY__PASSWORD":              vkPassword,
		"EVENTS_VALKEY__KEY_PREFIX":            "events",
		"EVENTS_AUTH__ISSUER":                  e.IssuerURL,
		"EVENTS_AUTH__JWKS_URL":                "http://identity:8080/.well-known/jwks.json",
		"EVENTS_AUTH__ALLOW_INSECURE_HTTP":     "true",
		"EVENTS_CLIENT__ID":                    "events",
		"EVENTS_CLIENT__KEY_ID":                e.Clients["events"].KeyID,
		"EVENTS_CLIENT__PRIVATE_KEY":           e.Clients["events"].PEM,
		"EVENTS_CLIENT__ISSUER":                e.IssuerURL,
		"EVENTS_CLIENT__TOKEN_URL":             "http://identity:8080/oauth/token",
		"EVENTS_CLIENT__ALLOW_INSECURE_HTTP":   "true",
		"EVENTS_IDENTITY__URL":                 "http://identity:8080",
		"EVENTS_IDENTITY__ALLOW_INSECURE_HTTP": "true",
	}
}

// Network returns the name of the container network.
func (e *Env) NetworkName() string { return e.Network.Name }

// ValkeyAdmin returns a client of the default user of Valkey. The user has all
// rights. It closes the client at cleanup.
func (e *Env) ValkeyAdmin(t testing.TB) valkeygo.Client {
	t.Helper()
	c, err := valkeygo.NewClient(valkeygo.ClientOption{
		InitAddress: []string{e.ValkeyAddress(t)}, Password: vkPassword, DisableCache: true,
	})
	if err != nil {
		t.Fatalf("testserver: valkey admin client: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// ACLSelectors returns the two ACL selectors of the user of the events service
// as separate texts, each in parentheses.
func ACLSelectors() []string {
	return regexp.MustCompile(`\([^)]*\)`).FindAllString(streamsvalkey.ACLSelectors("events"), -1)
}

// NewValkeyACLUser makes an ACL user like the user of the events service in
// production. It has the key pattern ~events:*, the command rule of
// platform-go, and the two stream selectors. It returns the name and the
// password.
func (e *Env) NewValkeyACLUser(t testing.TB) (string, string) {
	t.Helper()
	e.mu.Lock()
	e.dbSeq++
	n := e.dbSeq
	e.mu.Unlock()
	user := fmt.Sprintf("events_acl_%d_%d", time.Now().UnixNano(), n)
	password := fmt.Sprintf("pw%d", time.Now().UnixNano())
	args := []string{"ACL", "SETUSER", user, "on", ">" + password, "~events:*"}
	args = append(args, strings.Fields(containers.ACLRule)...)
	args = append(args, ACLSelectors()...)
	admin := e.ValkeyAdmin(t)
	if err := admin.Do(context.Background(), admin.B().Arbitrary(args[0]).Args(args[1:]...).Build()).Error(); err != nil {
		t.Fatalf("testserver: ACL SETUSER: %v", err)
	}
	return user, password
}
