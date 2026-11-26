package testserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"
	clientvk "github.com/techorionai/constellation-platform-go/clients/valkey"

	"github.com/techorionai/constellation-events/internal/config"
	"github.com/techorionai/constellation-events/internal/services"
)

type settings struct {
	db           *DB
	logger       *slog.Logger
	maxBodyBytes int64
	client       string
	configure    []func(*config.Config)
}

// Option changes a test server.
type Option func(*settings)

// WithDB uses an existing database, for example for a second instance.
func WithDB(db DB) Option { return func(s *settings) { s.db = &db } }

// WithLogger sets the logger of the service, for example a LogCapture.
func WithLogger(l *slog.Logger) Option { return func(s *settings) { s.logger = l } }

// WithMaxBodyBytes sets the body limit of the registration.
func WithMaxBodyBytes(n int64) Option { return func(s *settings) { s.maxBodyBytes = n } }

// WithConfigure changes the config before the service starts.
func WithConfigure(f func(*config.Config)) Option {
	return func(s *settings) { s.configure = append(s.configure, f) }
}

// Server is a running events service.
type Server struct {
	App       *services.App
	Config    config.Config
	DB        DB
	PublicURL string
	AdminURL  string

	stopOnce sync.Once
}

// Start starts the events service in the test process. It uses the client
// events of the identity fixture and a new database. The cleanup of the test
// stops it.
func (e *Env) Start(t testing.TB, opts ...Option) *Server {
	t.Helper()
	srv, err := e.StartE(t, opts...)
	if err != nil {
		t.Fatalf("testserver: start the events service: %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv
}

// StartE is Start with an error result. It is safe to call it from a goroutine
// of a test. The caller stops the server.
func (e *Env) StartE(t testing.TB, opts ...Option) (*Server, error) {
	s := settings{maxBodyBytes: config.DefaultMaxBodyBytes, client: "events"}
	for _, o := range opts {
		o(&s)
	}
	var db DB
	if s.db != nil {
		db = *s.db
	} else {
		db = e.NewDB(t)
	}
	cfg := config.Config{
		LogLevel: slog.LevelInfo,
		HTTP:     config.HTTP{Addr: "127.0.0.1:0"},
		Database: clientpg.Config{URL: db.URL, Database: db.Name, MaxConns: 10, ConnectTimeout: 5 * time.Second},
		Valkey:   clientvk.Config{Address: e.ValkeyAddress(t), Password: vkPassword, KeyPrefix: "events", DialTimeout: 5 * time.Second},
		Auth: config.Auth{
			Issuer: e.IssuerURL, RefreshInterval: time.Minute, MinRefetchInterval: time.Second,
			Leeway: 30 * time.Second, HTTPTimeout: 5 * time.Second, AllowInsecureHTTP: true,
		},
		Client:       e.ClientConfig(s.client),
		MaxBodyBytes: s.maxBodyBytes,
	}
	cfg.Admin.Addr = "127.0.0.1:0"
	cfg.Admin.CheckTimeout = 2 * time.Second
	cfg.Admin.DrainDelay = time.Millisecond
	cfg.Identity.URL = e.IssuerURL
	cfg.Identity.Timeout = 10 * time.Second
	cfg.Identity.AllowInsecureHTTP = true
	for _, f := range s.configure {
		f(&cfg)
	}
	logger := s.logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	app, err := services.Start(context.Background(), cfg, services.WithLogger(logger))
	if err != nil {
		return nil, err
	}
	srv := &Server{
		App: app, Config: cfg, DB: db,
		PublicURL: "http://" + app.PublicAddr(), AdminURL: "http://" + app.AdminAddr(),
	}
	return srv, nil
}

// Stop stops the service. It is safe to call it more than one time.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.App.Shutdown(ctx)
	})
}

// Ready returns the status of /readyz.
func (s *Server) Ready() int {
	resp, err := http.Get(s.AdminURL + "/readyz") //nolint:gosec,noctx // The URL is the local admin port of a test.
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// WaitReady waits until /readyz is 200.
func (s *Server) WaitReady(t testing.TB) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if s.Ready() == http.StatusOK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("testserver: /readyz is %d after 90 seconds", s.Ready())
}

// Response is the answer of a request.
type Response struct {
	Status int
	Header http.Header
	Body   []byte
}

// JSON decodes the body into v.
func (r Response) JSON(t testing.TB, v any) {
	t.Helper()
	if err := json.Unmarshal(r.Body, v); err != nil {
		t.Fatalf("testserver: body %q is not JSON: %v", r.Body, err)
	}
}

// Code returns the code of a Problem body.
func (r Response) Code(t testing.TB) string {
	t.Helper()
	var p struct {
		Code string `json:"code"`
	}
	r.JSON(t, &p)
	return p.Code
}

// Do sends a request with a bearer token. A token "" sends no header.
func (s *Server) Do(t testing.TB, method, path, token string, body []byte) Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, s.PublicURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("testserver: %s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return Response{Status: resp.StatusCode, Header: resp.Header, Body: data}
}

// LogCapture is a slog handler that keeps the records as text.
type LogCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// NewLogCapture makes a logger that writes JSON records to the capture.
func NewLogCapture() (*slog.Logger, *LogCapture) {
	c := &LogCapture{}
	return slog.New(slog.NewJSONHandler(&lockedWriter{c}, &slog.HandlerOptions{Level: slog.LevelDebug})), c
}

type lockedWriter struct{ c *LogCapture }

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	return w.c.buf.Write(p)
}

// Text returns all records.
func (c *LogCapture) Text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// Records returns the records that have the given action.
func (c *LogCapture) Records(action string) []map[string]any {
	var out []map[string]any
	for _, line := range strings.Split(c.Text(), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) == nil && m["action"] == action {
			out = append(out, m)
		}
	}
	return out
}
