// Package services is the composition root. It makes all adapters and use
// cases and connects them.
package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/techorionai/constellation-platform-go/auth/clientcred"
	"github.com/techorionai/constellation-platform-go/auth/jwt"
	"github.com/techorionai/constellation-platform-go/clients/identity"
	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"
	clientvk "github.com/techorionai/constellation-platform-go/clients/valkey"
	"github.com/techorionai/constellation-platform-go/decorators"
	"github.com/techorionai/constellation-platform-go/health"
	healthpg "github.com/techorionai/constellation-platform-go/health/postgres"
	healthvk "github.com/techorionai/constellation-platform-go/health/valkey"
	"github.com/techorionai/constellation-platform-go/manifest"
	"github.com/techorionai/constellation-platform-go/migrate"
	migratepg "github.com/techorionai/constellation-platform-go/migrate/postgres"
	"github.com/techorionai/constellation-platform-go/requestid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/techorionai/constellation-events/api/openapi"
	"github.com/techorionai/constellation-events/internal/adapters/postgres"
	"github.com/techorionai/constellation-events/internal/app/commands"
	"github.com/techorionai/constellation-events/internal/app/queries"
	apperrors "github.com/techorionai/constellation-events/internal/common/errors"
	"github.com/techorionai/constellation-events/internal/config"
	httpapi "github.com/techorionai/constellation-events/internal/handlers/http"
	"github.com/techorionai/constellation-events/migrations"
)

type options struct {
	log   *slog.Logger
	clock func() time.Time
}

// Option changes the composition. The options are for tests. Production code
// uses the config.
type Option func(*options)

// WithLogger sets the logger of the service.
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// WithClock sets the clock of the registration command.
func WithClock(now func() time.Time) Option { return func(o *options) { o.clock = now } }

func newOptions(opts []Option) options {
	o := options{log: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// App is a running service.
type App struct {
	cfg    config.Config
	log    *slog.Logger
	db     *clientpg.Client
	kv     *clientvk.Client
	pgChk  *healthpg.Checker
	state  *health.State
	adminL net.Listener
	admin  *http.Server
	pubL   net.Listener
	pub    *http.Server
	verify *jwt.Verifier

	stopPublisher context.CancelFunc
	publisherWG   sync.WaitGroup

	serveWG   sync.WaitGroup
	closeOnce sync.Once
}

// PublicAddr returns the address of the public listener.
func (a *App) PublicAddr() string { return a.pubL.Addr().String() }

// AdminAddr returns the address of the admin listener.
func (a *App) AdminAddr() string { return a.adminL.Addr().String() }

type funcChecker struct {
	name string
	fn   func() error
}

func (c funcChecker) Name() string                { return c.name }
func (c funcChecker) Check(context.Context) error { return c.fn() }

// errorLevel logs an error that the caller caused at the info level.
func errorLevel(err error) slog.Level {
	if _, ok := apperrors.AsValidationError(err); ok {
		return slog.LevelInfo
	}
	if pe, ok := apperrors.AsProblem(err); ok && pe.Status < http.StatusInternalServerError {
		return slog.LevelInfo
	}
	return slog.LevelError
}

// Start runs the startup order of the service and returns when the public
// server runs. On an error it releases everything that it made. The readiness
// stays 503 until the own manifest is published.
func Start(ctx context.Context, cfg config.Config, opts ...Option) (_ *App, err error) {
	o := newOptions(opts)
	log := o.log
	a := &App{cfg: cfg, log: log}
	defer func() {
		if err != nil {
			a.release(context.WithoutCancel(ctx))
		}
	}()

	// The checks of the embedded files stop the start before any connection.
	scopes, err := httpapi.NewScopeTable(openapi.Spec)
	if err != nil {
		return nil, err
	}
	perms, err := manifest.Load(openapi.Files, openapi.EventsManifestPath)
	if err != nil {
		return nil, fmt.Errorf("own manifest: %w", err)
	}
	tokens, err := clientcred.New(cfg.Client)
	if err != nil {
		return nil, fmt.Errorf("client credentials: %w", err)
	}
	idc, err := identity.New(cfg.Identity, tokens, []string{"identity:manifest:publish"})
	if err != nil {
		return nil, fmt.Errorf("identity client: %w", err)
	}
	publisher := manifest.NewPublisher(identity.NewManifestRegistry(idc), perms, manifest.WithLogger(log))

	// 1. Connect to PostgreSQL and Valkey.
	if a.db, err = clientpg.New(ctx, cfg.Database); err != nil {
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	if a.kv, err = clientvk.New(ctx, cfg.Valkey); err != nil {
		return nil, fmt.Errorf("connect to Valkey: %w", err)
	}
	if a.pgChk, err = healthpg.NewChecker(ctx, a.db.Pool); err != nil {
		return nil, fmt.Errorf("make PostgreSQL checker: %w", err)
	}

	// 2. Start the admin server. /livez is 200. /readyz is 503.
	var migrated atomic.Bool
	a.state = health.NewState(
		a.pgChk,
		healthvk.NewChecker(a.kv.Raw),
		funcChecker{"migrations", func() error {
			if !migrated.Load() {
				return errors.New("migrations are not done")
			}
			return nil
		}},
		publisher.Checker(),
	)
	if a.adminL, err = (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Admin.Addr); err != nil {
		return nil, fmt.Errorf("listen on the admin address: %w", err)
	}
	a.admin = &http.Server{
		Handler:           health.NewHandler(a.state, cfg.Admin),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	a.serve(a.admin, a.adminL, "admin")

	// 3. Run the migrations with the advisory lock.
	if err := RunMigrations(ctx, a.db); err != nil {
		return nil, err
	}
	migrated.Store(true)

	// 4. Publish the own manifest in the background. A producer gets a token
	// for the audience events only after this publish.
	pubCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	a.stopPublisher = stop
	a.publisherWG.Add(1)
	go func() {
		defer a.publisherWG.Done()
		if err := publisher.Run(pubCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("the own manifest is not published", "error", err)
		}
	}()

	// 5. Make the use cases and start the public server.
	repo := postgres.New(a.db.Pool)
	register, err := commands.NewRegisterEventTypes(repo, log, o.clock)
	if err != nil {
		return nil, err
	}
	if a.verify, err = jwt.New(cfg.Auth.JWT()); err != nil {
		return nil, fmt.Errorf("make the token verifier: %w", err)
	}
	a.verify.Start(context.WithoutCancel(ctx))
	observe := []decorators.Option{decorators.WithLogger(log), decorators.WithErrorLevel(errorLevel)}
	api := httpapi.New(httpapi.Deps{
		Register: decorators.ApplyTracing(decorators.ApplyLogging(register, "RegisterEventTypes", observe...), "RegisterEventTypes"),
		ListEventTypes: decorators.ApplyTracing(decorators.ApplyLogging(
			queries.NewListEventTypes(repo), "ListEventTypes", observe...), "ListEventTypes"),
		GetEventType: decorators.ApplyTracing(decorators.ApplyLogging(
			queries.NewGetEventType(repo), "GetEventType", observe...), "GetEventType"),
		ListRegistrations: decorators.ApplyTracing(decorators.ApplyLogging(
			queries.NewListRegistrations(repo), "ListRegistrations", observe...), "ListRegistrations"),
		GetRegistration: decorators.ApplyTracing(decorators.ApplyLogging(
			queries.NewGetRegistration(repo), "GetRegistration", observe...), "GetRegistration"),
		MaxBodyBytes: cfg.MaxBodyBytes,
		Verifier:     a.verify,
		Scopes:       scopes,
		Log:          log,
		LogDenied:    register.LogDenied,
	})
	handler := otelhttp.NewHandler(requestid.Middleware()(api.Handler()), "events")
	if a.pubL, err = (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.HTTP.Addr); err != nil {
		return nil, fmt.Errorf("listen on the public address: %w", err)
	}
	a.pub = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	a.serve(a.pub, a.pubL, "public")
	a.state.MarkStarted()
	return a, nil
}

func (a *App) serve(srv *http.Server, l net.Listener, name string) {
	a.serveWG.Add(1)
	go func() {
		defer a.serveWG.Done()
		if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("server stopped", "server", name, "error", err)
		}
	}()
}

// Shutdown stops the service in the order of the platform-go guide: drain the
// admin server, stop the public server, stop the publisher, close the
// clients, and stop the admin server last.
func (a *App) Shutdown(ctx context.Context) error {
	var errs []error
	if a.state != nil {
		a.state.MarkDraining()
		t := time.NewTimer(a.cfg.Admin.DrainDelay)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
		}
	}
	if a.pub != nil {
		errs = append(errs, a.pub.Shutdown(ctx))
	}
	a.release(ctx)
	return errors.Join(errs...)
}

// release stops everything that Start made. It is safe to call it more than
// one time.
func (a *App) release(ctx context.Context) {
	a.closeOnce.Do(func() {
		if a.pub != nil {
			_ = a.pub.Close()
		}
		if a.stopPublisher != nil {
			a.stopPublisher()
			a.publisherWG.Wait()
		}
		if a.verify != nil {
			a.verify.Close()
		}
		if a.pgChk != nil {
			a.pgChk.Close()
		}
		if a.kv != nil {
			a.kv.Close()
		}
		if a.db != nil {
			a.db.Close()
		}
		if a.admin != nil {
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = a.admin.Shutdown(sctx)
			cancel()
		}
		if a.adminL != nil {
			_ = a.adminL.Close()
		}
		if a.pubL != nil {
			_ = a.pubL.Close()
		}
		a.serveWG.Wait()
	})
}

// RunMigrations runs the migrations of the service with the advisory lock.
func RunMigrations(ctx context.Context, db *clientpg.Client) error {
	migrator, err := migratepg.NewMigrator(db.Pool, migrations.Files, migratepg.MigratorOptions{})
	if err != nil {
		return fmt.Errorf("make the migrator: %w", err)
	}
	if _, err := migrate.Run(ctx, migratepg.NewLocker(db.Pool, ""), migrator, migrate.Options{}); err != nil {
		return fmt.Errorf("run the migrations: %w", err)
	}
	return nil
}
