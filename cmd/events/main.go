// Command events runs the events service.
//
// Usage:
//
//	events               Start the server.
//	events healthcheck   Exit 0 if /readyz of the admin port is 200.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/techorionai/constellation-platform-go/telemetry"

	"github.com/techorionai/constellation-events/internal/config"
	"github.com/techorionai/constellation-events/internal/services"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const shutdownTimeout = 30 * time.Second

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return serve(stderr)
	}
	switch args[0] {
	case "healthcheck":
		return runHealthcheck(stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		return 2
	}
}

func serve(stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "config error: %v\n", err)
		return 1
	}
	tel, err := telemetry.Setup(ctx, telemetry.Options{ServiceName: "events", ServiceVersion: version, LogLevel: cfg.LogLevel})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "telemetry error: %v\n", err)
		return 1
	}
	defer func() { _ = tel.Shutdown(context.WithoutCancel(ctx)) }()

	app, err := services.Start(ctx, cfg)
	if err != nil {
		slog.ErrorContext(ctx, "the service cannot start", "error", err)
		return 1
	}
	slog.InfoContext(ctx, "the service is running", "public_addr", app.PublicAddr(), "admin_addr", app.AdminAddr())

	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if err := app.Shutdown(sctx); err != nil {
		slog.ErrorContext(ctx, "the service did not stop cleanly", "error", err)
		return 1
	}
	return 0
}
