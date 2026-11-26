//go:build integration

// Package image tests the container image of the service.
package image_test

import (
	"context"
	"crypto/rand"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/techorionai/constellation-events/resources/test/testserver"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// buildImage builds the Dockerfile with the docker CLI. The Dockerfile uses
// --platform=$BUILDPLATFORM, which the legacy builder of the testcontainers
// library does not support. It returns the image tag.
func buildImage(ctx context.Context, t *testing.T, root string) string {
	t.Helper()
	tag := "events-test:" + strings.ToLower(rand.Text())
	out, err := exec.CommandContext(ctx, "docker", "build", "-t", tag, root).CombinedOutput() //nolint:gosec // A test.
	if err != nil {
		t.Fatalf("docker build: %v: %s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.CommandContext(context.Background(), "docker", "rmi", "-f", tag).Run() //nolint:gosec // A test.
	})
	return tag
}

// I13: build the Dockerfile, run the image with PostgreSQL, Valkey, and
// identity, and run the healthcheck command.
func TestI13_Image(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env, err := testserver.NewEnv(ctx)
	if err != nil {
		t.Fatalf("the test environment does not start: %v", err)
	}
	t.Cleanup(env.Close)

	image := buildImage(ctx, t, repoRoot(t))
	app, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"8080/tcp", "9090/tcp"},
			Networks:     []string{env.NetworkName()},
			Env:          env.ContainerEnv(t),
			WaitingFor:   wait.ForHTTP("/readyz").WithPort("9090/tcp").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	if app != nil {
		t.Cleanup(func() { _ = testcontainers.TerminateContainer(app) })
	}
	if err != nil {
		if app != nil {
			if r, lerr := app.Logs(ctx); lerr == nil {
				logs, _ := io.ReadAll(r)
				t.Logf("container logs: %.4000s", logs)
			}
		}
		t.Fatalf("start the image: %v", err)
	}

	info, err := app.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if user := info.Config.User; user != "65532" && user != "65532:65532" && user != "nonroot" {
		t.Fatalf("image user = %q, want UID 65532", user)
	}
	if len(info.Config.Entrypoint) != 1 || info.Config.Entrypoint[0] != "/events" {
		t.Fatalf("entrypoint = %v", info.Config.Entrypoint)
	}

	// docker exec <container> /events healthcheck exits with 0 when ready.
	code, _, err := app.Exec(ctx, []string{"/events", "healthcheck"})
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("healthcheck exit code = %d, want 0", code)
	}

	// The healthcheck command in a container with no server exits with 1.
	probe, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      image,
			Cmd:        []string{"healthcheck"},
			WaitingFor: wait.ForExit().WithExitTimeout(time.Minute),
		},
		Started: true,
	})
	if probe != nil {
		t.Cleanup(func() { _ = testcontainers.TerminateContainer(probe) })
	}
	if err != nil {
		t.Fatalf("run the healthcheck command: %v", err)
	}
	state, err := probe.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.ExitCode != 1 {
		t.Fatalf("healthcheck with no server: exit code = %d, want 1", state.ExitCode)
	}
}
