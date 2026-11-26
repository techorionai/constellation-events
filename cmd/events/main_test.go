package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// U8: the healthcheck command exits 0 for a ready server, and 1 for a server
// that is not ready and for no server.
func TestU8_Healthcheck(t *testing.T) {
	ready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer ready.Close()
	notReady := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer notReady.Close()
	gone := httptest.NewServer(http.NotFoundHandler())
	goneURL := gone.URL
	gone.Close()

	for name, tc := range map[string]struct {
		url  string
		want int
	}{"ready": {ready.URL, 0}, "not ready": {notReady.URL, 1}, "no server": {goneURL, 1}} {
		var stdout, stderr bytes.Buffer
		if got := healthcheck(context.Background(), tc.url, &stdout, &stderr); got != tc.want {
			t.Fatalf("%s: exit code = %d, want %d", name, got, tc.want)
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if got := run([]string{"nope"}, &stdout, &stderr); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
}

func TestHealthcheckAddress(t *testing.T) {
	t.Setenv("EVENTS_ADMIN__ADDR", "no-port")
	var stdout, stderr bytes.Buffer
	if got := runHealthcheck(&stdout, &stderr); got != 1 {
		t.Fatalf("exit code = %d, want 1", got)
	}
}
