package config_test

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/techorionai/constellation-events/internal/config"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("EVENTS_DATABASE__URL", "postgres://u:p@localhost:5432/events?sslmode=disable")
	t.Setenv("EVENTS_DATABASE__NAME", "events")
	t.Setenv("EVENTS_VALKEY__ADDRESS", "localhost:6379")
	t.Setenv("EVENTS_VALKEY__KEY_PREFIX", "events")
	t.Setenv("EVENTS_AUTH__ISSUER", "https://id.example.com")
	t.Setenv("EVENTS_CLIENT__ID", "events")
	t.Setenv("EVENTS_CLIENT__KEY_ID", "k1")
	t.Setenv("EVENTS_CLIENT__PRIVATE_KEY", "pem")
	t.Setenv("EVENTS_CLIENT__ISSUER", "https://id.example.com")
	t.Setenv("EVENTS_IDENTITY__URL", "https://id.example.com")
}

// U6: a bad MaxBodyBytes gives an error that names the variable. The audience
// is the constant events, with or without EVENTS_AUTH__AUDIENCE.
func TestU6_Config(t *testing.T) {
	setRequired(t)
	for _, v := range []string{"1", strconv.Itoa(1<<20 - 1), strconv.Itoa(16<<20 + 1), "-5"} {
		t.Setenv("EVENTS_MAX_BODY_BYTES", v)
		_, err := config.Load()
		if err == nil || !strings.Contains(err.Error(), "EVENTS_MAX_BODY_BYTES") {
			t.Fatalf("value %s: error = %v", v, err)
		}
	}
	t.Setenv("EVENTS_MAX_BODY_BYTES", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxBodyBytes != config.DefaultMaxBodyBytes || cfg.Auth.JWT().Audience != "events" {
		t.Fatalf("max body = %d, audience = %q", cfg.MaxBodyBytes, cfg.Auth.JWT().Audience)
	}

	t.Setenv("EVENTS_AUTH__AUDIENCE", "identity")
	cfg, err = config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Auth.JWT().Audience; got != "events" {
		t.Fatalf("audience = %q, want events", got)
	}
}

func TestMissingVariable(t *testing.T) {
	setRequired(t)
	if err := os.Unsetenv("EVENTS_AUTH__ISSUER"); err != nil {
		t.Fatal(err)
	}
	_, err := config.Load()
	if err == nil || !strings.Contains(err.Error(), "AUTH__ISSUER") {
		t.Fatalf("error = %v", err)
	}
}

// U13: the interval has a default and limits. A bad value gives an error that
// names the variable.
func TestU13_LagInterval(t *testing.T) {
	setRequired(t)
	cases := []struct {
		value string
		want  time.Duration
		bad   bool
	}{
		{"", 30 * time.Second, false},
		{"5s", 5 * time.Second, false},
		{"2m", 2 * time.Minute, false},
		{"4s", 0, true},
		{"3m", 0, true},
		{"abc", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("EVENTS_LAG__INTERVAL", tc.value)
			cfg, err := config.Load()
			if tc.bad {
				if err == nil || !strings.Contains(err.Error(), "EVENTS_LAG__INTERVAL") {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil || cfg.Lag.Interval != tc.want {
				t.Fatalf("interval = %v, error = %v", cfg.Lag.Interval, err)
			}
		})
	}
}

// U4: bad values of the audit settings give errors that name the variable.
func TestU4_AuditConfig(t *testing.T) {
	cases := []struct{ name, key, value, want string }{
		{"trusted proxies", "EVENTS_HTTP__TRUSTED_PROXIES", "bad", "TRUSTED_PROXIES"},
		{"max conns", "EVENTS_DATABASE__MAX_CONNS", "2", "MAX_CONNS"},
		{"refused limit zero", "EVENTS_AUDIT__REFUSED_LIMIT_PER_MINUTE", "0", "REFUSED_LIMIT_PER_MINUTE"},
		{"refused limit large", "EVENTS_AUDIT__REFUSED_LIMIT_PER_MINUTE", "10001", "REFUSED_LIMIT_PER_MINUTE"},
		{"outbox interval", "EVENTS_RETENTION__AUDIT_OUTBOX__INTERVAL", "10s", "AUDIT_OUTBOX"},
		{"stream batch", "EVENTS_RETENTION__AUDIT_STREAM__BATCH_SIZE", "0", "AUDIT_STREAM"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRequired(t)
			t.Setenv(tc.key, tc.value)
			_, err := config.Load()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want the text %q", err, tc.want)
			}
		})
	}
	setRequired(t)
	t.Setenv("EVENTS_HTTP__TRUSTED_PROXIES", "10.0.0.0/8,192.168.1.5/32")
	t.Setenv("EVENTS_DATABASE__MAX_CONNS", "4")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Audit.RefusedLimitPerMinute != 30 {
		t.Fatalf("default limit = %d, want 30", cfg.Audit.RefusedLimitPerMinute)
	}
	if p, err := cfg.HTTP.TrustedProxyPrefixes(); err != nil || len(p) != 2 {
		t.Fatalf("proxies = %v, %v", p, err)
	}
}
