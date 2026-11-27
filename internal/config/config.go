// Package config holds the configuration of the events service. The values
// come from environment variables with the prefix EVENTS_.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"
	"time"

	"github.com/techorionai/constellation-platform-go/auth/clientcred"
	"github.com/techorionai/constellation-platform-go/auth/jwt"
	"github.com/techorionai/constellation-platform-go/clientinfo"
	"github.com/techorionai/constellation-platform-go/clients/identity"
	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"
	clientvk "github.com/techorionai/constellation-platform-go/clients/valkey"
	platformconfig "github.com/techorionai/constellation-platform-go/config"
	"github.com/techorionai/constellation-platform-go/health"
	"github.com/techorionai/constellation-platform-go/retention"
)

// Prefix is the prefix of all environment variables of the service.
const Prefix = "EVENTS_"

// Audience is the audience of the access tokens that the service accepts. The
// operator cannot change it.
const Audience = "events"

// Limits of MaxBodyBytes.
const (
	DefaultMaxBodyBytes = 4 << 20
	MinMaxBodyBytes     = 1 << 20
	MaxMaxBodyBytes     = 16 << 20
)

// Limits of the lag interval. The maximum gives two or more collections in the
// window of 5 minutes of the default alert for the collection time.
const (
	DefaultLagInterval = 30 * time.Second
	MinLagInterval     = 5 * time.Second
	MaxLagInterval     = 2 * time.Minute
)

// Lag is the config of the consumer lag worker.
type Lag struct {
	Interval time.Duration `env:"INTERVAL" envDefault:"30s"`
}

// HTTP is the config of the public listener.
type HTTP struct {
	Addr string `env:"ADDR" envDefault:":8080"`
	// TrustedProxies is a comma list of CIDR prefixes of the own reverse
	// proxies. The default is empty, so the service ignores X-Forwarded-For.
	TrustedProxies string `env:"TRUSTED_PROXIES"`
}

// Retention is the config of the retention jobs of the audit outbox and the
// audit stream. The default period of each job is 7 days.
type Retention struct {
	AuditOutbox retention.Config `envPrefix:"AUDIT_OUTBOX__"`
	AuditStream retention.Config `envPrefix:"AUDIT_STREAM__"`
}

// Audit is the config of the audit entries.
type Audit struct {
	// RefusedLimitPerMinute is the maximum number of entries of refused
	// registrations for each principal in one minute.
	RefusedLimitPerMinute int `env:"REFUSED_LIMIT_PER_MINUTE" envDefault:"30"`
}

// Limits of the pool and of the refused limit.
const (
	// MinMaxConns is the smallest pool size. A retention tick uses 2
	// connections, and the requests need at least 2 more.
	MinMaxConns = 4
	// MaxRefusedLimitPerMinute is the largest refused limit.
	MaxRefusedLimitPerMinute = 10000
	// DefaultRefusedLimitPerMinute is the default refused limit.
	DefaultRefusedLimitPerMinute = 30
)

// DefaultRetentionPeriod is the default period of the retention jobs.
const DefaultRetentionPeriod = 7 * 24 * time.Hour

// TrustedProxyPrefixes parses TrustedProxies.
func (h HTTP) TrustedProxyPrefixes() ([]netip.Prefix, error) {
	cfg, err := clientinfo.ParseTrustedProxies(strings.TrimSpace(h.TrustedProxies))
	if err != nil {
		return nil, err
	}
	return cfg.TrustedProxies, nil
}

// Auth is the config of the token verifier. It has the fields of
// auth/jwt.Config except Audience. The audience is the constant Audience.
type Auth struct {
	Issuer             string        `env:"ISSUER,required"`
	JWKSURL            string        `env:"JWKS_URL"`
	RefreshInterval    time.Duration `env:"REFRESH_INTERVAL" envDefault:"10m"`
	MinRefetchInterval time.Duration `env:"MIN_REFETCH_INTERVAL" envDefault:"30s"`
	Leeway             time.Duration `env:"LEEWAY" envDefault:"30s"`
	HTTPTimeout        time.Duration `env:"HTTP_TIMEOUT" envDefault:"5s"`
	AllowInsecureHTTP  bool          `env:"ALLOW_INSECURE_HTTP" envDefault:"false"`
}

// JWT returns the config of the verifier, with the audience of the service.
func (a Auth) JWT() jwt.Config {
	return jwt.Config{
		Issuer:             a.Issuer,
		Audience:           Audience,
		JWKSURL:            a.JWKSURL,
		RefreshInterval:    a.RefreshInterval,
		MinRefetchInterval: a.MinRefetchInterval,
		Leeway:             a.Leeway,
		HTTPTimeout:        a.HTTPTimeout,
		AllowInsecureHTTP:  a.AllowInsecureHTTP,
	}
}

// Config is the configuration of the service.
type Config struct {
	LogLevel     slog.Level        `env:"LOG_LEVEL" envDefault:"INFO"`
	HTTP         HTTP              `envPrefix:"HTTP__"`
	Admin        health.Config     `envPrefix:"ADMIN__"`
	Database     clientpg.Config   `envPrefix:"DATABASE__"`
	Valkey       clientvk.Config   `envPrefix:"VALKEY__"`
	Auth         Auth              `envPrefix:"AUTH__"`
	Client       clientcred.Config `envPrefix:"CLIENT__"`
	Identity     identity.Config   `envPrefix:"IDENTITY__"`
	Lag          Lag               `envPrefix:"LAG__"`
	Retention    Retention         `envPrefix:"RETENTION__"`
	Audit        Audit             `envPrefix:"AUDIT__"`
	MaxBodyBytes int64             `env:"MAX_BODY_BYTES" envDefault:"4194304"`
}

// Load reads the environment and checks the values.
func Load() (Config, error) {
	var cfg Config
	if err := platformconfig.Load(Prefix, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks the values that the loader cannot check.
func (c Config) Validate() error {
	var errs []error
	if c.MaxBodyBytes < MinMaxBodyBytes || c.MaxBodyBytes > MaxMaxBodyBytes {
		errs = append(errs, fmt.Errorf("%sMAX_BODY_BYTES: must be from %d to %d", Prefix, MinMaxBodyBytes, MaxMaxBodyBytes))
	}
	if c.Lag.Interval < MinLagInterval || c.Lag.Interval > MaxLagInterval {
		errs = append(errs, fmt.Errorf("%sLAG__INTERVAL: must be from %s to %s", Prefix, MinLagInterval, MaxLagInterval))
	}
	if _, err := c.HTTP.TrustedProxyPrefixes(); err != nil {
		errs = append(errs, fmt.Errorf("%sHTTP__TRUSTED_PROXIES: %w", Prefix, err))
	}
	if c.Database.MaxConns != 0 && c.Database.MaxConns < MinMaxConns {
		errs = append(errs, fmt.Errorf("%sDATABASE__MAX_CONNS: must be %d or more", Prefix, MinMaxConns))
	}
	if c.Audit.RefusedLimitPerMinute < 1 || c.Audit.RefusedLimitPerMinute > MaxRefusedLimitPerMinute {
		errs = append(errs, fmt.Errorf("%sAUDIT__REFUSED_LIMIT_PER_MINUTE: must be from 1 to %d", Prefix, MaxRefusedLimitPerMinute))
	}
	if err := c.Retention.AuditOutbox.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("%sRETENTION__AUDIT_OUTBOX__*: %w", Prefix, err))
	}
	if err := c.Retention.AuditStream.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("%sRETENTION__AUDIT_STREAM__*: %w", Prefix, err))
	}
	return errors.Join(errs...)
}
