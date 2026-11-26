// Package config holds the configuration of the events service. The values
// come from environment variables with the prefix EVENTS_.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/techorionai/constellation-platform-go/auth/clientcred"
	"github.com/techorionai/constellation-platform-go/auth/jwt"
	"github.com/techorionai/constellation-platform-go/clients/identity"
	clientpg "github.com/techorionai/constellation-platform-go/clients/postgres"
	clientvk "github.com/techorionai/constellation-platform-go/clients/valkey"
	platformconfig "github.com/techorionai/constellation-platform-go/config"
	"github.com/techorionai/constellation-platform-go/health"
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
	return errors.Join(errs...)
}
