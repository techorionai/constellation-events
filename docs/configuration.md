# Configuration

The service reads its settings from environment variables with the prefix `EVENTS_`. A double underscore `__` separates the levels of a nested name. A variable with a `_FILE` form reads its value from a file where the platform-go package says so.

## Variables

| Variable | Default | Rule | Use |
| -------- | ------- | ---- | --- |
| `EVENTS_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` | Log level. |
| `EVENTS_HTTP__ADDR` | `:8080` | | Public listener. |
| `EVENTS_ADMIN__ADDR` | `:9090` | | Listener for `/livez` and `/readyz`. Do not publish it at the edge. |
| `EVENTS_ADMIN__CHECK_TIMEOUT` | `2s` | | Time limit of the readiness checks. |
| `EVENTS_ADMIN__DRAIN_DELAY` | `5s` | | Time that `/readyz` is 503 before the public server stops. |
| `EVENTS_DATABASE__URL`, `EVENTS_DATABASE__NAME` | required | platform-go `clients/postgres` rules | PostgreSQL. Also `EVENTS_DATABASE__URL_FILE`. |
| `EVENTS_VALKEY__ADDRESS`, `EVENTS_VALKEY__KEY_PREFIX` | required | platform-go `clients/valkey` rules | Valkey. Use the key prefix `events`. The service uses Valkey for the readiness check and to read the consumer groups of the event streams. |
| `EVENTS_AUTH__ISSUER` | required | `https`, or `http` with `EVENTS_AUTH__ALLOW_INSECURE_HTTP` | Issuer of the identity service. It is the `iss` claim that the verifier requires. |
| `EVENTS_AUTH__JWKS_URL` | `<issuer>/.well-known/jwks.json` | | URL of the key set of identity. |
| `EVENTS_AUTH__REFRESH_INTERVAL`, `EVENTS_AUTH__MIN_REFETCH_INTERVAL`, `EVENTS_AUTH__LEEWAY`, `EVENTS_AUTH__HTTP_TIMEOUT` | `10m`, `30s`, `30s`, `5s` | Same as `auth/jwt.Config` of platform-go | Verifier settings. |
| `EVENTS_AUTH__ALLOW_INSECURE_HTTP` | `false` | | Allows `http` for the issuer. Use it only for local compose and tests. |
| `EVENTS_CLIENT__*` | required | `auth/clientcred.Config` of platform-go | The service client `events` of identity: `ID`, `KEY_ID`, `PRIVATE_KEY` (or `PRIVATE_KEY_FILE`), `ISSUER`, `TOKEN_URL`. The service uses it to publish its manifest. |
| `EVENTS_IDENTITY__URL` | required | `clients/identity.Config` of platform-go | Internal URL of identity. Also `EVENTS_IDENTITY__ALLOW_INSECURE_HTTP`. |
| `EVENTS_LAG__INTERVAL` | `30s` | `5s` to `2m` | Time between two lag collections. The maximum gives two or more collections in the window of 5 minutes of the alert for the collection time. See `consumer-lag.md`. |
| `EVENTS_MAX_BODY_BYTES` | `4194304` (4 MiB) | 1 MiB to 16 MiB | Largest body of a registration. A larger body gives 413 `content_too_large`. |

The audience of the service is the constant `events`. The service does not read `EVENTS_AUTH__AUDIENCE`. A value in that variable has no effect.

The service stops at start with a message that names the variable when a rule fails.

## Commands

| Command | Use |
| ------- | --- |
| `events` | Start the server. |
| `events healthcheck` | Exit 0 if `/readyz` of the local admin port is 200. Else exit 1. The container health check uses it. It reads `EVENTS_ADMIN__ADDR`. |

Telemetry uses the standard `OTEL_*` variables. See the `telemetry` package of `constellation-platform-go`.

## The service client

The service needs a client in the clients file of identity:

```yaml
clients:
  - id: events
    app: events
    name: Events service
    keys:
      - kid: events-2026-11
        jwk: { kty: EC, crv: P-256, x: "...", y: "..." }
```

The client needs no role. The implicit role `identity:service` lets it publish its manifest. Each producer service needs the role `events:producer`. A reader needs the role `events:reader`.
