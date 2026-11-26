# Architecture

The events service keeps the central event catalog of the platform. Each service registers its event types and their JSON Schemas at start. Agents, TechOrion AI, and admins read the catalog through the API. Events do not go through this service. They go through the outbox and the Valkey streams. See section 9 of `VISION.md` in the planner repository.

## Layout

The code follows the hexagonal architecture guide of the planner repository.

| Folder | Content |
| ------ | ------- |
| `cmd/events/` | `main.go` (`serve`) and `healthcheck.go`. |
| `api/openapi/` | Copy of the OpenAPI file and of the permission manifest from `constellation-contracts`. `CONTRACTS_VERSION` gives the release tag. |
| `internal/domain/` | The rules of a registration, the JSON Schema check, the canonical hash. It imports the standard library and the JSON Schema library only. |
| `internal/ports/registrations/` | The port of the catalog store. It has the method `ListApps` for the lag collector. |
| `internal/ports/streams/` | The port of the event streams: the groups of a stream and the lag of a group. |
| `internal/adapters/postgres/` | The PostgreSQL adapter of the registrations port. |
| `internal/adapters/valkey/` | The Valkey adapter of the streams port. It uses `streams/valkey` of platform-go. |
| `internal/app/commands/` | The use case `RegisterEventTypes`. |
| `internal/app/queries/` | The read use cases, the page builder, and the query `CollectConsumerLag`. |
| `internal/handlers/worker/lag/` | The lag worker: the timer, the snapshot, the instruments, and the logs. |
| `internal/handlers/http/` | The HTTP handlers, the scope table, the register body decode, and the page token. |
| `internal/handlers/http/gen/` | The generated server code. |
| `internal/services/` | The composition root: startup, shutdown, readiness. |
| `internal/config/` | The configuration. |
| `migrations/` | The SQL migrations. |
| `resources/test/` | The test helpers (including the mocks of the ports) and the lint and image tests. |

The depguard rules of `.golangci.yml` check the layers. The lint test in `resources/test/lint` tests the rules.

## Routes

| Route | Operation | Scope |
| ----- | --------- | ----- |
| `PUT /v1/event-type-registrations/{app}` | `EventTypeRegistrations_register` | `events:event_type:register` |
| `GET /v1/event-type-registrations` | `EventTypeRegistrations_list` | `events:catalog:read` |
| `GET /v1/event-type-registrations/{app}` | `EventTypeRegistrations_get` | `events:catalog:read` |
| `GET /v1/event-types` | `EventTypes_list` | `events:catalog:read` |
| `GET /v1/event-types/{type}` | `EventTypes_get` | `events:catalog:read` |

The scope table reads the `x-required-scopes` value of each operation from the embedded OpenAPI file. A `/v1` route with no entry gives 500 (fail closed).

The service accepts JWT access tokens with the single audience `events`. It does not accept personal API tokens.

## Startup order

1. Load the config. Set up telemetry with the service name `events`.
2. Connect PostgreSQL and Valkey. Start the admin server. `/readyz` is 503.
3. Run the migrations with the advisory lock.
4. Register the lag instruments and start the lag worker in a goroutine. It starts after the migrations, because it reads the `registrations` table.
5. Start the publisher of the own permission manifest. The checker of the publisher is part of the readiness.
6. Start the public server. Mark the service as started. `/readyz` is 200 when all checks pass.

Shutdown: drain the admin server, stop the public server, stop the publisher, stop the lag worker (cancel its context and wait at most 5 s), unregister the instruments, close the clients, stop the admin server.

Identity gives a token for the audience `events` only after it has the manifest of the events service. A producer thus waits for the first publish of the manifest.

## Data model

| Table | Columns |
| ----- | ------- |
| `registrations` | `app` (key), `source`, `version`, `hash`, `registered_at`, `registered_by` |
| `event_types` | `type` (key), `app`, `description`, `data_schema` (jsonb), `deprecated`, `created_at`, `updated_at`, `removed_at` |

A registration runs in one transaction. The transaction locks the row of the app. Registrations of one app run one after the other. The service has no outbox, no relay, and no consumer group. The lag worker only reads the streams. See `consumer-lag.md`. The audit data of a registration is a log record.

## Telemetry

- A span for each request (`otelhttp`) and for each use case.
- The metric `constellation.events.registration` counts the registrations by the attribute `outcome`: `changed`, `unchanged`, `invalid`, or `error`.
- The logs and the spans have no JSON Schema, no description, and no text of the schema compiler.
- The metrics of the lag worker: `constellation.events.consumer.lag`, `constellation.events.lag.collected`, `constellation.events.lag.groups`, and `constellation.events.lag.errors`. See `consumer-lag.md`. A lag error has no effect on `/readyz`.
