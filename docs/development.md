# Development

## Commands

| Task | Command |
| ---- | ------- |
| Lint | `test -z "$(gofmt -l .)" && go vet ./... && golangci-lint run` |
| Unit tests | `go test -race ./...` |
| Integration tests | `go test -race -tags=integration -timeout 40m ./...` |
| Generate the server code | `go generate ./...` |

The integration tests need Docker. They start PostgreSQL, Valkey, and the identity image. The first run pulls the images. Each integration test file has the build tag `integration`.

## Code generation

`internal/handlers/http/gen/events.gen.go` comes from `api/openapi/Constellation.Events.openapi.yaml` with `oapi-codegen`. The CI runs `go generate ./...` and fails if the work tree changes.

## Update the contract

1. Copy `generated/openapi/Constellation.Events.openapi.yaml` and `services/events/manifest.json` from a release tag of `constellation-contracts` into `api/openapi/`. The manifest file is `events-manifest.json`.
2. Write the tag in `api/openapi/CONTRACTS_VERSION`.
3. Run `go generate ./...`.
4. Run the tests.

## Dockerfile

The build stage uses `golang:1.27.4-alpine`. The final stage uses `gcr.io/distroless/static-debian12:nonroot`. Both images are pinned by digest. The entry point is `/events`. The container exposes the ports 8080 and 9090. The image workflow builds the image on each pull request and pushes it on a version tag.

## The identity fixture of the tests

The package `resources/test/testserver` starts one network with:

- one PostgreSQL container with the database `identity`, and one new database for each events service,
- one Valkey container,
- the identity image `ghcr.io/techorionai/constellation-identity` with the tag `0.7.0`.

The identity container has the issuer `http://localhost:<port>` with a fixed host port. The test makes one P-256 key pair for each client at the start and writes the clients file. The rate limits of identity are off.

| Client ID | App | Roles |
| --------- | --- | ----- |
| `events` | `events` | None. The implicit role `identity:service` lets it publish its manifest |
| `helpdesk-svc` | `helpdesk` | `events:producer` |
| `crm-svc` | `crm` | `events:producer` |
| `sales-svc` | `sales` | `events:producer` |
| `reader-svc` | `reader` | `events:reader` |
| `noroles-svc` | `noroles` | None |

Identity gives a token only for the scopes of the roles of the client. A client with no role cannot get a token with an `events` scope.

The events service runs in the test process, except in the image test. A test client gets tokens with `auth/clientcred` of platform-go.
