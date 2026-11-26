# constellation-events

The events service of the Constellation platform. It keeps the central event catalog. Each service registers its event types and their JSON Schemas at start. Agents, TechOrion AI, and admins read the catalog through the API.

Events do not go through this service. They go through the outbox and the Valkey streams.

## Docs

- [Architecture](docs/architecture.md)
- [Configuration](docs/configuration.md)
- [Event catalog](docs/event-catalog.md)
- [Consumer lag](docs/consumer-lag.md)
- [Development](docs/development.md)

## Quick start

```sh
go test -race ./...
go test -race -tags=integration -timeout 40m ./...
docker build -t constellation-events .
```

The API contract is in `constellation-contracts`. This repository has a copy of the OpenAPI file in `api/openapi/`. The file `api/openapi/CONTRACTS_VERSION` gives the release tag of the copy.

## License

MIT. See `LICENSE`.
