// Package openapi holds the copies of the files of constellation-contracts
// that the service reads at run time: the OpenAPI file, for the
// x-required-scopes values of the operations, and the permission manifest of
// the events service.
package openapi

import "embed"

// Spec is the OpenAPI file of constellation-contracts.
//
//go:embed Constellation.Events.openapi.yaml
var Spec []byte

// Files holds the permission manifest of the events service as
// events-manifest.json.
//
//go:embed events-manifest.json
var Files embed.FS

// EventsManifestPath is the path of the manifest in Files.
const EventsManifestPath = "events-manifest.json"
