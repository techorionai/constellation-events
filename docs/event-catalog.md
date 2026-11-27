# Event catalog

## Register the event types

`PUT /v1/event-type-registrations/{app}` replaces the registration of an app. The last registration wins. The caller must be a service client of the app: the token has `principal_type` `service` and the `app` claim. The `app` claim, `{app}`, and the body `app` must be equal. The token needs the scope `events:event_type:register` and the single audience `events`.

A producer service uses the `eventtypes` package of `constellation-platform-go`. The package checks the file in the CI of the producer and registers it at start.

### Order of the checks

1. 401: the token is not valid, or it has another audience.
2. 403: the token does not have the scope. The body has `missingScopes`.
3. 413 `content_too_large`: the body is larger than `EVENTS_MAX_BODY_BYTES`.
4. 400 `invalid_request`: the body is not JSON, has data after the object, has an unknown field at the top level or in an event type, has no required member, or has a member of the wrong JSON type. Members inside a `dataSchema` are free.
5. 422 `registration_app_mismatch`: the caller is not a service client of the app, or the app values differ.
6. 422 with the content checks. The answer has all errors, each with a JSON Pointer in `errors`.

### Content checks

| Code | Rule |
| ---- | ---- |
| `source_mismatch` | `source` is `/<app>`. |
| `event_type_app_mismatch` | Each `type` matches the `EventType` pattern and starts with `<app>.`. |
| `duplicate_event_type` | Each `type` occurs one time. |
| `limit_exceeded` | A registration has 1 to 200 types. A description has 1 to 1024 Unicode code points. An app has a maximum of 1000 types in the catalog, removed types included. |
| `invalid_data_schema` | See the next section. |

### The data schema

A `dataSchema` is untrusted data. The service compiles it as JSON Schema draft 2020-12 with a loader that refuses each URL. The service never reads a file and never calls the network.

- The schema is a JSON object with a maximum of 64 KiB.
- `$schema`, if present, is `https://json-schema.org/draft/2020-12/schema`.
- `$id` is not set.
- Each `$ref` and `$dynamicRef` starts with `#`. A reference to a file, a URL, or a relative file gives 422.
- A local reference points to a subschema that the service examines, or to its `$anchor`. A reference into an unknown keyword, `const`, `enum`, or `default` gives 422.
- The nesting depth is a maximum of 32.
- Each `pattern` is in the RE2 subset (Go `regexp`): no back reference and no look-around.
- Format and content assertions stay off.
- The error has a fixed text. The text of the compiler is not in a response or in a log.

The answer is the same for a file that exists and for a file that does not exist.

## Removed types

A type that the app registered before and that is not in the new body stays in the catalog with `removedAt`. A type that comes back gets no `removedAt`. `createdAt` does not change. During a rolling deploy, `removedAt` of a type can change for a short time. A reader must not use `removedAt` as a signal that no more events of the type come.

The same registration again (the same content, in any order) gives 200 with the stored `version` and `registeredAt`, and no change. A different number text (`1` and `1.0`) in a schema is a different content. It gives a new version.

## Breaking changes

The service has no compatibility check of a `dataSchema` change. A breaking change of `data` uses a new event type, for example `helpdesk.ticket.created_v2`, and the old type gets `deprecated: true`. A compatible change, for example a new optional field, keeps the type.

## Read the catalog

| Operation | Result |
| --------- | ------ |
| `EventTypeRegistrations_list` | The registrations in the order of `app`. Each has the types with no `removedAt`. |
| `EventTypeRegistrations_get` | One registration. 404 `registration_not_found` for an unknown app or an app that does not match its pattern. |
| `EventTypes_list` | The types in the order of `type`. The filters are `app` and `includeRemoved`. An `app` value that does not match the pattern gives 400 `invalid_parameter`. |
| `EventTypes_get` | One type, also a removed one, with `removedAt`. 404 `event_type_not_found` for an unknown type. |

The schemas keep the text of their numbers.

### Pages

- `pageSize` of `EventTypes_list` has the default 50 and the maximum 200. `pageSize` of `EventTypeRegistrations_list` has the default 10 and the maximum 10, because each item can be large.
- A page also ends before the item that makes the response body larger than 1 MiB. A page always has the first item. A server can thus return fewer items than `pageSize`. A page of `EventTypes_list` fits the default response limit of the events client of `constellation-platform-go`. A reader of registrations sets `WithMaxResponseBytes` to the body limit plus 1 MiB.
- The page token is opaque. It holds the operation, the filters, and the last key. A token that does not decode, that is longer than 1024 characters, or that has another operation or other filters gives 400 `invalid_page_token`.

## Roles

| Role | Permissions |
| ---- | ----------- |
| `events:producer` | `events:event_type:register`, `events:catalog:read` |
| `events:reader` | `events:catalog:read` |

The service publishes this manifest to identity at start. Identity gives a service client the scopes of its roles only.

## Audit entries

A registration writes audit entries with the `platform-go` audit outbox. An entry has the action `events:event_type:register`, the target type `registration`, and the target ID of the app. The actor is the verified principal of the request. The service never sets the actor.

| Result | When | Summary | `details` |
| ------ | ---- | ------- | --------- |
| `success` | A registration changes the catalog. An unchanged registration writes no entry. | `Event types of app <app> registered, version <version>` | `version`, `eventTypeCount`, `addedCount`, `removedCount`, `modifiedCount`, the lists `added`, `removed`, and `modified`, and the flags `addedTruncated`, `removedTruncated`, and `modifiedTruncated`. |
| `invalid` | The service refuses the request with 422. | `Registration of event types refused` | `code`: `registration_app_mismatch`, `limit_exceeded`, or the code of the first field error. |
| `denied` | The token has not the scope `events:event_type:register`. The response is 403. | `Registration of event types denied` | `code`: `forbidden`. |

- The entry of a `success` result is in the transaction of the registration. If the audit write fails, the registration rolls back. A database error gives 503 `unavailable` with `Retry-After: 5`. A rule error of the writer gives 500 `internal_error`.
- The entry of an `invalid` or `denied` result is in a short transaction of its own. If the write fails, the response keeps its 4xx status, and the service writes an error log record with the error type.
- Each list has a maximum of 30 type names, in the order of the type. A flag is `true` only when its list has more names than the limit. The counts are always complete.
- The target ID of a refused entry is the app of the path only when it is a valid app key. Else the entry has no `target`, and `details` has `appValid` `false`.
- An entry has no JSON Schema, no description, and no text of the schema compiler.
- Each principal can write a maximum of 30 entries of the results `invalid` and `denied` in each minute (`EVENTS_AUDIT__REFUSED_LIMIT_PER_MINUTE`). Over the limit, the request keeps its 4xx response, the service writes no entry, and the counter `constellation.events.audit.refused_dropped` increments. The limit is in the process, so N replicas give a maximum of N times the limit.
