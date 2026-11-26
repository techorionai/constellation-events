# Consumer lag

The events service measures the lag of each consumer group on the event stream of each registered app. Because the events service measures the lag, a consumer that stopped also has a lag value. See section 9.2 of `VISION.md` in the planner repository.

## Meaning of the lag

The lag of a group is the age of the oldest event that the group did not acknowledge. It is the older of two events:

- The oldest pending event of the group (read but not acknowledged).
- The first event after the last event that the group received (not read yet).

The lag is 0 when there is no such event. The age comes from the stream ID of the event. The stream ID has the Valkey time of the write. A clock difference between Valkey and the events service changes the lag a little.

The service gives an age only. It does not give a count of events. The `lag` and `pending` fields of `XINFO GROUPS` can be null, for example after `XDEL`, or after a trim of events that the group did not read.

## How the service finds the streams

The service reads the `registrations` table. It uses the stream `events.stream:<app>` of each registered app. A producer registers at its start, before it writes events. The lag of a stream of an app with no registration is not reported.

The Valkey user of the service has only the read commands that the collection needs on the streams: `XINFO GROUPS`, `XPENDING`, and `XRANGE`. The collection does not write to a stream.

## Instruments

All instruments are in the meter `events`.

| Instrument | Type and unit | Attributes | Meaning |
| ---------- | ------------- | ---------- | ------- |
| `constellation.events.consumer.lag` | Gauge, `s` | `stream`, `group` | The lag of a group. `stream` is the app name, for example `helpdesk`, not the key. The values are the same as the attributes of the consumer metrics of platform-go. |
| `constellation.events.lag.collected` | Gauge, `s` | None | The Unix time of the end of the last collection with no failure. It has no value before the first such collection. |
| `constellation.events.lag.groups` | Gauge, `{group}` | None | The count of groups that the last collection found, before the cap. |
| `constellation.events.lag.errors` | Counter, `{error}` | `operation` | The count of failed operations. `operation` is `apps`, `groups`, or `lag`. |

The gauge callbacks read a snapshot. They do not call Valkey or PostgreSQL.

## Collection

- The first collection runs at the start of the worker. Then one collection runs each `EVENTS_LAG__INTERVAL` (default `30s`, from `5s` to `2m`).
- The time limit of one collection is the interval.
- A collection with no error is a full collection. A group that a consumer deletes during the collection is not an error.
- The worker does not change `/readyz`. A lag error does not remove an instance from the load balancer.

### The rule of 5 collections

After a failed operation, the snapshot keeps the last value of each group that the failure affects. It keeps the value for a maximum of 5 collections. Then it drops the value. A failed `lag` affects one group. A failed `groups` affects all groups of the stream. A failed `apps` affects all groups. The kept value is the last value, with no change. With the default interval, 5 collections are 2.5 minutes. After that time, no value is better than an old value, and the alert on `constellation.events.lag.collected` shows the problem.

### The caps on groups

One collection reads a maximum of 100 groups for each stream and a maximum of 1000 groups in total, in the order of stream and group. `constellation.events.lag.groups` shows the count that the service found before the caps. When the collection does not read a group because of a cap, the worker writes the warn log `events.lag.groups_capped`. It writes the log only when the count of dropped groups changes. A collection in which a stream did not answer keeps the old count and the old log state.

The collection skips a group whose name is longer than 256 bytes or is not valid UTF-8. A name that is not valid UTF-8 makes the export of all metrics fail. The worker writes the warn log `events.lag.groups_skipped` when the count of skipped groups changes.

## Alerts

Each instance of the events service runs the worker. There is no leader election. Use `max` over the instances, for the lag and for the collection time. One working instance is sufficient.

- The alert of VISION 9.2: `max(constellation.events.consumer.lag)` is more than 12 hours (43200 s).
- The alert for a collector that does not work: `max(constellation.events.lag.collected)` is older than 5 minutes.

Example queries:

```promql
max by (stream, group) (constellation_events_consumer_lag_seconds) > 43200
(time() - max(constellation_events_lag_collected_seconds) > 300) or absent(constellation_events_lag_collected_seconds)
```

The Prometheus names depend on the exporter. The OpenTelemetry names are in the table above. A value of `constellation.events.lag.collected` that does not exist also means that no full collection happened.

## Limits

- Retention: for an event that the group did not read, the lag stops at about the retention period of the stream (3 days). The trim deletes the event. A pending event stays in the pending list after a trim, so its lag continues to go up.
- Clock: the lag uses the time of the events service and the time in the stream ID. A clock difference changes the lag a little.
- Count: the service gives no count of events that a group did not read or acknowledge.
- ACL: the ACL of Valkey cannot isolate consumer groups. The Valkey user of the service can also acknowledge the events of other groups. The collection sends only read commands.
- Keys: the ACL rule of platform-go does not refuse `SCAN`. The service does not use it.
