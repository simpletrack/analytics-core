# analytics-core

`analytics-core` is a business-neutral analytics data-plane core.

It owns event intake contracts, event bus adapters, ingestion boundaries,
storage adapters, metadata, and analysis primitives. Product concerns such as
pricing, billing, onboarding, account management, and dashboards belong to
upstream applications.

## P1 Scope

- Standard event envelope.
- Collect request validation and event normalization.
- Collect-time property intake guardrails for key shape, count, scalar value types, and string length.
- Collect handler that publishes normalized envelopes to an EventBus.
- fasthttp collect route that accepts JSON events and keeps framework details outside the core handler.
- EventBus abstraction.
- Direct in-process bus for tests and local demos.
- Redis Stream bus for the first deployable queue path.
- Kafka adapter boundary reserved for high-throughput deployments.
- Storage `EventWriter` boundary.
- Storage-neutral typed property row expansion for event and user properties.
- ClickHouse native property batch writer for typed property rows.
- Storage `PropertyIndexingEventWriter` decorator that composes event writes with property indexing.
- GORM/MySQL property indexing status guard for cross-table retry and duplicate protection.
- ClickHouse table routing by tenant, project, and source.
- ClickHouse native batch writer based on `clickhouse-go/v2 PrepareBatch`.
- GORM/MySQL ingestion status guard for idempotent event writes.
- Events and Realtime query plans built through a unified GORM ClickHouse query builder.
- Typed Events sort allowlist for query-builder controlled ordering.
- Typed Events filter field and operator allowlists with invalid-query error classification.
- Opt-in end-to-end test for collect -> Redis Stream -> ingestion -> ClickHouse -> Realtime/Events reader.
- Dependency-free browser tracker SDK for auto pageview, SPA route pageview, manual track, identify, debug logging, and snippet queue replay.

## Queue Semantics

- Consumers acknowledge messages only after storage writes succeed.
- Failed messages stay pending and are retried before new messages are read.
- Redis Stream attempts are read from consumer group pending metadata, not from local process memory.
- `MaxAttempts` with `DeadLetterStream` moves exhausted messages to a dead-letter stream and acknowledges the original message.
- Ingestion treats duplicate event writes as successful processing, so at-least-once delivery does not create duplicate stored events.
- Property indexing has a second MySQL checkpoint because the event row may be committed before property rows fail.
- Property indexing retries reclaim only explicit failed checkpoints; ambiguous processing checkpoints are not retried automatically because ClickHouse may already contain the property rows.
- `ingestion.Processor` is the P1 worker boundary: queue adapters supply messages, and `storage.EventWriter` owns durable append plus idempotency.

## HTTP API

- The event reporting hot path uses fasthttp as the mature third-party HTTP library.
- `internal/collect/httpapi` only decodes JSON, maps HTTP status codes, and calls `collect.Handler`.
- P1 exposes `POST /collect` as the stable event reporting route; health and query routes are added separately from the reporting hot path.
- `collect.Handler` remains framework-independent so future gRPC, SDK, or worker entrypoints can reuse the same validation and publish path.
- Event and user properties are accepted as bounded scalar bags in P1; nested objects and arrays wait for the explicit property storage model.

## Browser SDK

- `sdk/browser/tracker.js` is the P1 browser tracker for websites and docs snippets.
- It reads `data-tenant-id`, `data-project-id`, `data-source-id`, and `data-collect-url` from the script tag and posts the stable collect request shape to `POST /collect`.
- It sends an automatic `pageview`, patches SPA history changes for route pageviews, attaches allowlisted UTM/click-id query parameters, exposes `window.simpletrack.track(name, properties)`, exposes `window.simpletrack.identify(id, userProperties)`, and can suppress sends when opt-in DNT handling is enabled.
- The SDK does not add cookies. It stores the current `distinct_id` in `localStorage` when available and falls back to an in-memory id when storage is blocked.
- Browser SDK tests run with Node's built-in test runner and a fake browser window, so the SDK remains dependency-free.

## Storage Boundaries

- Event writes use `storage.EventWriter`; callers must not import ClickHouse driver APIs directly.
- Event and user property expansion uses `storage.FlattenEventProperties` so writers, metadata dictionaries, and future property filters share one typed-row contract.
- Typed property rows use `clickhouse.PropertyBatchWriter` behind `storage.PropertyIndexingEventWriter`, so ingestion still depends on `storage.EventWriter` while property indexing remains repairable.
- Duplicate protection uses `storage.EventWriteGuard` before appending an event row and commits only after durable append succeeds.
- Property duplicate protection uses `storage.PropertyWriteGuard` before appending property rows and commits only after the property batch succeeds.
- ClickHouse physical table names are generated by `clickhouse.TableRouter`; analysis and collect packages only see the logical `events` model.
- Events and Realtime queries use `storage.EventQueryBuilder`; P1 returns SQL plans before adding an execution adapter.
- ClickHouse query execution uses `storage.EventReader`; it executes query plans through GORM Raw and returns storage-neutral `EventRecord` rows.
- Events sorting is constrained to typed allowlisted fields and directions before SQL generation.
- Events filtering is constrained to typed allowlisted fields and operators; invalid caller filters return `storage.ErrInvalidEventQuery` before SQL generation.
- High-throughput event inserts use native ClickHouse batches. GORM is used for query construction and MySQL status storage, not as the default event insert hot path.

## Development

Install dependencies:

```powershell
go mod download
```

Run the standard verification set:

```powershell
go test ./...
go test -run Example ./...
go vet ./...
node --test sdk/browser/tracker.test.mjs
```

When network access is unstable, set the local proxy before dependency commands:

```powershell
$env:HTTP_PROXY='http://localhost:7897'
$env:HTTPS_PROXY='http://localhost:7897'
$env:GOPROXY='https://proxy.golang.org,direct'
```

## Local Runtime Dependencies

Start Redis Stack, MySQL, and ClickHouse for integration work:

```powershell
docker compose up -d
```

The compose file exposes:

- Redis Stack: `localhost:26379`, RedisInsight: `http://localhost:28001`
- MySQL: `localhost:23306`, database/user/password `analytics_core`
- ClickHouse HTTP: `http://localhost:28123`, native TCP: `localhost:29000`, database/user/password `analytics_core`

Ports can be overridden with `ANALYTICS_CORE_REDIS_PORT`,
`ANALYTICS_CORE_REDIS_INSIGHT_PORT`, `ANALYTICS_CORE_MYSQL_PORT`,
`ANALYTICS_CORE_CLICKHOUSE_HTTP_PORT`, and
`ANALYTICS_CORE_CLICKHOUSE_NATIVE_PORT`.

Run Redis Stream integration tests against the local Redis container:

```powershell
$env:ANALYTICS_CORE_REDIS_ADDR='127.0.0.1:26379'
go test ./internal/eventbus/redisstream
```

Run the full P1 data-pipeline end-to-end test against Redis, MySQL, and ClickHouse:

```powershell
$env:ANALYTICS_CORE_E2E='1'
go test ./internal/e2e -run TestCollectToRealtimeAndEventsPipeline -v
```

The end-to-end test creates routed ClickHouse event and property tables, publishes pageview and custom-event collect requests through Redis Stream, consumes them through `ingestion.Processor`, writes events and typed property rows with MySQL idempotency guards, then reads them back through Events, Realtime, and allowlisted property-filter queries.

Stop local dependencies:

```powershell
docker compose down
```

