# Read-Side Optimization Policy

`analytics-core` keeps ClickHouse read-side acceleration behind the storage
adapter. Product services and handlers must keep using `storage.EventReader`
and `storage.EventQueryBuilder`; they must not pick physical tables,
projections, materialized views, or aggregate tables directly.

This policy defines when a direct fact-table query can move to a stronger
ClickHouse optimization. It is a gate for future work, not a reason to add
physical structures before a query shape is stable.

## Required Evidence

Every optimization proposal must include all of the following:

- Product capability: Realtime, Events, Dashboard, Goal, Breakdown, Funnel,
  Journey, Retention, or another named feature.
- Query evidence from `EventQueryPlan.QueryEvidence()`: family, read path,
  current optimization, effective limit, offset, time-bound flags, bounded
  time window, scalar filter count, property filter count, property table
  usage, typed property filter shapes without values, sort field, and sort
  direction.
- Explain-plan evidence for the same query shape when a physical-structure
  change is under discussion. The explain output should show whether the
  current path still reads through the primary routed fact table, whether
  typed property filters add `CreatingSets`, and whether the read already uses
  the expected sort or primary-key condition.
- Representative benchmark data from local or staging ClickHouse with the same
  query family and filter shape.
- Expected row volume, source count, time window, page size, and sort behavior.
- A regression plan covering valid queries, invalid fields, invalid properties,
  pagination, sorting, `visit_id`, and tenant/project/source boundaries.
- Write-path impact: whether the change increases insert latency, write
  amplification, failure recovery work, or consistency risk.

Without this evidence, the correct decision is to stay on the direct fact-table
path and improve property governance or query-plan limits first.

## Decision Matrix

| Optimization | Use When | Do Not Use When |
| --- | --- | --- |
| Direct fact table | P1/P1.5 default for Realtime and Raw Events, especially when the query uses routed tenant/project/source tables, bounded time windows, capped limits, and allowlisted filters | The same query shape repeatedly exceeds the benchmark threshold and cannot be fixed by query-plan guardrails |
| Projection | A small number of Events or Realtime detail queries are hot, stable, and still need event-level rows from the same fact table | The product needs precomputed metrics, cross-source aggregation, or changing dimensions |
| Materialized view | A derived metric has stable semantics, is read often, and is too expensive to compute from fact rows on every request | The query is still exploratory, its dimensions are changing, or the feature still needs raw event rows |
| Hourly aggregate table | Dashboard, Goal, Top pages, Top events, or trend queries need fixed time buckets and stable dimensions | Raw Events, Realtime lists, or ad-hoc property exploration are the target |

## Initial Thresholds

The thresholds below are engineering gates for P1.5, not product SLAs. They
should be tightened after staging traffic and production hardware are known.

- Keep direct fact-table reads while the current query benchmark remains within
  the same order of magnitude as the recorded baseline for its query family.
- Consider a projection only when the same Events or Realtime detail query shape
  exceeds the baseline by 2x or more in two consecutive benchmark runs, and the
  shape is stable in filters, sort field, sort direction, and time window.
- Consider a materialized view only when a metric query is stable for at least
  one product feature and repeated direct aggregation over fact rows is the
  dominant cost in benchmark or trace evidence.
- Consider an hourly aggregate table when the feature is time-series first:
  dashboard counters, trends, goals, top events, top pages, or source health.
- Do not add a projection, materialized view, or aggregate table only because
  `pressure=high`. Pressure is a triage bucket; it is not proof that the query
  needs a new physical structure.

## Current Baseline Decisions

- Events and Realtime currently use `EventQueryOptimizationDirectFactTable`.
- Property filters use the typed property table through the query builder, not
  JSON extraction in product handlers.
- ClickHouse event writes stay on native `clickhouse-go/v2 PrepareBatch`.
  Benchmarks show GORM `CreateInBatches` works but is slower and allocates more
  for the current event model, so GORM remains a query-builder and low-frequency
  management-write tool.
- KafkaBus is only a package boundary today. Do not write Kafka pressure claims
  until a real Kafka adapter exists and has its own benchmark.

## Verification Commands

Run the standard suite before accepting any read-side optimization:

```powershell
go test ./...
```

Capture explain-plan evidence when evaluating whether a stable query shape
needs projection, materialized-view, or aggregate-table work:

```powershell
$env:ANALYTICS_CORE_CLICKHOUSE_BENCH='1'
$env:ANALYTICS_CORE_CLICKHOUSE_BENCH_ROWS='100000'
go test ./internal/e2e -run TestEventReaderClickHouseExplain -count=1 -v
```

Treat the explain output as evidence, not as an optimization trigger by
itself. A query can show `CreatingSets` or a full-granule property-table path
and still stay on the direct fact-table route if benchmark cost and product
pressure remain acceptable.

Run read-side ClickHouse execution benchmarks when local Docker dependencies
are available:

```powershell
$env:ANALYTICS_CORE_CLICKHOUSE_BENCH='1'
go test ./internal/e2e -run '^$' -bench 'BenchmarkEventReaderClickHouseExecution' -benchmem -count=3
```

Run write-path comparison benchmarks when a proposal changes ClickHouse event
write strategy:

```powershell
$env:ANALYTICS_CORE_CLICKHOUSE_BENCH='1'
$env:ANALYTICS_CORE_CLICKHOUSE_WRITER_BATCH_ROWS='100'
go test ./internal/e2e -run '^$' -bench 'Benchmark(BatchWriter|GORMCreateInBatches|NativePrepareBatchRows|GORMCreateInBatchesRows)ClickHouseExecution' -benchmem -count=3
```

The benchmark output must be saved in the implementation decision record or
source-reading notes that justify the change.
