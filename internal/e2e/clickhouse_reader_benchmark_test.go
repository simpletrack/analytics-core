package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
	gormclickhouse "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const clickHouseBenchmarkEnabledEnv = "ANALYTICS_CORE_CLICKHOUSE_BENCH"
const clickHouseBenchmarkRowsEnv = "ANALYTICS_CORE_CLICKHOUSE_BENCH_ROWS"
const benchmarkRecentEventsWindowRows = 5000 // benchmarkRecentEventsWindowRows keeps Events recent-window scenarios at 50 matching rows.

// benchmarkBoundedScalarWindowSpec describes one bounded scalar Events window
// that should only enter the suite when it stays distinct from the wide-window
// fixture at the current row volume.
type benchmarkBoundedScalarWindowSpec struct {
	name       string // name identifies the benchmark and explain subcase.
	windowRows int    // windowRows maps the bounded time slice to seeded one-second event rows.
}

var benchmarkBoundedScalarWindowSpecs = []benchmarkBoundedScalarWindowSpec{
	{
		name:       "medium_events_scalar_bounded_24h_window",
		windowRows: 24 * 60 * 60,
	},
	{
		name:       "medium_events_scalar_bounded_72h_window",
		windowRows: 72 * 60 * 60,
	},
	{
		name:       "medium_events_scalar_bounded_7d_window",
		windowRows: 7 * 24 * 60 * 60,
	},
}

// benchmarkReadScenario captures one timed EventReader benchmark case.
type benchmarkReadScenario struct {
	name                 string                                                // name identifies the benchmark subcase and expected query pressure.
	realtimeSince        time.Time                                             // realtimeSince marks the Realtime lower bound; zero means this is not a Realtime scenario.
	realtimeEligibleRows int                                                   // realtimeEligibleRows records the expected rows ClickHouse can scan for Realtime.
	eventFrom            time.Time                                             // eventFrom marks the Events lower bound; zero means this is not an Events scenario.
	eventTo              time.Time                                             // eventTo marks the Events upper bound used by EventReader.
	eventWindowRows      int                                                   // eventWindowRows records the expected seeded rows inside the Events window.
	plan                 func(context.Context) (storage.EventQueryPlan, error) // plan builds the sealed SQL shape for optional preflight checks.
	read                 func(context.Context) ([]storage.EventRecord, error)  // read executes the exact EventReader path being timed.
	check                func(*testing.B, []storage.EventRecord)               // check verifies result semantics before and during timing.
}

// benchmarkExplainScenario captures one ClickHouse EXPLAIN case aligned with the benchmark suite.
type benchmarkExplainScenario struct {
	name                 string                                                // name identifies the explain subcase and expected query pressure.
	realtimeSince        time.Time                                             // realtimeSince marks the Realtime lower bound; zero means this is not a Realtime scenario.
	realtimeEligibleRows int                                                   // realtimeEligibleRows records the expected rows ClickHouse can scan for Realtime.
	eventFrom            time.Time                                             // eventFrom marks the Events lower bound; zero means this is not an Events scenario.
	eventTo              time.Time                                             // eventTo marks the Events upper bound used by EventReader.
	eventWindowRows      int                                                   // eventWindowRows records the expected seeded rows inside the Events window.
	plan                 func(context.Context) (storage.EventQueryPlan, error) // plan builds the sealed EventQueryPlan passed to ClickHouse EXPLAIN.
}

// BenchmarkEventReaderClickHouseExecution measures EventReader latency against
// a real local ClickHouse instance and a seeded routed event table.
//
// The benchmark is opt-in because it depends on docker-compose ClickHouse. It
// complements the builder-only benchmark by measuring actual GORM Raw execution
// and ClickHouse read latency for recent-window Realtime, wide-since Realtime,
// recent-window Events, bounded scalar Events windows, wide-window Events, and
// high property-filtered Events query shapes.
func BenchmarkEventReaderClickHouseExecution(b *testing.B) {
	if os.Getenv(clickHouseBenchmarkEnabledEnv) != "1" {
		b.Skipf("set %s=1 to run real ClickHouse EventReader benchmark", clickHouseBenchmarkEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancel()

	// Phase 1: connect only to ClickHouse. This benchmark intentionally avoids
	// Redis and MySQL so read-side latency is not mixed with pipeline overhead.
	clickConn := openBenchmarkClickHouseNative(ctx, b)
	defer clickConn.Close()
	clickGorm := openBenchmarkClickHouseGORM(ctx, b)

	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		b.Fatalf("new table router failed: %v", err)
	}
	key := uniqueRoutingKey()
	table, err := router.RouteKey(key)
	if err != nil {
		b.Fatalf("route benchmark table failed: %v", err)
	}
	createBenchmarkEventTable(ctx, b, clickConn, table.Physical)
	propertyTable := createBenchmarkPropertyTable(ctx, b, clickConn, table)
	defer dropBenchmarkTables(b, clickConn, propertyTable.Physical, table.Physical)

	// Phase 2: seed deterministic event and property rows before timing starts.
	// The dataset is intentionally small enough for local iteration but shaped
	// to exercise direct recent reads, scalar filters, and property subqueries.
	rowCount := benchmarkRowCount()
	seedBenchmarkEvents(ctx, b, clickConn, table, key, rowCount)
	seedBenchmarkProperties(ctx, b, clickConn, table, key, rowCount)

	builder, err := clickhouse.NewEventQueryBuilder(router, clickhouse.WithAllowedPropertyFilters(
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"},
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "plan"},
		storage.PropertySelector{Scope: storage.PropertyScopeUser, Name: "tier"},
	))
	if err != nil {
		b.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := clickhouse.NewEventReader(clickGorm, builder)
	if err != nil {
		b.Fatalf("new event reader failed: %v", err)
	}

	baseTime := benchmarkBaseTime()
	recentRealtimeSince := benchmarkRecentRealtimeSince(rowCount)
	wideRealtimeSince := baseTime.Add(-time.Minute)
	recentEventsFrom := benchmarkRecentEventsFrom(rowCount)
	wideEventsFrom := benchmarkWideEventsFrom()
	eventsTo := benchmarkEndTime(rowCount)
	scenarios := []benchmarkReadScenario{
		{
			name:                 "low_realtime_recent_window",
			realtimeSince:        recentRealtimeSince,
			realtimeEligibleRows: 300,
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListRealtime(ctx, benchmarkRealtimeQuery(key, recentRealtimeSince))
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkRealtimeRows(b, records, key, rowCount, recentRealtimeSince, 300)
			},
		},
		{
			name:                 "low_realtime_wide_since",
			realtimeSince:        wideRealtimeSince,
			realtimeEligibleRows: rowCount,
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListRealtime(ctx, benchmarkRealtimeQuery(key, wideRealtimeSince))
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkRealtimeRows(b, records, key, rowCount, wideRealtimeSince, rowCount)
			},
		},
		{
			name:            "medium_events_scalar_recent_window",
			eventFrom:       recentEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: benchmarkRecentEventsWindowRows,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkScalarEventsQuery(key, recentEventsFrom, eventsTo))
			},
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListEvents(ctx, benchmarkScalarEventsQuery(key, recentEventsFrom, eventsTo))
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkScalarRows(b, records, key)
			},
		},
	}
	// Add only the bounded scalar windows that are still narrower than the
	// current fixture. This keeps each benchmark branch semantically distinct
	// from the unbounded wide-window scenario.
	scenarios = append(
		scenarios,
		benchmarkBoundedScalarReadScenarios(key, rowCount, eventsTo, builder, reader)...,
	)
	scenarios = append(scenarios,
		benchmarkReadScenario{
			name:            "high_events_property_recent_window",
			eventFrom:       recentEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: benchmarkRecentEventsWindowRows,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkPropertyEventsQuery(key, recentEventsFrom, eventsTo))
			},
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListEvents(ctx, benchmarkPropertyEventsQuery(key, recentEventsFrom, eventsTo))
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkPropertyRows(b, records, key)
			},
		},
		benchmarkReadScenario{
			name:            "medium_events_scalar_wide_window",
			eventFrom:       wideEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: rowCount,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkScalarEventsQuery(key, wideEventsFrom, eventsTo))
			},
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListEvents(ctx, benchmarkScalarEventsQuery(key, wideEventsFrom, eventsTo))
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkScalarRows(b, records, key)
			},
		},
		benchmarkReadScenario{
			name:            "high_events_property_wide_window",
			eventFrom:       wideEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: rowCount,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkPropertyEventsQuery(key, wideEventsFrom, eventsTo))
			},
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListEvents(ctx, benchmarkPropertyEventsQuery(key, wideEventsFrom, eventsTo))
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkPropertyRows(b, records, key)
			},
		},
	)

	// Phase 3: verify each scenario once, then time only EventReader execution.
	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
			if !scenario.realtimeSince.IsZero() {
				assertBenchmarkRealtimeEvidence(b, rowCount, scenario.realtimeSince, scenario.realtimeEligibleRows)
				b.Logf("realtime window evidence: since=%s eligible_rows=%d row_count=%d", scenario.realtimeSince.Format(time.RFC3339), scenario.realtimeEligibleRows, rowCount)
			}
			if !scenario.eventFrom.IsZero() {
				assertBenchmarkEventWindowEvidence(b, rowCount, scenario.eventFrom, scenario.eventTo, scenario.eventWindowRows)
				b.Logf("events window evidence: from=%s to=%s eligible_rows=%d row_count=%d", scenario.eventFrom.Format(time.RFC3339), scenario.eventTo.Format(time.RFC3339), scenario.eventWindowRows, rowCount)
			}
			if scenario.plan != nil {
				plan, err := scenario.plan(context.Background())
				if err != nil {
					b.Fatalf("preflight query plan failed: %v", err)
				}
				assertBenchmarkEventPlanEvidence(b, plan, scenario.eventFrom, scenario.eventTo)
			}

			records, err := scenario.read(context.Background())
			if err != nil {
				b.Fatalf("preflight read failed: %v", err)
			}
			scenario.check(b, records)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				records, err := scenario.read(context.Background())
				if err != nil {
					b.Fatalf("event reader query failed: %v", err)
				}
				scenario.check(b, records)
			}
		})
	}
}

// TestEventReaderClickHouseExplain records ClickHouse explain plans for read-side candidates.
func TestEventReaderClickHouseExplain(t *testing.T) {
	if os.Getenv(clickHouseBenchmarkEnabledEnv) != "1" {
		t.Skipf("set %s=1 to run real ClickHouse EventReader explain test", clickHouseBenchmarkEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancel()

	// Phase 1: build the same routed ClickHouse fixture as the reader benchmark
	// so explain output can be compared directly with benchmark latency.
	clickConn := openExplainClickHouseNative(ctx, t)
	defer clickConn.Close()
	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	key := uniqueRoutingKey()
	table, err := router.RouteKey(key)
	if err != nil {
		t.Fatalf("route benchmark table failed: %v", err)
	}
	createBenchmarkEventTable(ctx, t, clickConn, table.Physical)
	propertyTable := createBenchmarkPropertyTable(ctx, t, clickConn, table)
	defer dropBenchmarkTables(t, clickConn, propertyTable.Physical, table.Physical)

	// Phase 2: seed enough deterministic data to make property-filter plans
	// meaningful without depending on a production dataset.
	rowCount := benchmarkRowCount()
	seedBenchmarkEvents(ctx, t, clickConn, table, key, rowCount)
	seedBenchmarkProperties(ctx, t, clickConn, table, key, rowCount)

	builder, err := clickhouse.NewEventQueryBuilder(router, clickhouse.WithAllowedPropertyFilters(
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"},
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "plan"},
		storage.PropertySelector{Scope: storage.PropertyScopeUser, Name: "tier"},
	))
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	baseTime := benchmarkBaseTime()
	recentRealtimeSince := benchmarkRecentRealtimeSince(rowCount)
	wideRealtimeSince := baseTime.Add(-time.Minute)
	recentEventsFrom := benchmarkRecentEventsFrom(rowCount)
	wideEventsFrom := benchmarkWideEventsFrom()
	eventsTo := benchmarkEndTime(rowCount)
	scenarios := []benchmarkExplainScenario{
		{
			name:                 "low_realtime_recent_window",
			realtimeSince:        recentRealtimeSince,
			realtimeEligibleRows: 300,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildRealtimeQuery(ctx, benchmarkRealtimeQuery(key, recentRealtimeSince))
			},
		},
		{
			name:                 "low_realtime_wide_since",
			realtimeSince:        wideRealtimeSince,
			realtimeEligibleRows: rowCount,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildRealtimeQuery(ctx, benchmarkRealtimeQuery(key, wideRealtimeSince))
			},
		},
		{
			name:            "medium_events_scalar_recent_window",
			eventFrom:       recentEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: benchmarkRecentEventsWindowRows,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkScalarEventsQuery(key, recentEventsFrom, eventsTo))
			},
		},
	}
	// Keep the explain suite aligned with the benchmark suite so latency and
	// EXPLAIN evidence describe the same bounded scalar ClickHouse window shapes.
	scenarios = append(
		scenarios,
		benchmarkBoundedScalarExplainScenarios(key, rowCount, eventsTo, builder)...,
	)
	scenarios = append(scenarios,
		benchmarkExplainScenario{
			name:            "high_events_property_recent_window",
			eventFrom:       recentEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: benchmarkRecentEventsWindowRows,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkPropertyEventsQuery(key, recentEventsFrom, eventsTo))
			},
		},
		benchmarkExplainScenario{
			name:            "medium_events_scalar_wide_window",
			eventFrom:       wideEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: rowCount,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkScalarEventsQuery(key, wideEventsFrom, eventsTo))
			},
		},
		benchmarkExplainScenario{
			name:            "high_events_property_wide_window",
			eventFrom:       wideEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: rowCount,
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkPropertyEventsQuery(key, wideEventsFrom, eventsTo))
			},
		},
	)

	// Phase 3: log structured evidence and ClickHouse's index-aware explain
	// output. The test fails if any scenario cannot be planned or explained.
	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			if !scenario.realtimeSince.IsZero() {
				assertBenchmarkRealtimeEvidence(t, rowCount, scenario.realtimeSince, scenario.realtimeEligibleRows)
				t.Logf("realtime window evidence: since=%s eligible_rows=%d row_count=%d", scenario.realtimeSince.Format(time.RFC3339), scenario.realtimeEligibleRows, rowCount)
			}
			if !scenario.eventFrom.IsZero() {
				assertBenchmarkEventWindowEvidence(t, rowCount, scenario.eventFrom, scenario.eventTo, scenario.eventWindowRows)
				t.Logf("events window evidence: from=%s to=%s eligible_rows=%d row_count=%d", scenario.eventFrom.Format(time.RFC3339), scenario.eventTo.Format(time.RFC3339), scenario.eventWindowRows, rowCount)
			}

			plan, err := scenario.plan(ctx)
			if err != nil {
				t.Fatalf("build query plan failed: %v", err)
			}
			if !scenario.eventFrom.IsZero() {
				assertBenchmarkEventPlanEvidence(t, plan, scenario.eventFrom, scenario.eventTo)
			}
			t.Logf("query evidence: %+v", plan.QueryEvidence())
			for _, line := range explainPlan(ctx, t, clickConn, plan) {
				t.Logf("explain: %s", line)
			}
		})
	}
}

// benchmarkRowCount returns the local fixture size used for opt-in ClickHouse runs.
func benchmarkRowCount() int {
	// Keep the default dataset local-friendly while allowing larger manual
	// pressure runs without changing source code.
	value := strings.TrimSpace(os.Getenv(clickHouseBenchmarkRowsEnv))
	if value == "" {
		return 10000
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= benchmarkRecentEventsWindowRows {
		return 10000
	}
	return parsed
}

// benchmarkBaseTime returns a stable timestamp anchor for seeded rows.
func benchmarkBaseTime() time.Time {
	return time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
}

// benchmarkRecentRealtimeSince returns a bounded recent-window anchor.
func benchmarkRecentRealtimeSince(rowCount int) time.Time {
	// Keep the recent-window scenario tied to the newest fixture rows. This
	// prevents larger pressure runs from accidentally turning Realtime into a
	// wide historical scan while still returning the latest 50 records.
	return benchmarkBaseTime().Add(time.Duration(rowCount-300) * time.Second)
}

// benchmarkRecentEventsFrom returns a bounded Events window anchor.
func benchmarkRecentEventsFrom(rowCount int) time.Time {
	// Events needs a wider recent window than Realtime because the scalar and
	// typed property fixtures only match every 100 seeded rows. Keeping exactly
	// 5000 recent rows gives each Events scenario 50 matching records while still
	// staying distinct from wide-window pressure runs.
	return benchmarkBaseTime().Add(time.Duration(rowCount-benchmarkRecentEventsWindowRows) * time.Second)
}

// benchmarkBoundedScalarReadScenarios expands the bounded scalar benchmark
// suite for every window that still stays distinct from the current fixture.
func benchmarkBoundedScalarReadScenarios(
	key clickhouse.RoutingKey,
	rowCount int,
	eventsTo time.Time,
	builder *clickhouse.EventQueryBuilder,
	reader *clickhouse.EventReader,
) []benchmarkReadScenario {
	var scenarios []benchmarkReadScenario

	// Build one scenario per bounded scalar window so the benchmark suite can
	// compare 24h, multi-day, and week-long bounded scans without duplicating
	// the surrounding EventReader harness.
	for _, spec := range benchmarkBoundedScalarWindowSpecs {
		if !benchmarkSupportsBoundedScalarWindow(rowCount, spec.windowRows) {
			continue
		}

		boundedEventsFrom := benchmarkBoundedEventsFrom(rowCount, spec.windowRows)
		scenarios = append(scenarios, benchmarkReadScenario{
			name:            spec.name,
			eventFrom:       boundedEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: benchmarkBoundedEventsRows(spec.windowRows),
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkScalarEventsQuery(key, boundedEventsFrom, eventsTo))
			},
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListEvents(ctx, benchmarkScalarEventsQuery(key, boundedEventsFrom, eventsTo))
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkScalarRows(b, records, key)
			},
		})
	}

	return scenarios
}

// benchmarkBoundedScalarExplainScenarios mirrors the bounded scalar benchmark
// suite so the explain output always refers to the same window shapes.
func benchmarkBoundedScalarExplainScenarios(
	key clickhouse.RoutingKey,
	rowCount int,
	eventsTo time.Time,
	builder *clickhouse.EventQueryBuilder,
) []benchmarkExplainScenario {
	var scenarios []benchmarkExplainScenario

	// Reuse the exact same window expansion rules as the timed benchmark. This
	// avoids comparing latency from one bounded slice with EXPLAIN output from a
	// different slice length.
	for _, spec := range benchmarkBoundedScalarWindowSpecs {
		if !benchmarkSupportsBoundedScalarWindow(rowCount, spec.windowRows) {
			continue
		}

		boundedEventsFrom := benchmarkBoundedEventsFrom(rowCount, spec.windowRows)
		scenarios = append(scenarios, benchmarkExplainScenario{
			name:            spec.name,
			eventFrom:       boundedEventsFrom,
			eventTo:         eventsTo,
			eventWindowRows: benchmarkBoundedEventsRows(spec.windowRows),
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, benchmarkScalarEventsQuery(key, boundedEventsFrom, eventsTo))
			},
		})
	}

	return scenarios
}

// benchmarkSupportsBoundedScalarWindow reports whether the bounded slice stays
// distinct from the wide-window benchmark fixture at the current row volume.
func benchmarkSupportsBoundedScalarWindow(rowCount int, windowRows int) bool {
	return rowCount > windowRows
}

// benchmarkBoundedEventsFrom returns the lower bound for one bounded Events
// history slice when the seeded dataset is larger than that window.
func benchmarkBoundedEventsFrom(rowCount int, windowRows int) time.Time {
	return benchmarkBaseTime().Add(time.Duration(rowCount-windowRows) * time.Second)
}

// benchmarkBoundedEventsRows returns how many seeded rows belong to the
// bounded Events history slice selected for the current benchmark scenario.
func benchmarkBoundedEventsRows(windowRows int) int {
	return windowRows
}

// benchmarkWideEventsFrom returns the lower bound used for wide Events scans.
func benchmarkWideEventsFrom() time.Time {
	return benchmarkBaseTime().Add(-time.Minute)
}

// benchmarkEndTime returns an exclusive upper bound after the newest fixture row.
func benchmarkEndTime(rowCount int) time.Time {
	// Seeded rows occupy [baseTime, baseTime+(rowCount-1)s]. Using rowCount
	// seconds keeps the upper bound exclusive while preserving exact window
	// sizes in QueryEvidence, including the canonical 24h = 86400s slice.
	return benchmarkBaseTime().Add(time.Duration(rowCount) * time.Second)
}

// benchmarkEligibleRows returns how many seeded events satisfy since.
func benchmarkEligibleRows(rowCount int, since time.Time) int {
	baseTime := benchmarkBaseTime()
	if !since.After(baseTime) {
		return rowCount
	}
	elapsed := int(since.Sub(baseTime) / time.Second)
	if elapsed >= rowCount {
		return 0
	}
	return rowCount - elapsed
}

// benchmarkWindowRows returns how many seeded events satisfy a bounded Events window.
func benchmarkWindowRows(rowCount int, from time.Time, to time.Time) int {
	baseTime := benchmarkBaseTime()
	start := 0
	if from.After(baseTime) {
		start = int(from.Sub(baseTime) / time.Second)
	}
	end := rowCount
	latest := benchmarkEndTime(rowCount)
	if to.Before(latest) {
		end = int(to.Sub(baseTime) / time.Second)
	}
	if start < 0 {
		start = 0
	}
	if end > rowCount {
		end = rowCount
	}
	if end < start {
		return 0
	}
	return end - start
}

// benchmarkRealtimeQuery returns one Realtime query shape for reader benchmarks.
func benchmarkRealtimeQuery(key clickhouse.RoutingKey, since time.Time) storage.RealtimeQuery {
	return storage.RealtimeQuery{
		TenantID:  key.TenantID,
		ProjectID: key.ProjectID,
		SourceID:  key.SourceID,
		Since:     since,
		Limit:     50,
	}
}

// benchmarkScalarEventsQuery returns the medium-pressure scalar Events shape.
func benchmarkScalarEventsQuery(key clickhouse.RoutingKey, from time.Time, to time.Time) storage.EventListQuery {
	return storage.EventListQuery{
		TenantID:   key.TenantID,
		ProjectID:  key.ProjectID,
		SourceID:   key.SourceID,
		EventName:  "page_view",
		DistinctID: "visitor_2",
		From:       from,
		To:         to,
		Limit:      50,
	}
}

// benchmarkPropertyEventsQuery returns the high-pressure typed-property Events shape.
func benchmarkPropertyEventsQuery(key clickhouse.RoutingKey, from time.Time, to time.Time) storage.EventListQuery {
	return storage.EventListQuery{
		TenantID:      key.TenantID,
		ProjectID:     key.ProjectID,
		SourceID:      key.SourceID,
		EventName:     "signup_clicked",
		DistinctID:    "visitor_1",
		From:          from,
		To:            to,
		SortField:     storage.EventSortByReceivedAt,
		SortDirection: storage.EventSortDescending,
		Limit:         50,
		Filters: []storage.EventFilter{
			{Field: storage.EventFilterBySessionID, Operator: storage.EventFilterEquals, Value: "session_1"},
			{Field: storage.EventFilterByVisitID, Operator: storage.EventFilterEquals, Value: "visit_1"},
			{Field: storage.EventFilterBySourceType, Operator: storage.EventFilterNotEquals, Value: "server"},
		},
		PropertyFilters: benchmarkPropertyFilters(),
	}
}

// benchmarkPropertyFilters returns the shared high-pressure typed property shape.
func benchmarkPropertyFilters() []storage.EventPropertyFilter {
	return []storage.EventPropertyFilter{
		{Scope: storage.PropertyScopeEvent, Name: "button", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "hero"},
		{Scope: storage.PropertyScopeEvent, Name: "plan", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "pro"},
		{Scope: storage.PropertyScopeUser, Name: "tier", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "team"},
	}
}

// openBenchmarkClickHouseNative waits for the native ClickHouse write path.
func openBenchmarkClickHouseNative(ctx context.Context, b *testing.B) driver.Conn {
	b.Helper()
	return openClickHouseNativeForTB(ctx, b)
}

// openExplainClickHouseNative waits for ClickHouse in tests that need native EXPLAIN.
func openExplainClickHouseNative(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	return openClickHouseNativeForTB(ctx, t)
}

// openClickHouseNativeForTB waits for the native ClickHouse protocol to become ready.
func openClickHouseNativeForTB(ctx context.Context, tb testing.TB) driver.Conn {
	tb.Helper()

	// Retry native handshakes because Docker may expose the port before the
	// server can complete ClickHouse protocol negotiation.
	ticker := time.NewTicker(e2eDependencyPollInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		conn, err := clickhousedriver.Open(&clickhousedriver.Options{
			Addr: []string{envOr("ANALYTICS_CORE_CLICKHOUSE_NATIVE_ADDR", "127.0.0.1:29000")},
			Auth: clickhousedriver.Auth{
				Database: envOr("ANALYTICS_CORE_CLICKHOUSE_DATABASE", "analytics_core"),
				Username: envOr("ANALYTICS_CORE_CLICKHOUSE_USER", "analytics_core"),
				Password: envOr("ANALYTICS_CORE_CLICKHOUSE_PASSWORD", "analytics_core"),
			},
		})
		if err != nil {
			lastErr = err
		} else if pingErr := conn.Ping(ctx); pingErr == nil {
			return conn
		} else {
			lastErr = pingErr
			_ = conn.Close()
		}

		select {
		case <-ctx.Done():
			tb.Fatalf("native clickhouse did not become ready before timeout: %v", lastErr)
		case <-ticker.C:
		}
	}
}

// openBenchmarkClickHouseGORM waits for the GORM read path.
func openBenchmarkClickHouseGORM(ctx context.Context, b *testing.B) *gorm.DB {
	b.Helper()

	// Use the same DSN helper as e2e tests so benchmark reads exercise the
	// production EventReader connection shape.
	dsn := envOr("ANALYTICS_CORE_CLICKHOUSE_GORM_DSN", defaultClickHouseGORMDSN())
	ticker := time.NewTicker(e2eDependencyPollInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		db, err := gorm.Open(gormclickhouse.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			lastErr = err
		} else if sqlDB, sqlErr := db.DB(); sqlErr != nil {
			lastErr = sqlErr
		} else if pingErr := sqlDB.PingContext(ctx); pingErr == nil {
			return db
		} else {
			lastErr = pingErr
			_ = sqlDB.Close()
		}

		select {
		case <-ctx.Done():
			b.Fatalf("gorm clickhouse did not become ready before timeout: %v", lastErr)
		case <-ticker.C:
		}
	}
}

// createBenchmarkEventTable creates one routed event table for the benchmark.
func createBenchmarkEventTable(ctx context.Context, tb testing.TB, conn driver.Conn, tableName string) {
	tb.Helper()

	// DDL comes from the production schema helper so benchmark fixtures cannot
	// drift from EventWriter/EventReader expectations.
	ddl, err := clickhouse.CreateEventTableStatement(clickhouse.Table{
		Logical:  "events",
		Physical: tableName,
	})
	if err != nil {
		tb.Fatalf("build clickhouse event DDL failed: %v", err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		tb.Fatalf("create clickhouse event table failed: %v", err)
	}
}

// createBenchmarkPropertyTable creates the typed property table paired with table.
func createBenchmarkPropertyTable(ctx context.Context, tb testing.TB, conn driver.Conn, table clickhouse.Table) clickhouse.Table {
	tb.Helper()

	// Property table routing must stay derived from the event table to preserve
	// the same tenant/project/source physical boundary as production writes.
	propertyTable, err := clickhouse.PropertyTableFor(table)
	if err != nil {
		tb.Fatalf("route property table failed: %v", err)
	}
	ddl, err := clickhouse.CreatePropertyTableStatement(table)
	if err != nil {
		tb.Fatalf("build clickhouse property DDL failed: %v", err)
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		tb.Fatalf("create clickhouse property table failed: %v", err)
	}
	return propertyTable
}

// dropBenchmarkTables tears down benchmark tables before the native connection closes.
func dropBenchmarkTables(tb testing.TB, conn driver.Conn, tableNames ...string) {
	tb.Helper()

	// Drop all routed tables with checked errors so benchmark cleanup failures
	// cannot leave stale ClickHouse tables that distort later pressure runs.
	dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var joined error
	for _, tableName := range tableNames {
		if err := conn.Exec(dropCtx, "DROP TABLE IF EXISTS "+quoteIdent(tableName)); err != nil {
			joined = errors.Join(joined, fmt.Errorf("drop table %s: %w", tableName, err))
		}
	}
	if joined != nil {
		tb.Fatalf("drop benchmark clickhouse tables failed: %v", joined)
	}
}

// seedBenchmarkEvents inserts deterministic event rows through a native batch.
func seedBenchmarkEvents(ctx context.Context, tb testing.TB, conn driver.Conn, table clickhouse.Table, key clickhouse.RoutingKey, rows int) {
	tb.Helper()

	// Seed with the native driver so fixture setup is fast and does not measure
	// ingestion idempotency or EventBus behavior.
	batch, err := conn.PrepareBatch(ctx, benchmarkEventInsertStatement(table.Physical))
	if err != nil {
		tb.Fatalf("prepare event benchmark batch failed: %v", err)
	}
	baseTime := benchmarkBaseTime()
	for idx := 0; idx < rows; idx++ {
		eventName := "page_view"
		if idx%2 == 1 {
			eventName = "signup_clicked"
		}
		eventTime := baseTime.Add(time.Duration(idx) * time.Second)
		if err := batch.Append(
			benchmarkEventID(idx),
			key.TenantID,
			key.ProjectID,
			key.SourceID,
			"web",
			eventName,
			fmt.Sprintf("visitor_%d", idx%100),
			fmt.Sprintf("session_%d", idx%50),
			fmt.Sprintf("visit_%d", idx%25),
			eventTime,
			eventTime.Add(250*time.Millisecond),
			`{"path":"/bench","button":"hero","plan":"pro"}`,
			`{"tier":"team"}`,
			"benchmark",
		); err != nil {
			_ = batch.Abort()
			tb.Fatalf("append event benchmark row failed: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		tb.Fatalf("send event benchmark batch failed: %v", err)
	}
}

// seedBenchmarkProperties inserts typed property rows for high-pressure queries.
func seedBenchmarkProperties(ctx context.Context, tb testing.TB, conn driver.Conn, table clickhouse.Table, key clickhouse.RoutingKey, rows int) {
	tb.Helper()

	// Only a subset of rows need property triples: enough to make the property
	// subqueries return data without making setup dominate local benchmark runs.
	propertyTable, err := clickhouse.PropertyTableFor(table)
	if err != nil {
		tb.Fatalf("route property table failed: %v", err)
	}
	batch, err := conn.PrepareBatch(ctx, benchmarkPropertyInsertStatement(propertyTable.Physical))
	if err != nil {
		tb.Fatalf("prepare property benchmark batch failed: %v", err)
	}
	baseTime := benchmarkBaseTime()
	for idx := 1; idx < rows; idx += 100 {
		eventTime := baseTime.Add(time.Duration(idx) * time.Second)
		appendBenchmarkPropertyRows(tb, batch, key, idx, eventTime)
	}
	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		tb.Fatalf("send property benchmark batch failed: %v", err)
	}
}

// appendBenchmarkPropertyRows appends the event/user property triple for one event.
func appendBenchmarkPropertyRows(tb testing.TB, batch driver.Batch, key clickhouse.RoutingKey, idx int, eventTime time.Time) {
	tb.Helper()

	values := []struct {
		scope     storage.PropertyScope
		name      string
		valueType storage.PropertyValueType
		value     string
	}{
		{scope: storage.PropertyScopeEvent, name: "button", valueType: storage.PropertyValueString, value: "hero"},
		{scope: storage.PropertyScopeEvent, name: "plan", valueType: storage.PropertyValueString, value: "pro"},
		{scope: storage.PropertyScopeUser, name: "tier", valueType: storage.PropertyValueString, value: "team"},
	}
	for _, value := range values {
		// Keep property row identity aligned with the seeded event row so tuple
		// IN property filters can match through EventReader.
		if err := batch.Append(
			benchmarkEventID(idx),
			key.TenantID,
			key.ProjectID,
			key.SourceID,
			"web",
			"signup_clicked",
			fmt.Sprintf("visitor_%d", idx%100),
			fmt.Sprintf("session_%d", idx%50),
			fmt.Sprintf("visit_%d", idx%25),
			eventTime,
			eventTime.Add(250*time.Millisecond),
			"benchmark",
			string(value.scope),
			value.name,
			string(value.valueType),
			value.value,
			0.0,
			false,
		); err != nil {
			_ = batch.Abort()
			tb.Fatalf("append property benchmark row failed: %v", err)
		}
	}
}

// explainPlan returns ClickHouse's index-aware execution explanation for plan.
func explainPlan(ctx context.Context, tb testing.TB, conn driver.Conn, plan storage.EventQueryPlan) []string {
	tb.Helper()

	// Prefix the sealed query plan instead of rebuilding SQL here; this keeps
	// EXPLAIN tied to the same SQL and bound arguments EventReader executes.
	rows, err := conn.Query(ctx, "EXPLAIN indexes = 1 "+plan.SQL, plan.Args...)
	if err != nil {
		tb.Fatalf("explain query plan failed: %v", err)
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			tb.Fatalf("scan explain row failed: %v", err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("iterate explain rows failed: %v", err)
	}
	if len(lines) == 0 {
		tb.Fatal("explain returned no rows")
	}
	return lines
}

// benchmarkEventInsertStatement returns the fixture insert statement for events.
func benchmarkEventInsertStatement(tableName string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (`event_id`,`tenant_id`,`project_id`,`source_id`,`source_type`,`event_name`,`distinct_id`,`session_id`,`visit_id`,`event_time`,`received_at`,`properties`,`user_properties`,`source`)",
		quoteIdent(tableName),
	)
}

// benchmarkPropertyInsertStatement returns the fixture insert statement for properties.
func benchmarkPropertyInsertStatement(tableName string) string {
	return fmt.Sprintf(
		"INSERT INTO %s (`event_id`,`tenant_id`,`project_id`,`source_id`,`source_type`,`event_name`,`distinct_id`,`session_id`,`visit_id`,`event_time`,`received_at`,`source`,`property_scope`,`property_name`,`property_type`,`string_value`,`number_value`,`bool_value`)",
		quoteIdent(tableName),
	)
}

// benchmarkEventID returns the deterministic event id used by benchmark fixtures.
func benchmarkEventID(idx int) string {
	return fmt.Sprintf("evt_%06d", idx)
}

// assertBenchmarkRealtimeRows verifies the low-pressure Realtime query shape.
func assertBenchmarkRealtimeRows(b *testing.B, records []storage.EventRecord, key clickhouse.RoutingKey, rowCount int, since time.Time, expectedEligibleRows int) {
	b.Helper()

	assertBenchmarkRealtimeEvidence(b, rowCount, since, expectedEligibleRows)

	// Realtime should return the latest 50 rows for the routed source. Checking
	// the newest event prevents the benchmark from silently timing an unfiltered
	// or incorrectly ordered query.
	assertBenchmarkRecordCount(b, records, 50)
	assertBenchmarkRecord(b, records[0], key, benchmarkEventID(rowCount-1), "", "", "", "", nil, nil)
	assertBenchmarkRecord(b, records[len(records)-1], key, benchmarkEventID(rowCount-50), "", "", "", "", nil, nil)
	for _, record := range records {
		// Returned rows must stay inside the scenario-specific Realtime window;
		// otherwise the narrow and wide benchmark shapes have collapsed.
		if record.EventTime.Before(since) {
			b.Fatalf("realtime row %s is before since %s", record.EventTime.Format(time.RFC3339), since.Format(time.RFC3339))
		}
	}
}

// assertBenchmarkRealtimeEvidence verifies Realtime scenario selectivity.
func assertBenchmarkRealtimeEvidence(tb testing.TB, rowCount int, since time.Time, expectedEligibleRows int) {
	tb.Helper()

	// The eligible-row count is intentionally asserted before timing and during
	// explain logging, so the benchmark cannot silently widen the recent-window
	// scenario while still returning the same latest 50 records.
	actualEligibleRows := benchmarkEligibleRows(rowCount, since)
	if actualEligibleRows != expectedEligibleRows {
		tb.Fatalf("realtime eligible rows = %d, want %d for since %s", actualEligibleRows, expectedEligibleRows, since.Format(time.RFC3339))
	}
	if expectedEligibleRows < 50 {
		tb.Fatalf("realtime scenario has only %d eligible rows, want at least 50", expectedEligibleRows)
	}
}

// assertBenchmarkEventWindowEvidence verifies Events scenario selectivity.
func assertBenchmarkEventWindowEvidence(tb testing.TB, rowCount int, from time.Time, to time.Time, expectedWindowRows int) {
	tb.Helper()

	// Events benchmark rows are split into recent and wide windows so later
	// optimization decisions can distinguish normal product filtering from broad
	// pressure scans. The assertion fails before timing or EXPLAIN if those
	// windows collapse into the same shape.
	actualWindowRows := benchmarkWindowRows(rowCount, from, to)
	if actualWindowRows != expectedWindowRows {
		tb.Fatalf("events window rows = %d, want %d for from %s to %s", actualWindowRows, expectedWindowRows, from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	if expectedWindowRows < benchmarkRecentEventsWindowRows {
		tb.Fatalf("events scenario has only %d eligible rows, want at least %d", expectedWindowRows, benchmarkRecentEventsWindowRows)
	}
}

// assertBenchmarkEventPlanEvidence verifies Events scenarios keep real time predicates.
func assertBenchmarkEventPlanEvidence(tb testing.TB, plan storage.EventQueryPlan, from time.Time, to time.Time) {
	tb.Helper()

	// Check both the storage-neutral evidence and the bound SQL arguments. The
	// evidence keeps optimization decisions observable, while the argument check
	// catches future drift where From/To remain present in the request but stop
	// crossing into the actual ClickHouse plan.
	evidence := plan.QueryEvidence()
	if evidence.Family != storage.EventQueryFamilyEvents {
		tb.Fatalf("events plan family = %q, want %q; evidence: %#v", evidence.Family, storage.EventQueryFamilyEvents, evidence)
	}
	if !evidence.HasTimeLowerBound || !evidence.HasTimeUpperBound {
		tb.Fatalf("events plan missing time bounds; evidence: %#v", evidence)
	}
	wantWindow := int64(to.UTC().Sub(from.UTC()) / time.Second)
	if evidence.TimeWindowSeconds != wantWindow {
		tb.Fatalf("events plan time window = %d, want %d; evidence: %#v", evidence.TimeWindowSeconds, wantWindow, evidence)
	}
	if !containsTimeArg(plan.Args, from.UTC()) || !containsTimeArg(plan.Args, to.UTC()) {
		tb.Fatalf("events plan args do not contain from/to bounds; from=%s to=%s args=%#v", from.UTC().Format(time.RFC3339), to.UTC().Format(time.RFC3339), plan.Args)
	}
}

// containsTimeArg reports whether args include the exact UTC time predicate.
func containsTimeArg(args []any, want time.Time) bool {
	for _, arg := range args {
		// GORM preserves time.Time arguments for the ClickHouse driver. Matching
		// on UTC equality keeps this helper independent of local timezone state.
		value, ok := arg.(time.Time)
		if ok && value.UTC().Equal(want.UTC()) {
			return true
		}
	}
	return false
}

// assertBenchmarkScalarRows verifies the medium-pressure scalar filter shape.
func assertBenchmarkScalarRows(b *testing.B, records []storage.EventRecord, key clickhouse.RoutingKey) {
	b.Helper()

	// The scalar scenario is intentionally constrained to page_view + visitor_2.
	// Every returned row must prove both predicates were honored.
	assertBenchmarkRecordCount(b, records, 50)
	for _, record := range records {
		assertBenchmarkRecord(b, record, key, "", "page_view", "visitor_2", "", "", nil, nil)
	}
}

// assertBenchmarkPropertyRows verifies the high-pressure property filter shape.
func assertBenchmarkPropertyRows(b *testing.B, records []storage.EventRecord, key clickhouse.RoutingKey) {
	b.Helper()

	// Property-filtered rows are seeded every 100 events. The full attribute and
	// property check keeps this benchmark aligned with EventReader semantics, not
	// merely with non-empty ClickHouse output.
	assertBenchmarkRecordCount(b, records, 50)
	for _, record := range records {
		assertBenchmarkRecord(
			b,
			record,
			key,
			"",
			"signup_clicked",
			"visitor_1",
			"session_1",
			"visit_1",
			[]string{`"button":"hero"`, `"plan":"pro"`},
			[]string{`"tier":"team"`},
		)
		if record.SourceType == "server" {
			b.Fatalf("source_type filter returned server row: %#v", record)
		}
	}
}

// assertBenchmarkRecordCount verifies benchmark scenarios keep timing equal workloads.
func assertBenchmarkRecordCount(b *testing.B, records []storage.EventRecord, want int) {
	b.Helper()

	if len(records) != want {
		b.Fatalf("benchmark record count = %d, want %d; records: %#v", len(records), want, records)
	}
}

// assertBenchmarkRecord verifies one EventReader result against routed fixture data.
func assertBenchmarkRecord(
	b *testing.B,
	record storage.EventRecord,
	key clickhouse.RoutingKey,
	eventID string,
	eventName string,
	distinctID string,
	sessionID string,
	visitID string,
	propertyFragments []string,
	userPropertyFragments []string,
) {
	b.Helper()

	if record.TenantID != key.TenantID || record.ProjectID != key.ProjectID || record.SourceID != key.SourceID {
		b.Fatalf("record routing key mismatch: got %#v, want %#v", record, key)
	}
	if eventID != "" && record.ID != eventID {
		b.Fatalf("record id = %q, want %q; record: %#v", record.ID, eventID, record)
	}
	if eventName != "" && record.EventName != eventName {
		b.Fatalf("record event_name = %q, want %q; record: %#v", record.EventName, eventName, record)
	}
	if distinctID != "" && record.DistinctID != distinctID {
		b.Fatalf("record distinct_id = %q, want %q; record: %#v", record.DistinctID, distinctID, record)
	}
	if sessionID != "" && record.SessionID != sessionID {
		b.Fatalf("record session_id = %q, want %q; record: %#v", record.SessionID, sessionID, record)
	}
	if visitID != "" && record.VisitID != visitID {
		b.Fatalf("record visit_id = %q, want %q; record: %#v", record.VisitID, visitID, record)
	}
	if !containsAll(record.Properties, propertyFragments) {
		b.Fatalf("record properties %q do not contain %v; record: %#v", record.Properties, propertyFragments, record)
	}
	if !containsAll(record.UserProperties, userPropertyFragments) {
		b.Fatalf("record user_properties %q do not contain %v; record: %#v", record.UserProperties, userPropertyFragments, record)
	}
}
