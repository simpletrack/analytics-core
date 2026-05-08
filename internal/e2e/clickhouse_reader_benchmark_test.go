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

// BenchmarkEventReaderClickHouseExecution measures EventReader latency against
// a real local ClickHouse instance and a seeded routed event table.
//
// The benchmark is opt-in because it depends on docker-compose ClickHouse. It
// complements the builder-only benchmark by measuring actual GORM Raw execution
// and ClickHouse read latency for the same low, medium, and high query shapes.
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
	scenarios := []struct {
		name  string
		read  func(context.Context) ([]storage.EventRecord, error)
		check func(*testing.B, []storage.EventRecord)
	}{
		{
			name: "low_realtime",
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListRealtime(ctx, storage.RealtimeQuery{
					TenantID:  key.TenantID,
					ProjectID: key.ProjectID,
					SourceID:  key.SourceID,
					Since:     baseTime.Add(-time.Minute),
					Limit:     50,
				})
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkRealtimeRows(b, records, key, rowCount)
			},
		},
		{
			name: "medium_events_scalar",
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListEvents(ctx, storage.EventListQuery{
					TenantID:   key.TenantID,
					ProjectID:  key.ProjectID,
					SourceID:   key.SourceID,
					EventName:  "page_view",
					DistinctID: "visitor_2",
					From:       baseTime.Add(-time.Minute),
					To:         baseTime.Add(time.Duration(rowCount+1) * time.Second),
					Limit:      50,
				})
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkScalarRows(b, records, key)
			},
		},
		{
			name: "high_events_property",
			read: func(ctx context.Context) ([]storage.EventRecord, error) {
				return reader.ListEvents(ctx, storage.EventListQuery{
					TenantID:      key.TenantID,
					ProjectID:     key.ProjectID,
					SourceID:      key.SourceID,
					EventName:     "signup_clicked",
					DistinctID:    "visitor_1",
					From:          baseTime.Add(-time.Minute),
					To:            baseTime.Add(time.Duration(rowCount+1) * time.Second),
					SortField:     storage.EventSortByReceivedAt,
					SortDirection: storage.EventSortDescending,
					Limit:         50,
					Filters: []storage.EventFilter{
						{Field: storage.EventFilterBySessionID, Operator: storage.EventFilterEquals, Value: "session_1"},
						{Field: storage.EventFilterByVisitID, Operator: storage.EventFilterEquals, Value: "visit_1"},
						{Field: storage.EventFilterBySourceType, Operator: storage.EventFilterNotEquals, Value: "server"},
					},
					PropertyFilters: []storage.EventPropertyFilter{
						{Scope: storage.PropertyScopeEvent, Name: "button", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "hero"},
						{Scope: storage.PropertyScopeEvent, Name: "plan", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "pro"},
						{Scope: storage.PropertyScopeUser, Name: "tier", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "team"},
					},
				})
			},
			check: func(b *testing.B, records []storage.EventRecord) {
				assertBenchmarkPropertyRows(b, records, key)
			},
		},
	}

	// Phase 3: verify each scenario once, then time only EventReader execution.
	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
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
	scenarios := []struct {
		name string
		plan func(context.Context) (storage.EventQueryPlan, error)
	}{
		{
			name: "low_realtime",
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildRealtimeQuery(ctx, storage.RealtimeQuery{
					TenantID:  key.TenantID,
					ProjectID: key.ProjectID,
					SourceID:  key.SourceID,
					Since:     baseTime.Add(-time.Minute),
					Limit:     50,
				})
			},
		},
		{
			name: "medium_events_scalar",
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, storage.EventListQuery{
					TenantID:   key.TenantID,
					ProjectID:  key.ProjectID,
					SourceID:   key.SourceID,
					EventName:  "page_view",
					DistinctID: "visitor_2",
					From:       baseTime.Add(-time.Minute),
					To:         baseTime.Add(time.Duration(rowCount+1) * time.Second),
					Limit:      50,
				})
			},
		},
		{
			name: "high_events_property",
			plan: func(ctx context.Context) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, storage.EventListQuery{
					TenantID:      key.TenantID,
					ProjectID:     key.ProjectID,
					SourceID:      key.SourceID,
					EventName:     "signup_clicked",
					DistinctID:    "visitor_1",
					From:          baseTime.Add(-time.Minute),
					To:            baseTime.Add(time.Duration(rowCount+1) * time.Second),
					SortField:     storage.EventSortByReceivedAt,
					SortDirection: storage.EventSortDescending,
					Limit:         50,
					Filters: []storage.EventFilter{
						{Field: storage.EventFilterBySessionID, Operator: storage.EventFilterEquals, Value: "session_1"},
						{Field: storage.EventFilterByVisitID, Operator: storage.EventFilterEquals, Value: "visit_1"},
						{Field: storage.EventFilterBySourceType, Operator: storage.EventFilterNotEquals, Value: "server"},
					},
					PropertyFilters: []storage.EventPropertyFilter{
						{Scope: storage.PropertyScopeEvent, Name: "button", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "hero"},
						{Scope: storage.PropertyScopeEvent, Name: "plan", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "pro"},
						{Scope: storage.PropertyScopeUser, Name: "tier", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "team"},
					},
				})
			},
		},
	}

	// Phase 3: log structured evidence and ClickHouse's index-aware explain
	// output. The test fails if any scenario cannot be planned or explained.
	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			plan, err := scenario.plan(ctx)
			if err != nil {
				t.Fatalf("build query plan failed: %v", err)
			}
			t.Logf("query evidence: %+v", plan.QueryEvidence())
			for _, line := range explainPlan(ctx, t, clickConn, plan) {
				t.Logf("explain: %s", line)
			}
		})
	}
}

func benchmarkRowCount() int {
	// Keep the default dataset local-friendly while allowing larger manual
	// pressure runs without changing source code.
	value := strings.TrimSpace(os.Getenv(clickHouseBenchmarkRowsEnv))
	if value == "" {
		return 10000
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1000 {
		return 10000
	}
	return parsed
}

// benchmarkBaseTime returns a stable timestamp anchor for seeded rows.
func benchmarkBaseTime() time.Time {
	return time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
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
func assertBenchmarkRealtimeRows(b *testing.B, records []storage.EventRecord, key clickhouse.RoutingKey, rowCount int) {
	b.Helper()

	// Realtime should return the latest 50 rows for the routed source. Checking
	// the newest event prevents the benchmark from silently timing an unfiltered
	// or incorrectly ordered query.
	assertBenchmarkRecordCount(b, records, 50)
	assertBenchmarkRecord(b, records[0], key, benchmarkEventID(rowCount-1), "", "", "", "", nil, nil)
	assertBenchmarkRecord(b, records[len(records)-1], key, benchmarkEventID(rowCount-50), "", "", "", "", nil, nil)
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
