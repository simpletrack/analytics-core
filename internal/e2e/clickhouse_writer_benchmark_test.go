package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
	"gorm.io/gorm"
)

const clickHouseWriterBatchRowsEnv = "ANALYTICS_CORE_CLICKHOUSE_WRITER_BATCH_ROWS"

// BenchmarkBatchWriterClickHouseExecution measures the current event write hot path.
//
// The benchmark is opt-in because it requires the docker-compose ClickHouse
// service. It intentionally exercises BatchWriter.WriteEvent rather than a
// hand-written multi-row fixture batch so the result reflects the production
// storage.EventWriter boundary used by ingestion today.
func BenchmarkBatchWriterClickHouseExecution(b *testing.B) {
	if testing.Short() {
		b.Skip("real ClickHouse writer benchmark is skipped in short mode")
	}
	if !clickHouseBenchmarkEnabled() {
		b.Skipf("set %s=1 to run real ClickHouse BatchWriter benchmark", clickHouseBenchmarkEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancel()

	// Phase 1: prepare a dedicated routed table so benchmark runs do not depend
	// on shared fixture state or on tables left behind by earlier failures.
	clickConn := openBenchmarkClickHouseNative(ctx, b)
	defer clickConn.Close()
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
	defer dropBenchmarkTables(b, clickConn, table.Physical)

	writer, err := clickhouse.NewBatchWriter(clickConn, router)
	if err != nil {
		b.Fatalf("new clickhouse batch writer failed: %v", err)
	}

	// Phase 2: verify one write and one table read before timing starts. The
	// benchmark must fail fast if routing, DDL, or column order drifts.
	preflight := benchmarkWriteEnvelope(key, "preflight", benchmarkBaseTime())
	result, err := writer.WriteEvent(ctx, preflight)
	if err != nil {
		b.Fatalf("preflight write failed: %v", err)
	}
	if !result.Inserted {
		b.Fatal("preflight write should insert a fresh event")
	}
	assertBenchmarkEventCount(ctx, b, clickConn, table.Physical, 1)

	// Precompute timed input so ns/op and allocs/op describe the storage writer
	// path rather than benchmark fixture construction.
	envelopes := make([]contracts.EventEnvelope, b.N)
	for idx := 0; idx < b.N; idx++ {
		envelopes[idx] = benchmarkWriteEnvelope(key, fmt.Sprintf("bench_%06d", idx), benchmarkBaseTime().Add(time.Duration(idx)*time.Millisecond))
	}

	writeCtx, cancelWrites := context.WithTimeout(context.Background(), benchmarkWriteTimeout(b.N))
	defer cancelWrites()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		// Each precomputed envelope uses a unique id so a future idempotency
		// guard can be added without turning writes into duplicate no-ops.
		result, err := writer.WriteEvent(writeCtx, envelopes[idx])
		if err != nil {
			b.Fatalf("batch writer write failed: %v", err)
		}
		if !result.Inserted {
			b.Fatalf("batch writer unexpectedly skipped unique event %q", envelopes[idx].ID)
		}
	}
	b.StopTimer()

	// Phase 3: assert the table received every event that was timed. This keeps
	// the benchmark from timing a path that returns success without persistence.
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancelVerify()
	assertBenchmarkEventCount(verifyCtx, b, clickConn, table.Physical, uint64(b.N+1))
}

// BenchmarkGORMCreateInBatchesClickHouseExecution measures the ORM batch insert alternative.
//
// The benchmark uses the same routed table schema and event shape as
// BenchmarkBatchWriterClickHouseExecution, but writes through GORM
// CreateInBatches. It exists as a decision baseline only: the production event
// hot path remains the native clickhouse-go/v2 BatchWriter unless this
// benchmark proves GORM is close enough under the same workload.
func BenchmarkGORMCreateInBatchesClickHouseExecution(b *testing.B) {
	if testing.Short() {
		b.Skip("real ClickHouse GORM writer benchmark is skipped in short mode")
	}
	if !clickHouseBenchmarkEnabled() {
		b.Skipf("set %s=1 to run real ClickHouse GORM writer benchmark", clickHouseBenchmarkEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancel()

	// Phase 1: prepare the same routed table shape as the native writer
	// benchmark, then open GORM only for the timed insert path.
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
	defer dropBenchmarkTables(b, clickConn, table.Physical)

	// Phase 2: preflight one envelope through the same route + marshal + insert
	// helper used by timed iterations, so table names and tags are validated.
	preflight := benchmarkWriteEnvelope(key, "gorm_preflight", benchmarkBaseTime())
	if err := insertGORMBenchmarkEnvelope(ctx, clickGorm, router, preflight); err != nil {
		b.Fatalf("preflight GORM create failed: %v", err)
	}
	assertBenchmarkEventCount(ctx, b, clickConn, table.Physical, 1)

	// Precompute envelopes, not GORM rows. The timed loop must include routing
	// and JSON serialization because BatchWriter.WriteEvent includes that work.
	envelopes := make([]contracts.EventEnvelope, b.N)
	for idx := 0; idx < b.N; idx++ {
		envelopes[idx] = benchmarkWriteEnvelope(key, fmt.Sprintf("gorm_%06d", idx), benchmarkBaseTime().Add(time.Duration(idx)*time.Millisecond))
	}

	writeCtx, cancelWrites := context.WithTimeout(context.Background(), benchmarkWriteTimeout(b.N))
	defer cancelWrites()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		// Route and serialize inside the timer so this remains comparable with
		// BatchWriter.WriteEvent rather than with a pre-marshaled insert helper.
		if err := insertGORMBenchmarkEnvelope(writeCtx, clickGorm, router, envelopes[idx]); err != nil {
			b.Fatalf("GORM create in batches failed: %v", err)
		}
	}
	b.StopTimer()

	// Phase 3: assert persistence after timing so a successful GORM return value
	// cannot hide a driver or table-routing regression.
	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancelVerify()
	assertBenchmarkEventCount(verifyCtx, b, clickConn, table.Physical, uint64(b.N+1))
}

// BenchmarkNativePrepareBatchRowsClickHouseExecution measures native multi-row batch inserts.
//
// This benchmark bypasses BatchWriter.WriteEvent on purpose. It answers the
// separate decision question of raw clickhouse-go/v2 batch throughput for
// larger write groups under the same routed event schema.
func BenchmarkNativePrepareBatchRowsClickHouseExecution(b *testing.B) {
	if testing.Short() {
		b.Skip("real ClickHouse native bulk writer benchmark is skipped in short mode")
	}
	if !clickHouseBenchmarkEnabled() {
		b.Skipf("set %s=1 to run real ClickHouse native bulk writer benchmark", clickHouseBenchmarkEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancel()

	// Phase 1: create an isolated routed event table and keep the timed path
	// limited to PrepareBatch + Append + Send.
	clickConn := openBenchmarkClickHouseNative(ctx, b)
	defer clickConn.Close()
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
	defer dropBenchmarkTables(b, clickConn, table.Physical)

	batchRows := benchmarkWriterBatchRows()
	preflight := benchmarkGORMEventRows(b, key, "native_preflight", 0, batchRows)
	if err := insertNativeBenchmarkRows(ctx, clickConn, table.Physical, preflight); err != nil {
		b.Fatalf("preflight native bulk insert failed: %v", err)
	}
	assertBenchmarkEventCount(ctx, b, clickConn, table.Physical, uint64(batchRows))

	// Precompute all rows so timed iterations measure only the native insert path.
	rows := benchmarkGORMEventRows(b, key, "native_bulk", batchRows, b.N*batchRows)
	writeCtx, cancelWrites := context.WithTimeout(context.Background(), benchmarkBulkWriteTimeout(b.N, batchRows))
	defer cancelWrites()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		start := idx * batchRows
		end := start + batchRows
		if err := insertNativeBenchmarkRows(writeCtx, clickConn, table.Physical, rows[start:end]); err != nil {
			b.Fatalf("native bulk insert failed: %v", err)
		}
	}
	b.StopTimer()

	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancelVerify()
	assertBenchmarkEventCount(verifyCtx, b, clickConn, table.Physical, uint64((b.N+1)*batchRows))
}

// BenchmarkGORMCreateInBatchesRowsClickHouseExecution measures GORM multi-row batch inserts.
//
// It uses the same batch size, table routing, and row shape as the native bulk
// benchmark so the result can directly inform the event-write strategy tradeoff.
func BenchmarkGORMCreateInBatchesRowsClickHouseExecution(b *testing.B) {
	if testing.Short() {
		b.Skip("real ClickHouse GORM bulk writer benchmark is skipped in short mode")
	}
	if !clickHouseBenchmarkEnabled() {
		b.Skipf("set %s=1 to run real ClickHouse GORM bulk writer benchmark", clickHouseBenchmarkEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancel()

	// Phase 1: match the native bulk benchmark's fixture shape and only swap the
	// timed insert implementation to GORM CreateInBatches.
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
	defer dropBenchmarkTables(b, clickConn, table.Physical)

	batchRows := benchmarkWriterBatchRows()
	preflight := benchmarkGORMEventRows(b, key, "gorm_preflight_batch", 0, batchRows)
	if err := insertGORMBenchmarkRows(ctx, clickGorm, table.Physical, preflight, batchRows); err != nil {
		b.Fatalf("preflight GORM bulk insert failed: %v", err)
	}
	assertBenchmarkEventCount(ctx, b, clickConn, table.Physical, uint64(batchRows))

	rows := benchmarkGORMEventRows(b, key, "gorm_bulk", batchRows, b.N*batchRows)
	writeCtx, cancelWrites := context.WithTimeout(context.Background(), benchmarkBulkWriteTimeout(b.N, batchRows))
	defer cancelWrites()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		start := idx * batchRows
		end := start + batchRows
		if err := insertGORMBenchmarkRows(writeCtx, clickGorm, table.Physical, rows[start:end], batchRows); err != nil {
			b.Fatalf("GORM bulk insert failed: %v", err)
		}
	}
	b.StopTimer()

	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancelVerify()
	assertBenchmarkEventCount(verifyCtx, b, clickConn, table.Physical, uint64((b.N+1)*batchRows))
}

// clickHouseBenchmarkEnabled reports whether opt-in ClickHouse benchmarks may run.
func clickHouseBenchmarkEnabled() bool {
	return envOr(clickHouseBenchmarkEnabledEnv, "") == "1"
}

// benchmarkWriterBatchRows returns the row count used by bulk writer benchmarks.
func benchmarkWriterBatchRows() int {
	value := os.Getenv(clickHouseWriterBatchRowsEnv)
	if value == "" {
		return 100
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 100
	}
	return parsed
}

// benchmarkWriteEnvelope builds one deterministic event for writer benchmarks.
func benchmarkWriteEnvelope(key clickhouse.RoutingKey, suffix string, eventTime time.Time) contracts.EventEnvelope {
	return contracts.EventEnvelope{
		ID:         "evt_write_" + suffix,
		TenantID:   key.TenantID,
		ProjectID:  key.ProjectID,
		SourceID:   key.SourceID,
		SourceType: "web",
		EventName:  "page_view",
		DistinctID: "writer_visitor",
		SessionID:  "writer_session",
		VisitID:    "writer_visit",
		EventTime:  eventTime,
		ReceivedAt: eventTime.Add(10 * time.Millisecond),
		Properties: map[string]any{
			"path": "/writer-bench",
			"kind": "native-batch",
		},
		UserProps: map[string]any{
			"tier": "bench",
		},
		Source: "writer-benchmark",
	}
}

// benchmarkGORMEventRow is the GORM view of the routed ClickHouse event table.
type benchmarkGORMEventRow struct {
	EventID        string    `gorm:"column:event_id"`        // EventID is the stable event id used for benchmark row identity
	TenantID       string    `gorm:"column:tenant_id"`       // TenantID is the tenant boundary key
	ProjectID      string    `gorm:"column:project_id"`      // ProjectID is the project boundary key
	SourceID       string    `gorm:"column:source_id"`       // SourceID is the source boundary key
	SourceType     string    `gorm:"column:source_type"`     // SourceType is the source category
	EventName      string    `gorm:"column:event_name"`      // EventName is the analytics event name
	DistinctID     string    `gorm:"column:distinct_id"`     // DistinctID is the visitor or user key
	SessionID      string    `gorm:"column:session_id"`      // SessionID is the session key
	VisitID        string    `gorm:"column:visit_id"`        // VisitID is the canonical analytics visit key
	EventTime      time.Time `gorm:"column:event_time"`      // EventTime is the timestamp produced by the source
	ReceivedAt     time.Time `gorm:"column:received_at"`     // ReceivedAt is the timestamp accepted by collect
	Properties     string    `gorm:"column:properties"`      // Properties is serialized event properties
	UserProperties string    `gorm:"column:user_properties"` // UserProperties is serialized user properties
	Source         string    `gorm:"column:source"`          // Source is the optional diagnostic source label
}

// benchmarkGORMEventRowFromEnvelope converts the production event shape to GORM tags.
func benchmarkGORMEventRowFromEnvelope(tb testing.TB, envelope contracts.EventEnvelope) benchmarkGORMEventRow {
	tb.Helper()

	row, err := benchmarkGORMEventRowFromEnvelopeResult(envelope)
	if err != nil {
		tb.Fatalf("convert GORM benchmark row failed: %v", err)
	}
	return row
}

// benchmarkGORMEventRowFromEnvelopeResult converts envelope to a GORM row.
func benchmarkGORMEventRowFromEnvelopeResult(envelope contracts.EventEnvelope) (benchmarkGORMEventRow, error) {
	// Marshal the same property payload shape as BatchWriter so the ORM path
	// remains a fair comparison against the production event writer contract.
	properties, err := benchmarkMarshalJSON(envelope.Properties)
	if err != nil {
		return benchmarkGORMEventRow{}, fmt.Errorf("marshal properties: %w", err)
	}
	userProperties, err := benchmarkMarshalJSON(envelope.UserProps)
	if err != nil {
		return benchmarkGORMEventRow{}, fmt.Errorf("marshal user properties: %w", err)
	}
	return benchmarkGORMEventRow{
		EventID:        envelope.ID,
		TenantID:       envelope.TenantID,
		ProjectID:      envelope.ProjectID,
		SourceID:       envelope.SourceID,
		SourceType:     envelope.SourceType,
		EventName:      envelope.EventName,
		DistinctID:     envelope.DistinctID,
		SessionID:      envelope.SessionID,
		VisitID:        envelope.VisitID,
		EventTime:      envelope.EventTime.UTC(),
		ReceivedAt:     envelope.ReceivedAt.UTC(),
		Properties:     properties,
		UserProperties: userProperties,
		Source:         envelope.Source,
	}, nil
}

// benchmarkGORMEventRows builds deterministic rows for one benchmark table.
func benchmarkGORMEventRows(tb testing.TB, key clickhouse.RoutingKey, prefix string, offset int, count int) []benchmarkGORMEventRow {
	tb.Helper()

	rows := make([]benchmarkGORMEventRow, count)
	baseTime := benchmarkBaseTime()
	for idx := 0; idx < count; idx++ {
		eventIndex := offset + idx
		rows[idx] = benchmarkGORMEventRowFromEnvelope(
			tb,
			benchmarkWriteEnvelope(key, fmt.Sprintf("%s_%06d", prefix, eventIndex), baseTime.Add(time.Duration(eventIndex)*time.Millisecond)),
		)
	}
	return rows
}

// benchmarkMarshalJSON serializes benchmark property maps for ClickHouse string columns.
func benchmarkMarshalJSON(value map[string]any) (string, error) {
	if value == nil {
		return "{}", nil
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

// insertGORMBenchmarkRows writes rows through GORM CreateInBatches.
func insertGORMBenchmarkRows(ctx context.Context, db *gorm.DB, tableName string, rows []benchmarkGORMEventRow, batchSize int) error {
	// Table names still come from TableRouter and remain inside benchmark/storage
	// code; no product handler or service code is allowed to build them.
	return db.WithContext(ctx).Table(tableName).CreateInBatches(rows, batchSize).Error
}

// insertGORMBenchmarkEnvelope writes one envelope through the GORM writer shape.
func insertGORMBenchmarkEnvelope(ctx context.Context, db *gorm.DB, router *clickhouse.TableRouter, envelope contracts.EventEnvelope) error {
	// Keep the single-row GORM benchmark comparable with BatchWriter.WriteEvent:
	// both routes, serializes property maps, and sends one event inside timing.
	table, err := router.Route(envelope)
	if err != nil {
		return err
	}
	row, err := benchmarkGORMEventRowFromEnvelopeResult(envelope)
	if err != nil {
		return err
	}
	return insertGORMBenchmarkRows(ctx, db, table.Physical, []benchmarkGORMEventRow{row}, 1)
}

// insertNativeBenchmarkRows writes rows through clickhouse-go/v2 PrepareBatch.
func insertNativeBenchmarkRows(ctx context.Context, conn driver.Conn, tableName string, rows []benchmarkGORMEventRow) error {
	// Keep direct driver usage inside the benchmark/storage boundary so product
	// code still depends on storage.EventWriter instead of SQL strings.
	batch, err := conn.PrepareBatch(ctx, benchmarkEventInsertStatement(tableName))
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := batch.Append(
			row.EventID,
			row.TenantID,
			row.ProjectID,
			row.SourceID,
			row.SourceType,
			row.EventName,
			row.DistinctID,
			row.SessionID,
			row.VisitID,
			row.EventTime,
			row.ReceivedAt,
			row.Properties,
			row.UserProperties,
			row.Source,
		); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		return err
	}
	return nil
}

// benchmarkWriteTimeout returns a bounded write window for benchmark iterations.
func benchmarkWriteTimeout(iterations int) time.Duration {
	// Scale the timeout with b.N so manual long-benchtime runs remain bounded
	// without failing only because setup consumed the shared dependency deadline.
	timeout := e2eDependencyTimeout + time.Duration(iterations)*250*time.Millisecond
	if timeout < e2eDependencyTimeout {
		return e2eDependencyTimeout
	}
	return timeout
}

// benchmarkBulkWriteTimeout scales the timeout with row count for bulk insert comparisons.
func benchmarkBulkWriteTimeout(iterations int, batchRows int) time.Duration {
	timeout := e2eDependencyTimeout + time.Duration(iterations*batchRows)*25*time.Millisecond
	if timeout < e2eDependencyTimeout {
		return e2eDependencyTimeout
	}
	return timeout
}

// assertBenchmarkEventCount verifies the persisted row count in the routed table.
func assertBenchmarkEventCount(ctx context.Context, b *testing.B, conn driver.Conn, tableName string, want uint64) {
	b.Helper()

	query := fmt.Sprintf("SELECT count() FROM %s", quoteIdent(tableName))
	var got uint64
	if err := conn.QueryRow(ctx, query).Scan(&got); err != nil {
		b.Fatalf("query benchmark event count failed: %v", err)
	}
	if got != want {
		b.Fatalf("benchmark event count = %d, want %d", got, want)
	}
}
