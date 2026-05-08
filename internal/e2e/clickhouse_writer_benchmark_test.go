package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
)

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

// clickHouseBenchmarkEnabled reports whether opt-in ClickHouse benchmarks may run.
func clickHouseBenchmarkEnabled() bool {
	return envOr(clickHouseBenchmarkEnabledEnv, "") == "1"
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
