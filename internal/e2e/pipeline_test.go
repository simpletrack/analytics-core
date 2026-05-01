package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/internal/collect"
	"github.com/simpletrack/analytics-core/internal/eventbus"
	"github.com/simpletrack/analytics-core/internal/eventbus/redisstream"
	"github.com/simpletrack/analytics-core/internal/ingestion"
	"github.com/simpletrack/analytics-core/internal/storage"
	"github.com/simpletrack/analytics-core/internal/storage/clickhouse"
	"github.com/simpletrack/analytics-core/internal/storage/mysql"
	gormclickhouse "gorm.io/driver/clickhouse"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const e2eEnabledEnv = "ANALYTICS_CORE_E2E"

func TestCollectToRealtimeAndEventsPipeline(t *testing.T) {
	if os.Getenv(e2eEnabledEnv) != "1" {
		t.Skipf("set %s=1 to run Redis/MySQL/ClickHouse end-to-end test", e2eEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Open the three runtime dependencies first so failures point to the
	// missing service before any schema or queue work starts.
	redisClient := openRedis(ctx, t)
	defer redisClient.Close()
	mysqlDB := openMySQL(t)
	clickConn := openClickHouseNative(ctx, t)
	defer clickConn.Close()
	clickGorm := openClickHouseGORM(t)

	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	key := uniqueRoutingKey()
	table, err := router.RouteKey(key)
	if err != nil {
		t.Fatalf("route test table failed: %v", err)
	}
	createEventTable(ctx, t, clickConn, table.Physical)

	// Build the production-shaped pipeline: collect publishes to Redis Stream,
	// Processor consumes through EventBus, and BatchWriter commits to ClickHouse
	// with MySQL-backed idempotency.
	stream := "analytics_core_e2e_" + key.SourceID
	bus, err := redisstream.New(redisClient, redisstream.Options{
		Stream:         stream,
		Block:          100 * time.Millisecond,
		Count:          1,
		EnsureConsumer: true,
	})
	if err != nil {
		t.Fatalf("new redis stream bus failed: %v", err)
	}
	handler, err := collect.NewHandler(bus, func() time.Time {
		return time.Date(2026, 5, 1, 8, 0, 1, 0, time.UTC)
	})
	if err != nil {
		t.Fatalf("new collect handler failed: %v", err)
	}
	guard, err := mysql.NewIngestionStatusGuard(mysqlDB)
	if err != nil {
		t.Fatalf("new ingestion status guard failed: %v", err)
	}
	if err := guard.AutoMigrate(ctx); err != nil {
		t.Fatalf("migrate ingestion status failed: %v", err)
	}
	writer, err := clickhouse.NewBatchWriter(clickConn, router, clickhouse.WithEventWriteGuard(guard))
	if err != nil {
		t.Fatalf("new clickhouse batch writer failed: %v", err)
	}
	processor, err := ingestion.NewProcessor(bus, eventbus.ConsumerGroup{
		Name:     "analytics-core-e2e",
		Consumer: "consumer-" + key.SourceID,
	}, writer)
	if err != nil {
		t.Fatalf("new ingestion processor failed: %v", err)
	}
	processorCtx, stopProcessor := context.WithCancel(ctx)
	defer stopProcessor()
	processorDone := runProcessor(processorCtx, t, processor)

	eventTime := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	pageview, err := handler.Handle(ctx, collect.Request{
		ID:         "evt_page_" + key.SourceID,
		TenantID:   key.TenantID,
		ProjectID:  key.ProjectID,
		SourceID:   key.SourceID,
		SourceType: "web",
		EventName:  "page_view",
		DistinctID: "visitor_" + key.SourceID,
		SessionID:  "session_" + key.SourceID,
		EventTime:  eventTime.Add(-30 * time.Second),
		Properties: map[string]any{"path": "/"},
		Source:     "e2e-test",
	})
	if err != nil {
		t.Fatalf("collect pageview failed: %v", err)
	}
	customEvent, err := handler.Handle(ctx, collect.Request{
		ID:         "evt_custom_" + key.SourceID,
		TenantID:   key.TenantID,
		ProjectID:  key.ProjectID,
		SourceID:   key.SourceID,
		SourceType: "web",
		EventName:  "signup_clicked",
		DistinctID: "visitor_" + key.SourceID,
		SessionID:  "session_" + key.SourceID,
		EventTime:  eventTime,
		Properties: map[string]any{"path": "/signup", "button": "hero"},
		UserProps:  map[string]any{"plan": "free"},
		Source:     "e2e-test",
	})
	if err != nil {
		t.Fatalf("collect custom event failed: %v", err)
	}

	builder, err := clickhouse.NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := clickhouse.NewEventReader(clickGorm, builder)
	if err != nil {
		t.Fatalf("new event reader failed: %v", err)
	}

	// Poll through the public reader boundary. The test should not peek into the
	// Redis message or ClickHouse table directly once the pipeline starts.
	pageviews := waitForEvents(ctx, t, reader, storage.EventListQuery{
		TenantID:  key.TenantID,
		ProjectID: key.ProjectID,
		SourceID:  key.SourceID,
		EventName: "page_view",
		From:      eventTime.Add(-time.Minute),
		To:        eventTime.Add(time.Minute),
		Limit:     10,
	})
	events := waitForEvents(ctx, t, reader, storage.EventListQuery{
		TenantID:  key.TenantID,
		ProjectID: key.ProjectID,
		SourceID:  key.SourceID,
		EventName: "signup_clicked",
		From:      eventTime.Add(-time.Minute),
		To:        eventTime.Add(time.Minute),
		Limit:     10,
	})
	realtime := waitForRealtime(ctx, t, reader, storage.RealtimeQuery{
		TenantID:  key.TenantID,
		ProjectID: key.ProjectID,
		SourceID:  key.SourceID,
		Since:     eventTime.Add(-time.Minute),
		Limit:     10,
	})

	assertEventRecord(t, pageviews, pageview.ID, []string{`"path":"/"`}, nil)
	assertEventRecord(t, events, customEvent.ID, []string{`"button":"hero"`}, []string{`"plan":"free"`})
	assertEventRecord(t, realtime, pageview.ID, []string{`"path":"/"`}, nil)
	assertEventRecord(t, realtime, customEvent.ID, []string{`"button":"hero"`}, []string{`"plan":"free"`})

	stopProcessor()
	if err := <-processorDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("processor stopped with unexpected error: %v", err)
	}
}

func openRedis(ctx context.Context, t *testing.T) *redis.Client {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: envOr("ANALYTICS_CORE_REDIS_ADDR", "127.0.0.1:26379"),
	})
	// Ping before returning so failures are reported at the Redis boundary, not
	// later as a publish or subscribe timeout.
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("ping redis failed: %v", err)
	}
	return client
}

func openMySQL(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := envOr("ANALYTICS_CORE_MYSQL_DSN", "analytics_core:analytics_core@tcp(127.0.0.1:23306)/analytics_core?parseTime=true")
	db, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open mysql failed: %v", err)
	}
	return db
}

func openClickHouseNative(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()

	conn, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{envOr("ANALYTICS_CORE_CLICKHOUSE_NATIVE_ADDR", "127.0.0.1:29000")},
		Auth: clickhousedriver.Auth{
			Database: envOr("ANALYTICS_CORE_CLICKHOUSE_DATABASE", "analytics_core"),
			Username: envOr("ANALYTICS_CORE_CLICKHOUSE_USER", "analytics_core"),
			Password: envOr("ANALYTICS_CORE_CLICKHOUSE_PASSWORD", "analytics_core"),
		},
	})
	if err != nil {
		t.Fatalf("open native clickhouse failed: %v", err)
	}
	// Ping verifies the native write path before schema setup and BatchWriter
	// attempt to prepare native batches.
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		t.Fatalf("ping native clickhouse failed: %v", err)
	}
	return conn
}

func openClickHouseGORM(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := envOr("ANALYTICS_CORE_CLICKHOUSE_GORM_DSN", defaultClickHouseGORMDSN())
	db, err := gorm.Open(gormclickhouse.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm clickhouse failed: %v", err)
	}
	return db
}

func createEventTable(ctx context.Context, t *testing.T, conn driver.Conn, tableName string) {
	t.Helper()

	// The table name is generated by TableRouter, so it is a hashed safe
	// identifier. Keep the DDL local to the integration test until migrations
	// become a first-class P1 deliverable.
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	event_id String,
	tenant_id String,
	project_id String,
	source_id String,
	source_type String,
	event_name String,
	distinct_id String,
	session_id String,
	event_time DateTime64(3, 'UTC'),
	received_at DateTime64(3, 'UTC'),
	properties String,
	user_properties String,
	source String
) ENGINE = MergeTree
ORDER BY (tenant_id, project_id, source_id, event_time, event_id)
`, quoteIdent(tableName))
	if err := conn.Exec(ctx, ddl); err != nil {
		t.Fatalf("create clickhouse event table failed: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Exec(dropCtx, "DROP TABLE IF EXISTS "+quoteIdent(tableName))
	})
}

func runProcessor(ctx context.Context, t *testing.T, processor *ingestion.Processor) <-chan error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- processor.Run(ctx)
	}()
	return done
}

func waitForEvents(ctx context.Context, t *testing.T, reader storage.EventReader, query storage.EventListQuery) []storage.EventRecord {
	t.Helper()

	var records []storage.EventRecord
	waitForCondition(ctx, t, func() (bool, error) {
		var err error
		records, err = reader.ListEvents(ctx, query)
		return len(records) > 0, err
	})
	return records
}

func waitForRealtime(ctx context.Context, t *testing.T, reader storage.EventReader, query storage.RealtimeQuery) []storage.EventRecord {
	t.Helper()

	var records []storage.EventRecord
	waitForCondition(ctx, t, func() (bool, error) {
		var err error
		records, err = reader.ListRealtime(ctx, query)
		return len(records) > 0, err
	})
	return records
}

func waitForCondition(ctx context.Context, t *testing.T, check func() (bool, error)) {
	t.Helper()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		// Check before sleeping so fast local runs do not pay an avoidable delay.
		ok, err := check()
		if err != nil {
			lastErr = err
		}
		if ok {
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf("condition did not become true before timeout; last error: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func assertEventRecord(t *testing.T, records []storage.EventRecord, eventID string, propertyFragments []string, userPropertyFragments []string) {
	t.Helper()

	for _, record := range records {
		if record.ID == eventID && containsAll(record.Properties, propertyFragments) && containsAll(record.UserProperties, userPropertyFragments) {
			return
		}
	}
	t.Fatalf("event %q with expected properties not found in records: %#v", eventID, records)
}

func containsAll(value string, fragments []string) bool {
	for _, fragment := range fragments {
		if !strings.Contains(value, fragment) {
			return false
		}
	}
	return true
}

func uniqueRoutingKey() clickhouse.RoutingKey {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	return clickhouse.RoutingKey{
		TenantID:  "tenant_" + suffix,
		ProjectID: "project_" + suffix,
		SourceID:  "source_" + suffix,
	}
}

func defaultClickHouseGORMDSN() string {
	addr := envOr("ANALYTICS_CORE_CLICKHOUSE_NATIVE_ADDR", "127.0.0.1:29000")
	database := envOr("ANALYTICS_CORE_CLICKHOUSE_DATABASE", "analytics_core")
	user := envOr("ANALYTICS_CORE_CLICKHOUSE_USER", "analytics_core")
	password := envOr("ANALYTICS_CORE_CLICKHOUSE_PASSWORD", "analytics_core")
	return fmt.Sprintf("clickhouse://%s:%s@%s/%s?dial_timeout=10s&read_timeout=20s", user, password, addr, database)
}

func envOr(name string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func quoteIdent(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
