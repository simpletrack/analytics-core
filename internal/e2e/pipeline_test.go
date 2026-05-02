package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/collect"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/eventbus/redisstream"
	"github.com/simpletrack/analytics-core/ingestion"
	"github.com/simpletrack/analytics-core/storage"
	"github.com/simpletrack/analytics-core/storage/clickhouse"
	"github.com/simpletrack/analytics-core/storage/mysql"
	gormclickhouse "gorm.io/driver/clickhouse"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const e2eEnabledEnv = "ANALYTICS_CORE_E2E"
const e2eDependencyTimeout = 90 * time.Second
const e2eDependencyPollInterval = 500 * time.Millisecond

func TestCollectToRealtimeAndEventsPipeline(t *testing.T) {
	if os.Getenv(e2eEnabledEnv) != "1" {
		t.Skipf("set %s=1 to run Redis/MySQL/ClickHouse end-to-end test", e2eEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancel()

	// Open the three runtime dependencies first so failures point to the
	// missing service before any schema or queue work starts.
	redisClient := openRedis(ctx, t)
	defer redisClient.Close()
	mysqlDB := openMySQL(ctx, t)
	clickConn := openClickHouseNative(ctx, t)
	defer clickConn.Close()
	clickGorm := openClickHouseGORM(ctx, t)

	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	key := uniqueRoutingKey()
	table, err := router.RouteKey(key)
	if err != nil {
		t.Fatalf("route test table failed: %v", err)
	}
	propertyTableName := table.Physical + "_properties"
	createEventTable(ctx, t, clickConn, table.Physical)
	createPropertyTable(ctx, t, clickConn, propertyTableName)

	// Build the production-shaped pipeline: collect publishes to Redis Stream,
	// Processor consumes through EventBus, the primary BatchWriter commits the
	// event row with MySQL-backed idempotency, and the storage decorator writes
	// typed property rows after the event exists.
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
	propertyGuard, err := mysql.NewPropertyIndexingStatusGuard(mysqlDB)
	if err != nil {
		t.Fatalf("new property indexing status guard failed: %v", err)
	}
	if err := propertyGuard.AutoMigrate(ctx); err != nil {
		t.Fatalf("migrate property indexing status failed: %v", err)
	}
	eventWriter, err := clickhouse.NewBatchWriter(clickConn, router, clickhouse.WithEventWriteGuard(guard))
	if err != nil {
		t.Fatalf("new clickhouse batch writer failed: %v", err)
	}
	propertyWriter, err := clickhouse.NewPropertyBatchWriter(clickConn, router)
	if err != nil {
		t.Fatalf("new clickhouse property writer failed: %v", err)
	}
	writer, err := storage.NewPropertyIndexingEventWriter(eventWriter, propertyWriter, propertyGuard)
	if err != nil {
		t.Fatalf("new property indexing event writer failed: %v", err)
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

	builder, err := clickhouse.NewEventQueryBuilder(router, clickhouse.WithAllowedPropertyFilters(
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"},
		storage.PropertySelector{Scope: storage.PropertyScopeUser, Name: "plan"},
	))
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
	filtered := waitForEvents(ctx, t, reader, storage.EventListQuery{
		TenantID:  key.TenantID,
		ProjectID: key.ProjectID,
		SourceID:  key.SourceID,
		From:      eventTime.Add(-time.Minute),
		To:        eventTime.Add(time.Minute),
		Limit:     10,
		PropertyFilters: []storage.EventPropertyFilter{
			{
				Scope:       storage.PropertyScopeEvent,
				Name:        "button",
				ValueType:   storage.PropertyValueString,
				Operator:    storage.EventFilterEquals,
				StringValue: "hero",
			},
			{
				Scope:       storage.PropertyScopeUser,
				Name:        "plan",
				ValueType:   storage.PropertyValueString,
				Operator:    storage.EventFilterEquals,
				StringValue: "free",
			},
		},
	})
	assertOnlyEventIDs(t, filtered, customEvent.ID)
	assertEventRecord(t, filtered, customEvent.ID, []string{`"button":"hero"`}, []string{`"plan":"free"`})

	stopProcessor()
	if err := <-processorDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("processor stopped with unexpected error: %v", err)
	}
}

func TestPropertyBatchWriterToClickHouse(t *testing.T) {
	if os.Getenv(e2eEnabledEnv) != "1" {
		t.Skipf("set %s=1 to run ClickHouse property writer end-to-end test", e2eEnabledEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e2eDependencyTimeout)
	defer cancel()

	clickConn := openClickHouseNative(ctx, t)
	defer clickConn.Close()
	clickGorm := openClickHouseGORM(ctx, t)

	router, err := clickhouse.NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	key := uniqueRoutingKey()
	table, err := router.RouteKey(key)
	if err != nil {
		t.Fatalf("route test table failed: %v", err)
	}
	propertyTableName := table.Physical + "_properties"
	createEventTable(ctx, t, clickConn, table.Physical)
	createPropertyTable(ctx, t, clickConn, propertyTableName)

	writer, err := clickhouse.NewPropertyBatchWriter(clickConn, router)
	if err != nil {
		t.Fatalf("new clickhouse property writer failed: %v", err)
	}

	eventTime := time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC)
	envelope := contracts.EventEnvelope{
		ID:         "evt_property_" + key.SourceID,
		TenantID:   key.TenantID,
		ProjectID:  key.ProjectID,
		SourceID:   key.SourceID,
		SourceType: "web",
		EventName:  "signup_clicked",
		DistinctID: "visitor_" + key.SourceID,
		SessionID:  "session_" + key.SourceID,
		EventTime:  eventTime,
		ReceivedAt: eventTime.Add(time.Second),
		Properties: map[string]any{"button": "hero", "score": 42.5},
		UserProps:  map[string]any{"beta": true, "plan": "free"},
		Source:     "e2e-test",
	}
	eventWriter, err := clickhouse.NewBatchWriter(clickConn, router)
	if err != nil {
		t.Fatalf("new clickhouse event writer failed: %v", err)
	}
	eventResult, err := eventWriter.WriteEvent(ctx, envelope)
	if err != nil {
		t.Fatalf("write event failed: %v", err)
	}
	if !eventResult.Inserted {
		t.Fatal("property e2e event write should insert a fresh event")
	}
	propertyRecords, err := storage.FlattenEventProperties(envelope)
	if err != nil {
		t.Fatalf("flatten event properties failed: %v", err)
	}
	result, err := writer.WriteEventProperties(ctx, propertyRecords)
	if err != nil {
		t.Fatalf("write event properties failed: %v", err)
	}
	if result.Rows != len(propertyRecords) {
		t.Fatalf("property rows = %d, want %d", result.Rows, len(propertyRecords))
	}
	otherEnvelope := envelope
	otherEnvelope.ID = "evt_property_other_" + key.SourceID
	otherEnvelope.EventName = "checkout_clicked"
	otherEnvelope.Properties = map[string]any{"button": "footer", "score": 13}
	otherEnvelope.UserProps = map[string]any{"beta": false, "plan": "paid"}
	otherEventResult, err := eventWriter.WriteEvent(ctx, otherEnvelope)
	if err != nil {
		t.Fatalf("write nonmatching event failed: %v", err)
	}
	if !otherEventResult.Inserted {
		t.Fatal("nonmatching property e2e event write should insert a fresh event")
	}
	otherPropertyRecords, err := storage.FlattenEventProperties(otherEnvelope)
	if err != nil {
		t.Fatalf("flatten nonmatching event properties failed: %v", err)
	}
	otherResult, err := writer.WriteEventProperties(ctx, otherPropertyRecords)
	if err != nil {
		t.Fatalf("write nonmatching event properties failed: %v", err)
	}
	if otherResult.Rows != len(otherPropertyRecords) {
		t.Fatalf("nonmatching property rows = %d, want %d", otherResult.Rows, len(otherPropertyRecords))
	}

	var rows []propertyResultRow
	waitForCondition(ctx, t, func() (bool, error) {
		var err error
		rows, err = queryPropertyRows(ctx, clickConn, propertyTableName, envelope.ID)
		return len(rows) == len(propertyRecords), err
	})

	assertPropertyRow(t, rows, storage.PropertyScopeEvent, "button", storage.PropertyValueString, "hero", 0, false)
	assertPropertyRow(t, rows, storage.PropertyScopeEvent, "score", storage.PropertyValueNumber, "", 42.5, false)
	assertPropertyRow(t, rows, storage.PropertyScopeUser, "beta", storage.PropertyValueBool, "", 0, true)
	assertPropertyRow(t, rows, storage.PropertyScopeUser, "plan", storage.PropertyValueString, "free", 0, false)

	builder, err := clickhouse.NewEventQueryBuilder(router, clickhouse.WithAllowedPropertyFilters(
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"},
		storage.PropertySelector{Scope: storage.PropertyScopeUser, Name: "plan"},
	))
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := clickhouse.NewEventReader(clickGorm, builder)
	if err != nil {
		t.Fatalf("new event reader failed: %v", err)
	}
	filtered := waitForEvents(ctx, t, reader, storage.EventListQuery{
		TenantID:  key.TenantID,
		ProjectID: key.ProjectID,
		SourceID:  key.SourceID,
		From:      eventTime.Add(-time.Minute),
		To:        eventTime.Add(time.Minute),
		Limit:     10,
		PropertyFilters: []storage.EventPropertyFilter{
			{
				Scope:       storage.PropertyScopeEvent,
				Name:        "button",
				ValueType:   storage.PropertyValueString,
				Operator:    storage.EventFilterEquals,
				StringValue: "hero",
			},
			{
				Scope:       storage.PropertyScopeUser,
				Name:        "plan",
				ValueType:   storage.PropertyValueString,
				Operator:    storage.EventFilterEquals,
				StringValue: "free",
			},
		},
	})
	assertOnlyEventIDs(t, filtered, envelope.ID)
	assertEventRecord(t, filtered, envelope.ID, []string{`"button":"hero"`}, []string{`"plan":"free"`})
}

func openRedis(ctx context.Context, t *testing.T) *redis.Client {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: envOr("ANALYTICS_CORE_REDIS_ADDR", "127.0.0.1:26379"),
	})
	ticker := time.NewTicker(e2eDependencyPollInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		// Ping before returning so failures are reported at the Redis boundary,
		// not later as a publish or subscribe timeout.
		err := client.Ping(ctx).Err()
		if err == nil {
			return client
		}
		lastErr = err

		select {
		case <-ctx.Done():
			_ = client.Close()
			t.Fatalf("redis did not become ready before timeout: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func openMySQL(ctx context.Context, t *testing.T) *gorm.DB {
	t.Helper()

	dsn := envOr("ANALYTICS_CORE_MYSQL_DSN", "analytics_core:analytics_core@tcp(127.0.0.1:23306)/analytics_core?parseTime=true")
	ticker := time.NewTicker(e2eDependencyPollInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		// Probe readiness through database/sql first. GORM's MySQL dialer logs
		// startup EOFs directly, which makes cold-start e2e output noisy before
		// the server is actually ready to authenticate.
		sqlDB, err := sql.Open("mysql", dsn)
		if err != nil {
			lastErr = err
		} else {
			pingErr := sqlDB.PingContext(ctx)
			_ = sqlDB.Close()
			if pingErr == nil {
				// Build the production GORM handle only after the dependency is
				// reachable so the test still exercises the real MySQL adapter.
				db, gormErr := gorm.Open(gormmysql.Open(dsn), &gorm.Config{})
				if gormErr == nil {
					return db
				}
				lastErr = gormErr
			} else {
				lastErr = pingErr
			}
		}

		// Keep retrying until the shared e2e deadline expires so Docker cold
		// starts and slow health transitions do not fail at the first handshake.
		select {
		case <-ctx.Done():
			t.Fatalf("mysql did not become ready before timeout: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func openClickHouseNative(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()

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
		} else {
			// Ping verifies the native write path before schema setup and
			// BatchWriter attempt to prepare native batches. ClickHouse can
			// accept a TCP connection before the server is ready to complete the
			// native handshake, so startup tests retry EOF/handshake failures.
			pingErr := conn.Ping(ctx)
			if pingErr == nil {
				return conn
			}
			lastErr = pingErr
			_ = conn.Close()
		}

		select {
		case <-ctx.Done():
			t.Fatalf("native clickhouse did not become ready before timeout: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func openClickHouseGORM(ctx context.Context, t *testing.T) *gorm.DB {
	t.Helper()

	dsn := envOr("ANALYTICS_CORE_CLICKHOUSE_GORM_DSN", defaultClickHouseGORMDSN())
	ticker := time.NewTicker(e2eDependencyPollInterval)
	defer ticker.Stop()
	var lastErr error
	for {
		db, err := gorm.Open(gormclickhouse.Open(dsn), &gorm.Config{})
		if err != nil {
			lastErr = err
		} else {
			sqlDB, sqlErr := db.DB()
			if sqlErr != nil {
				lastErr = sqlErr
			} else {
				pingErr := sqlDB.PingContext(ctx)
				if pingErr == nil {
					return db
				}
				lastErr = pingErr
				_ = sqlDB.Close()
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("gorm clickhouse did not become ready before timeout: %v", lastErr)
		case <-ticker.C:
		}
	}
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

func createPropertyTable(ctx context.Context, t *testing.T, conn driver.Conn, tableName string) {
	t.Helper()

	// The property table mirrors storage.EventPropertyRecord. It is created
	// beside each routed event table so the e2e can prove both hot-path property
	// indexing and the standalone property writer.
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
	source String,
	property_scope String,
	property_name String,
	property_type String,
	string_value String,
	number_value Float64,
	bool_value Bool
) ENGINE = MergeTree
ORDER BY (tenant_id, project_id, source_id, property_scope, property_name, event_time, event_id)
`, quoteIdent(tableName))
	if err := conn.Exec(ctx, ddl); err != nil {
		t.Fatalf("create clickhouse property table failed: %v", err)
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

type propertyResultRow struct {
	Scope       string  // Scope is the persisted property scope
	Name        string  // Name is the persisted property key
	ValueType   string  // ValueType is the persisted scalar type
	StringValue string  // StringValue is the persisted string value
	NumberValue float64 // NumberValue is the persisted numeric value
	BoolValue   bool    // BoolValue is the persisted boolean value
}

func queryPropertyRows(ctx context.Context, conn driver.Conn, tableName string, eventID string) ([]propertyResultRow, error) {
	query := fmt.Sprintf(`
SELECT property_scope, property_name, property_type, string_value, number_value, bool_value
FROM %s
WHERE event_id = ?
ORDER BY property_scope, property_name
`, quoteIdent(tableName))
	rows, err := conn.Query(ctx, query, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []propertyResultRow
	for rows.Next() {
		var row propertyResultRow
		if err := rows.Scan(&row.Scope, &row.Name, &row.ValueType, &row.StringValue, &row.NumberValue, &row.BoolValue); err != nil {
			return nil, err
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func assertPropertyRow(t *testing.T, rows []propertyResultRow, scope storage.PropertyScope, name string, valueType storage.PropertyValueType, stringValue string, numberValue float64, boolValue bool) {
	t.Helper()

	for _, row := range rows {
		if row.Scope == string(scope) && row.Name == name && row.ValueType == string(valueType) &&
			row.StringValue == stringValue && row.NumberValue == numberValue && row.BoolValue == boolValue {
			return
		}
	}
	t.Fatalf("property %s.%s=%s/%q/%f/%t not found in rows: %#v", scope, name, valueType, stringValue, numberValue, boolValue, rows)
}

func assertOnlyEventIDs(t *testing.T, records []storage.EventRecord, ids ...string) {
	t.Helper()

	// Exact id matching prevents filter tests from passing when a query ignores
	// its filter and merely includes the expected event among extra rows.
	if len(records) != len(ids) {
		t.Fatalf("event count = %d, want %d; records: %#v", len(records), len(ids), records)
	}
	want := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		want[id] = struct{}{}
	}
	for _, record := range records {
		if _, ok := want[record.ID]; !ok {
			t.Fatalf("unexpected event %q in records: %#v", record.ID, records)
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
