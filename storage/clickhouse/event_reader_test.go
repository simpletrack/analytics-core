package clickhouse

import (
	"context"
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/simpletrack/analytics-core/storage"
	gormclickhouse "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func TestEventReaderListEventsExecutesQueryPlan(t *testing.T) {
	db, mock, cleanup := newMockClickHouseDB(t)
	defer cleanup()

	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := NewEventReader(db, builder)
	if err != nil {
		t.Fatalf("new event reader failed: %v", err)
	}

	query := storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Limit:     10,
	}
	plan, err := builder.BuildEventsQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("build expected plan failed: %v", err)
	}

	eventTime := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	receivedAt := eventTime.Add(time.Second)
	mock.ExpectQuery(regexp.QuoteMeta(plan.SQL)).
		WithArgs(driverArgs(plan.Args)...).
		WillReturnRows(newEventRows().AddRow(
			"evt_1",
			"tenant_1",
			"project_1",
			"source_1",
			"web",
			"page_view",
			"visitor_1",
			"session_1",
			"visit_1",
			eventTime,
			receivedAt,
			`{"path":"/"}`,
			`{"plan":"free"}`,
			"browser",
		))

	records, err := reader.ListEvents(context.Background(), query)
	if err != nil {
		t.Fatalf("list events failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected one record, got %d", len(records))
	}
	if got := records[0].ID; got != "evt_1" {
		t.Fatalf("expected event id evt_1, got %q", got)
	}
	if got, want := records[0].VisitID, "visit_1"; got != want {
		t.Fatalf("expected stored visit id %q, got %q", want, got)
	}
	if got := records[0].Properties; got != `{"path":"/"}` {
		t.Fatalf("expected properties JSON, got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestEventReaderListRealtimeUsesRealtimePlan(t *testing.T) {
	db, mock, cleanup := newMockClickHouseDB(t)
	defer cleanup()

	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := NewEventReader(db, builder)
	if err != nil {
		t.Fatalf("new event reader failed: %v", err)
	}

	query := storage.RealtimeQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Since:     time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC),
		Limit:     5,
	}
	plan, err := builder.BuildRealtimeQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("build expected plan failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(plan.SQL)).
		WithArgs(driverArgs(plan.Args)...).
		WillReturnRows(newEventRows())

	records, err := reader.ListRealtime(context.Background(), query)
	if err != nil {
		t.Fatalf("list realtime failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no realtime rows, got %d", len(records))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestEventReaderCountEventsUsesCountPlan(t *testing.T) {
	db, mock, cleanup := newMockClickHouseDB(t)
	defer cleanup()

	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := NewEventReader(db, builder)
	if err != nil {
		t.Fatalf("new event reader failed: %v", err)
	}

	query := storage.EventCountQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		EventName: "signup_started",
		From:      time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC),
		To:        time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC),
	}
	plan, err := builder.BuildEventCountQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("build expected count plan failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(plan.SQL)).
		WithArgs(driverArgs(plan.Args)...).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(42)))

	result, err := reader.CountEventsWithEvidence(context.Background(), query)
	if err != nil {
		t.Fatalf("count events with evidence failed: %v", err)
	}
	if result.Count != 42 {
		t.Fatalf("expected count 42, got %d", result.Count)
	}
	if result.Evidence.Family != storage.EventQueryFamilyGoal {
		t.Fatalf("expected goal count evidence, got %#v", result.Evidence)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestEventReaderListEventsWithEvidenceReturnsPlanEvidence(t *testing.T) {
	db, mock, cleanup := newMockClickHouseDB(t)
	defer cleanup()

	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := NewEventReader(db, builder)
	if err != nil {
		t.Fatalf("new event reader failed: %v", err)
	}

	query := storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Limit:     10,
	}
	plan, err := builder.BuildEventsQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("build expected plan failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(plan.SQL)).
		WithArgs(driverArgs(plan.Args)...).
		WillReturnRows(newEventRows())

	result, err := reader.ListEventsWithEvidence(context.Background(), query)
	if err != nil {
		t.Fatalf("list events with evidence failed: %v", err)
	}
	if len(result.Records) != 0 {
		t.Fatalf("expected no records, got %d", len(result.Records))
	}
	if result.Evidence.Family != storage.EventQueryFamilyEvents {
		t.Fatalf("expected events evidence, got %#v", result.Evidence)
	}
	if result.Evidence.Optimization != storage.EventQueryOptimizationDirectFactTable {
		t.Fatalf("expected direct fact-table evidence, got %#v", result.Evidence)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestEventReaderListEventsTreatsMissingRoutedTableAsEmpty(t *testing.T) {
	db, mock, cleanup := newMockClickHouseDB(t)
	defer cleanup()

	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := NewEventReader(db, builder)
	if err != nil {
		t.Fatalf("new event reader failed: %v", err)
	}

	query := storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Limit:     10,
	}
	plan, err := builder.BuildEventsQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("build expected plan failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(plan.SQL)).
		WithArgs(driverArgs(plan.Args)...).
		WillReturnError(&clickhouseproto.Exception{
			Code:    60,
			Message: "Unknown table expression identifier 'events_deadbeef'",
		})

	records, err := reader.ListEvents(context.Background(), query)
	if err != nil {
		t.Fatalf("expected missing routed table to behave like empty results: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no records when routed table is missing, got %d", len(records))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestEventReaderCountEventsTreatsMissingRoutedTableAsZero(t *testing.T) {
	db, mock, cleanup := newMockClickHouseDB(t)
	defer cleanup()

	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}
	reader, err := NewEventReader(db, builder)
	if err != nil {
		t.Fatalf("new event reader failed: %v", err)
	}

	query := storage.EventCountQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		EventName: "signup_started",
		From:      time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC),
		To:        time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC),
	}
	plan, err := builder.BuildEventCountQuery(context.Background(), query)
	if err != nil {
		t.Fatalf("build expected count plan failed: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(plan.SQL)).
		WithArgs(driverArgs(plan.Args)...).
		WillReturnError(&clickhouseproto.Exception{
			Code:    60,
			Message: "Unknown table expression identifier 'events_deadbeef'",
		})

	count, err := reader.CountEvents(context.Background(), query)
	if err != nil {
		t.Fatalf("expected missing routed table to behave like zero count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected zero count when routed table is missing, got %d", count)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func newMockClickHouseDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock, func()) {
	t.Helper()

	// Use sqlmock behind the ClickHouse GORM dialector so reader tests exercise
	// Raw execution without requiring a local ClickHouse server.
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("new sqlmock failed: %v", err)
	}
	db, err := gorm.Open(gormclickhouse.New(gormclickhouse.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		_ = sqlDB.Close()
		t.Fatalf("open gorm clickhouse db failed: %v", err)
	}
	return db, mock, func() {
		_ = sqlDB.Close()
	}
}

func newEventRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"event_id",
		"tenant_id",
		"project_id",
		"source_id",
		"source_type",
		"event_name",
		"distinct_id",
		"session_id",
		"visit_id",
		"event_time",
		"received_at",
		"properties",
		"user_properties",
		"source",
	})
}

func driverArgs(values []any) []driver.Value {
	args := make([]driver.Value, 0, len(values))
	// sqlmock expects driver.Value variadic arguments, while query plans keep a
	// storage-neutral []any shape for production callers.
	for _, value := range values {
		args = append(args, value)
	}
	return args
}
