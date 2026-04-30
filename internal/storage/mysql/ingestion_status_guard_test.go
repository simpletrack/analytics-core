package mysql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/simpletrack/analytics-core/pkg/contracts"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func ExampleIngestionStatus_TableName() {
	fmt.Println(IngestionStatus{}.TableName())
	// Output:
	// ingestion_status
}

func TestIngestionStatusGuardClaimsNewEvent(t *testing.T) {
	guard, mock, closeDB := newTestGuard(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `ingestion_status`")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	claim, err := guard.StartEventWrite(context.Background(), validEnvelope())
	if err != nil {
		t.Fatalf("start event write failed: %v", err)
	}
	if claim.AlreadyInserted() {
		t.Fatal("new event should not be marked already inserted")
	}
	assertSQLExpectations(t, mock)
}

func TestIngestionStatusGuardSkipsInsertedDuplicate(t *testing.T) {
	guard, mock, closeDB := newTestGuard(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `ingestion_status`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `ingestion_status`")).
		WillReturnRows(statusRows().AddRow("tenant_1", "project_1", "source_1", "evt_1", ingestionStatusInserted, 1, "", time.Now(), time.Now(), time.Now()))

	claim, err := guard.StartEventWrite(context.Background(), validEnvelope())
	if err != nil {
		t.Fatalf("start event write failed: %v", err)
	}
	if !claim.AlreadyInserted() {
		t.Fatal("inserted duplicate should be marked already inserted")
	}
	assertSQLExpectations(t, mock)
}

func TestIngestionStatusGuardReclaimsFailedEvent(t *testing.T) {
	guard, mock, closeDB := newTestGuard(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `ingestion_status`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `ingestion_status`")).
		WillReturnRows(statusRows().AddRow("tenant_1", "project_1", "source_1", "evt_1", ingestionStatusFailed, 2, "send failed", time.Now(), time.Now(), time.Now()))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `ingestion_status` SET")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	claim, err := guard.StartEventWrite(context.Background(), validEnvelope())
	if err != nil {
		t.Fatalf("start event write failed: %v", err)
	}
	if claim.AlreadyInserted() {
		t.Fatal("failed event should be reclaimed for retry")
	}
	assertSQLExpectations(t, mock)
}

func TestIngestionStatusClaimCommitAndRollbackUpdateStatus(t *testing.T) {
	guard, mock, closeDB := newTestGuard(t)
	defer closeDB()

	claim := &ingestionStatusClaim{
		db:  guard.db,
		key: statusKeyFromEnvelope(validEnvelope()),
		now: guard.now,
	}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE `ingestion_status` SET")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := claim.Commit(context.Background()); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE `ingestion_status` SET")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := claim.Rollback(context.Background(), errors.New("send failed")); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	assertSQLExpectations(t, mock)
}

func TestIngestionStatusGuardValidatesEventKey(t *testing.T) {
	guard, _, closeDB := newTestGuard(t)
	defer closeDB()

	_, err := guard.StartEventWrite(context.Background(), contracts.EventEnvelope{ID: "evt_1"})
	if err == nil || err.Error() != "tenant_id is required" {
		t.Fatalf("validation error = %v, want tenant_id is required", err)
	}
}

func newTestGuard(t *testing.T) (*IngestionStatusGuard, sqlmock.Sqlmock, func()) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("new sqlmock failed: %v", err)
	}
	gormDB, err := gorm.Open(gormmysql.New(gormmysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		t.Fatalf("open gorm failed: %v", err)
	}

	guard, err := NewIngestionStatusGuard(gormDB, WithClock(func() time.Time {
		return time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	}))
	if err != nil {
		t.Fatalf("new ingestion status guard failed: %v", err)
	}

	return guard, mock, func() { _ = sqlDB.Close() }
}

func validEnvelope() contracts.EventEnvelope {
	return contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		ReceivedAt: time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC),
	}
}

func statusRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"tenant_id",
		"project_id",
		"source_id",
		"event_id",
		"status",
		"attempt",
		"last_error",
		"received_at",
		"created_at",
		"updated_at",
	})
}

func assertSQLExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations were not met: %v", err)
	}
}
