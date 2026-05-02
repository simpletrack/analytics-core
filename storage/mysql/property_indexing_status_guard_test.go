package mysql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/simpletrack/analytics-core/contracts"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func ExamplePropertyIndexingStatus_TableName() {
	fmt.Println(PropertyIndexingStatus{}.TableName())
	// Output:
	// property_indexing_status
}

func TestPropertyIndexingStatusGuardClaimsNewEvent(t *testing.T) {
	guard, mock, closeDB := newTestPropertyGuard(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `property_indexing_status`")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	claim, err := guard.StartPropertyWrite(context.Background(), validEnvelope())
	if err != nil {
		t.Fatalf("start property write failed: %v", err)
	}
	if claim.AlreadyInserted() {
		t.Fatal("new property index should not be marked already inserted")
	}
	assertSQLExpectations(t, mock)
}

func TestPropertyIndexingStatusGuardSkipsInsertedDuplicate(t *testing.T) {
	guard, mock, closeDB := newTestPropertyGuard(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `property_indexing_status`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `property_indexing_status`")).
		WillReturnRows(statusRows().AddRow("tenant_1", "project_1", "source_1", "evt_1", propertyIndexingStatusInserted, 1, "", validEnvelope().ReceivedAt, validEnvelope().ReceivedAt, validEnvelope().ReceivedAt))

	claim, err := guard.StartPropertyWrite(context.Background(), validEnvelope())
	if err != nil {
		t.Fatalf("start property write failed: %v", err)
	}
	if !claim.AlreadyInserted() {
		t.Fatal("inserted property duplicate should be marked already inserted")
	}
	assertSQLExpectations(t, mock)
}

func TestPropertyIndexingStatusGuardReclaimsFailedEvent(t *testing.T) {
	guard, mock, closeDB := newTestPropertyGuard(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `property_indexing_status`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `property_indexing_status`")).
		WillReturnRows(statusRows().AddRow("tenant_1", "project_1", "source_1", "evt_1", propertyIndexingStatusFailed, 2, "send failed", validEnvelope().ReceivedAt, validEnvelope().ReceivedAt, validEnvelope().ReceivedAt))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `property_indexing_status` SET")).
		WillReturnResult(sqlmock.NewResult(0, 1))

	claim, err := guard.StartPropertyWrite(context.Background(), validEnvelope())
	if err != nil {
		t.Fatalf("start property write failed: %v", err)
	}
	if claim.AlreadyInserted() {
		t.Fatal("failed property index should be reclaimed for retry")
	}
	assertSQLExpectations(t, mock)
}

func TestPropertyIndexingStatusGuardRejectsProcessingEvent(t *testing.T) {
	guard, mock, closeDB := newTestPropertyGuard(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `property_indexing_status`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `property_indexing_status`")).
		WillReturnRows(statusRows().AddRow("tenant_1", "project_1", "source_1", "evt_1", propertyIndexingStatusProcessing, 2, "", validEnvelope().ReceivedAt, validEnvelope().ReceivedAt, validEnvelope().ReceivedAt))

	_, err := guard.StartPropertyWrite(context.Background(), validEnvelope())
	if err == nil || !strings.Contains(err.Error(), "property indexing status \"processing\" is not reclaimable") {
		t.Fatalf("start property write error = %v, want non-reclaimable processing", err)
	}
	assertSQLExpectations(t, mock)
}

func TestPropertyIndexingStatusGuardRequiresExclusiveFailedReclaim(t *testing.T) {
	guard, mock, closeDB := newTestPropertyGuard(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `property_indexing_status`")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `property_indexing_status`")).
		WillReturnRows(statusRows().AddRow("tenant_1", "project_1", "source_1", "evt_1", propertyIndexingStatusFailed, 2, "send failed", validEnvelope().ReceivedAt, validEnvelope().ReceivedAt, validEnvelope().ReceivedAt))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE `property_indexing_status` SET")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	_, err := guard.StartPropertyWrite(context.Background(), validEnvelope())
	if err == nil || !strings.Contains(err.Error(), "property indexing status was not reclaimed") {
		t.Fatalf("start property write error = %v, want exclusive reclaim failure", err)
	}
	assertSQLExpectations(t, mock)
}

func TestPropertyIndexingStatusClaimCommitAndRollbackUpdateStatus(t *testing.T) {
	guard, mock, closeDB := newTestPropertyGuard(t)
	defer closeDB()

	claim := &propertyIndexingStatusClaim{
		db:  guard.db,
		key: statusKeyFromEnvelope(validEnvelope()),
	}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE `property_indexing_status` SET")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := claim.Commit(context.Background()); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE `property_indexing_status` SET")).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := claim.Rollback(context.Background(), errors.New("send failed")); err != nil {
		t.Fatalf("rollback failed: %v", err)
	}
	assertSQLExpectations(t, mock)
}

func TestPropertyIndexingStatusGuardValidatesEventKey(t *testing.T) {
	guard, _, closeDB := newTestPropertyGuard(t)
	defer closeDB()

	_, err := guard.StartPropertyWrite(context.Background(), contracts.EventEnvelope{ID: "evt_1"})
	if err == nil || err.Error() != "tenant_id is required" {
		t.Fatalf("validation error = %v, want tenant_id is required", err)
	}
}

func newTestPropertyGuard(t *testing.T) (*PropertyIndexingStatusGuard, sqlmock.Sqlmock, func()) {
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

	guard, err := NewPropertyIndexingStatusGuard(gormDB)
	if err != nil {
		t.Fatalf("new property indexing status guard failed: %v", err)
	}

	return guard, mock, func() { _ = sqlDB.Close() }
}
