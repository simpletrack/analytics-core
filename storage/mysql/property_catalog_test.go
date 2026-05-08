package mysql

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/simpletrack/analytics-core/storage"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func ExamplePropertyCatalogEntry_TableName() {
	fmt.Println(PropertyCatalogEntry{}.TableName())
	// Output:
	// property_catalog
}

func TestPropertyCatalogUpsertsEntries(t *testing.T) {
	catalog, mock, closeDB := newTestPropertyCatalog(t)
	defer closeDB()

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `property_catalog`")).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := catalog.UpsertPropertyCatalogEntries(context.Background(), []storage.PropertyCatalogEntry{validPropertyCatalogEntry()})
	if err != nil {
		t.Fatalf("upsert property catalog entries failed: %v", err)
	}
	if result.Entries != 1 {
		t.Fatalf("entries = %d, want 1", result.Entries)
	}
	assertSQLExpectations(t, mock)
}

func TestPropertyCatalogSkipsEmptyBatch(t *testing.T) {
	catalog, mock, closeDB := newTestPropertyCatalog(t)
	defer closeDB()

	result, err := catalog.UpsertPropertyCatalogEntries(context.Background(), nil)
	if err != nil {
		t.Fatalf("empty catalog upsert failed: %v", err)
	}
	if result.Entries != 0 {
		t.Fatalf("entries = %d, want 0", result.Entries)
	}
	assertSQLExpectations(t, mock)
}

func TestPropertyCatalogValidatesEntries(t *testing.T) {
	catalog, _, closeDB := newTestPropertyCatalog(t)
	defer closeDB()

	entry := validPropertyCatalogEntry()
	entry.LastSeenAt = time.Time{}
	_, err := catalog.UpsertPropertyCatalogEntries(context.Background(), []storage.PropertyCatalogEntry{entry})
	if err == nil || !strings.Contains(err.Error(), "last_seen_at is required") {
		t.Fatalf("error = %v, want last_seen_at validation", err)
	}
}

// newTestPropertyCatalog returns a GORM catalog backed by sqlmock.
func newTestPropertyCatalog(t *testing.T) (*PropertyCatalog, sqlmock.Sqlmock, func()) {
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

	catalog, err := NewPropertyCatalog(gormDB)
	if err != nil {
		t.Fatalf("new property catalog failed: %v", err)
	}
	return catalog, mock, func() { _ = sqlDB.Close() }
}

// validPropertyCatalogEntry returns one complete property catalog entry.
func validPropertyCatalogEntry() storage.PropertyCatalogEntry {
	seenAt := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	return storage.PropertyCatalogEntry{
		TenantID:    "tenant_1",
		ProjectID:   "project_1",
		SourceID:    "source_1",
		Scope:       storage.PropertyScopeEvent,
		Name:        "button",
		ValueType:   storage.PropertyValueString,
		FirstSeenAt: seenAt,
		LastSeenAt:  seenAt,
	}
}
