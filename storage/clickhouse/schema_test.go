package clickhouse

import (
	"strings"
	"testing"
)

func TestCreateEventTableStatementUsesWriterColumnContract(t *testing.T) {
	ddl, err := CreateEventTableStatement(Table{Logical: "events", Physical: "events_contract"})
	if err != nil {
		t.Fatalf("create event table statement failed: %v", err)
	}

	for _, column := range eventInsertColumns {
		if !strings.Contains(ddl, quoteClickHouseIdentifier(column)+" ") {
			t.Fatalf("event DDL does not contain writer column %q: %s", column, ddl)
		}
	}
	if !strings.Contains(ddl, "ORDER BY (tenant_id, project_id, source_id, event_time, event_id)") {
		t.Fatalf("event DDL does not contain expected order key: %s", ddl)
	}
}

func TestCreatePropertyTableStatementUsesWriterColumnContract(t *testing.T) {
	ddl, err := CreatePropertyTableStatement(Table{Logical: "events", Physical: "events_contract"})
	if err != nil {
		t.Fatalf("create property table statement failed: %v", err)
	}

	for _, column := range propertyInsertColumns {
		if !strings.Contains(ddl, quoteClickHouseIdentifier(column)+" ") {
			t.Fatalf("property DDL does not contain writer column %q: %s", column, ddl)
		}
	}
	if !strings.Contains(ddl, "ORDER BY (tenant_id, project_id, source_id, property_scope, property_name, event_time, event_id)") {
		t.Fatalf("property DDL does not contain expected order key: %s", ddl)
	}
}

func TestCreateTableStatementsRejectUnsafePhysicalNames(t *testing.T) {
	_, eventErr := CreateEventTableStatement(Table{Logical: "events", Physical: "events;DROP"})
	if eventErr == nil {
		t.Fatalf("expected unsafe event table name to fail")
	}

	_, propertyErr := CreatePropertyTableStatement(Table{Logical: "events", Physical: "events demo"})
	if propertyErr == nil {
		t.Fatalf("expected unsafe property table name to fail")
	}
}

func TestPropertyTableForUsesEventTableFamily(t *testing.T) {
	propertyTable, err := PropertyTableFor(Table{Logical: "events", Physical: "events_contract"})
	if err != nil {
		t.Fatalf("property table routing failed: %v", err)
	}
	if propertyTable.Logical != "events_properties" {
		t.Fatalf("unexpected logical property table %q", propertyTable.Logical)
	}
	if propertyTable.Physical != "events_contract_properties" {
		t.Fatalf("unexpected physical property table %q", propertyTable.Physical)
	}
}
