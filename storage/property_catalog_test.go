package storage

import (
	"fmt"
	"testing"
	"time"
)

func ExampleBuildPropertyCatalogEntries() {
	records := []EventPropertyRecord{
		{
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			SourceID:  "source_1",
			Scope:     PropertyScopeEvent,
			Name:      "button",
			ValueType: PropertyValueString,
			EventTime: time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		},
	}

	entries, _ := BuildPropertyCatalogEntries(records)
	fmt.Println(entries[0].Scope, entries[0].Name, entries[0].ValueType)
	// Output:
	// event button string
}

func TestBuildPropertyCatalogEntriesDeduplicatesAndSorts(t *testing.T) {
	records := []EventPropertyRecord{
		catalogRecord(PropertyScopeUser, "plan", PropertyValueString, "signup", time.Date(2026, 5, 8, 10, 3, 0, 0, time.UTC)),
		catalogRecord(PropertyScopeEvent, "button", PropertyValueString, "pageview", time.Date(2026, 5, 8, 10, 1, 0, 0, time.UTC)),
		catalogRecord(PropertyScopeEvent, "button", PropertyValueString, "cta_click", time.Date(2026, 5, 8, 10, 5, 0, 0, time.UTC)),
	}

	entries, err := BuildPropertyCatalogEntries(records)
	if err != nil {
		t.Fatalf("build property catalog entries failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2: %#v", len(entries), entries)
	}
	if entries[0].Scope != PropertyScopeEvent || entries[0].Name != "button" {
		t.Fatalf("first entry should be event.button, got %#v", entries[0])
	}
	if entries[0].FirstSeenAt != time.Date(2026, 5, 8, 10, 1, 0, 0, time.UTC) {
		t.Fatalf("event.button first seen = %s", entries[0].FirstSeenAt)
	}
	if entries[0].LastSeenAt != time.Date(2026, 5, 8, 10, 5, 0, 0, time.UTC) {
		t.Fatalf("event.button last seen = %s", entries[0].LastSeenAt)
	}
	if entries[1].Scope != PropertyScopeUser || entries[1].Name != "plan" {
		t.Fatalf("second entry should be user.plan, got %#v", entries[1])
	}
}

func TestBuildPropertyCatalogEntriesRejectsInvalidRecords(t *testing.T) {
	_, err := BuildPropertyCatalogEntries([]EventPropertyRecord{
		{
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			SourceID:  "source_1",
			Scope:     PropertyScopeEvent,
			ValueType: PropertyValueString,
			EventTime: time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		},
	})
	if err == nil || err.Error() != "property name is required" {
		t.Fatalf("error = %v, want property name is required", err)
	}
}

// catalogRecord builds one minimal property row for catalog tests.
func catalogRecord(scope PropertyScope, name string, valueType PropertyValueType, eventName string, eventTime time.Time) EventPropertyRecord {
	return EventPropertyRecord{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Scope:     scope,
		Name:      name,
		ValueType: valueType,
		EventName: eventName,
		EventTime: eventTime,
	}
}
