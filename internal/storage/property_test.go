package storage

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

func TestFlattenEventPropertiesBuildsDeterministicTypedRows(t *testing.T) {
	eventTime := time.Date(2026, 5, 1, 8, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	receivedAt := eventTime.Add(time.Second)
	records, err := FlattenEventProperties(contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "signup",
		DistinctID: "visitor_1",
		SessionID:  "session_1",
		EventTime:  eventTime,
		ReceivedAt: receivedAt,
		Source:     "browser",
		Properties: map[string]any{
			"button": "hero",
			"score":  json.Number("42.5"),
		},
		UserProps: map[string]any{
			"beta": true,
			"plan": "free",
		},
	})
	if err != nil {
		t.Fatalf("flatten properties failed: %v", err)
	}

	if len(records) != 4 {
		t.Fatalf("expected four property rows, got %d: %#v", len(records), records)
	}
	wantOrder := []struct {
		scope PropertyScope
		name  string
	}{
		{scope: PropertyScopeEvent, name: "button"},
		{scope: PropertyScopeEvent, name: "score"},
		{scope: PropertyScopeUser, name: "beta"},
		{scope: PropertyScopeUser, name: "plan"},
	}
	for idx, want := range wantOrder {
		got := records[idx]
		if got.Scope != want.scope || got.Name != want.name {
			t.Fatalf("record %d scope/name = %s/%s, want %s/%s", idx, got.Scope, got.Name, want.scope, want.name)
		}
		if got.EventID != "evt_1" || got.TenantID != "tenant_1" || got.ProjectID != "project_1" || got.SourceID != "source_1" {
			t.Fatalf("record %d did not copy event identity: %#v", idx, got)
		}
		if got.Source != "browser" {
			t.Fatalf("record %d source = %q, want browser", idx, got.Source)
		}
		if got.EventTime != eventTime.UTC() || got.ReceivedAt != receivedAt.UTC() {
			t.Fatalf("record %d timestamps = %s/%s, want UTC copies", idx, got.EventTime, got.ReceivedAt)
		}
	}
	if records[0].ValueType != PropertyValueString || records[0].StringValue != "hero" {
		t.Fatalf("button value = %#v", records[0])
	}
	if records[1].ValueType != PropertyValueNumber || records[1].NumberValue != 42.5 {
		t.Fatalf("score value = %#v", records[1])
	}
	if records[2].ValueType != PropertyValueBool || !records[2].BoolValue {
		t.Fatalf("beta value = %#v", records[2])
	}
}

func TestFlattenEventPropertiesReturnsNoRowsForEmptyMaps(t *testing.T) {
	records, err := FlattenEventProperties(contracts.EventEnvelope{ID: "evt_1"})
	if err != nil {
		t.Fatalf("flatten empty properties failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no property rows, got %#v", records)
	}
}

func TestFlattenEventPropertiesRejectsUnsupportedValues(t *testing.T) {
	_, err := FlattenEventProperties(contracts.EventEnvelope{
		ID:         "evt_1",
		Properties: map[string]any{"nested": map[string]any{"path": "/"}},
	})
	if err == nil {
		t.Fatal("expected unsupported value error")
	}
	if !strings.Contains(err.Error(), "property event.nested has unsupported value type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFlattenEventPropertiesRejectsInvalidNumbers(t *testing.T) {
	_, err := FlattenEventProperties(contracts.EventEnvelope{
		ID:         "evt_1",
		Properties: map[string]any{"score": json.Number("not-a-number")},
	})
	if err == nil {
		t.Fatal("expected invalid number error")
	}
	if !strings.Contains(err.Error(), "property event.score has invalid number") {
		t.Fatalf("unexpected error: %v", err)
	}
}
