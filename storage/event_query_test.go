package storage

import "testing"

func TestEventQueryPlanQueryEvidenceClonesPropertyFilters(t *testing.T) {
	t.Parallel()

	evidence := EventQueryEvidence{
		Family:              EventQueryFamilyEvents,
		ReadPath:            EventReadPathFactEvents,
		Optimization:        EventQueryOptimizationDirectFactTable,
		PropertyFilterCount: 1,
		UsesPropertyTable:   true,
		PropertyFilters: []EventPropertyFilterEvidence{
			{
				Scope:     PropertyScopeEvent,
				Name:      "plan",
				ValueType: PropertyValueString,
				Operator:  EventFilterEquals,
			},
		},
	}

	plan := NewEventQueryPlan("SELECT 1", nil, "events", "events_tenant", 50, evidence)

	// Mutating the caller-owned source slice must not rewrite plan evidence.
	evidence.PropertyFilters[0].Name = "mutated-before-read"
	firstRead := plan.QueryEvidence()
	if got := firstRead.PropertyFilters[0].Name; got != "plan" {
		t.Fatalf("stored property filter evidence was mutated: got %q", got)
	}

	// Mutating a returned slice must not rewrite the next evidence read.
	firstRead.PropertyFilters[0].Name = "mutated-after-read"
	secondRead := plan.QueryEvidence()
	if got := secondRead.PropertyFilters[0].Name; got != "plan" {
		t.Fatalf("returned property filter evidence was mutable: got %q", got)
	}
}
