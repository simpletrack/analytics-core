package storage_test

import (
	"fmt"

	"github.com/simpletrack/analytics-core/storage"
)

func ExampleEventQueryPlan_QueryEvidence() {
	plan := storage.NewEventQueryPlan("SELECT 1", nil, "events", "events_tenant", 50, storage.EventQueryEvidence{
		Family:              storage.EventQueryFamilyEvents,
		ReadPath:            storage.EventReadPathFactEvents,
		Optimization:        storage.EventQueryOptimizationDirectFactTable,
		PropertyFilterCount: 1,
		UsesPropertyTable:   true,
		PropertyFilters: []storage.EventPropertyFilterEvidence{
			{
				Scope:     storage.PropertyScopeEvent,
				Name:      "plan",
				ValueType: storage.PropertyValueString,
				Operator:  storage.EventFilterEquals,
			},
		},
	})

	evidence := plan.QueryEvidence()
	filter := evidence.PropertyFilters[0]

	fmt.Println(evidence.ReadPath)
	fmt.Println(filter.Scope, filter.Name, filter.ValueType, filter.Operator)

	// Output:
	// fact_events
	// event plan string eq
}
