package storage

import (
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
)

func ExampleFlattenEventProperties() {
	records, _ := FlattenEventProperties(contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "signup",
		DistinctID: "visitor_1",
		VisitID:    "visit_1",
		EventTime:  time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 5, 1, 8, 0, 1, 0, time.UTC),
		Properties: map[string]any{"button": "hero"},
		UserProps:  map[string]any{"plan": "free"},
	})

	for _, record := range records {
		fmt.Printf("%s %s=%s\n", record.Scope, record.Name, record.StringValue)
	}

	// Output:
	// event button=hero
	// user plan=free
}
