package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/simpletrack/analytics-core/storage"
)

func ExampleEventQueryBuilder_BuildEventsQuery() {
	router, _ := NewTableRouter("events")
	builder, _ := NewEventQueryBuilder(router)

	plan, _ := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Limit:     20,
	})

	fmt.Println(plan.LogicalTable)
	fmt.Println(strings.HasPrefix(plan.PhysicalTable, "events_"))
	fmt.Println(strings.Contains(plan.SQL, "ORDER BY event_time DESC"))
	fmt.Println(plan.Limit)

	// Output:
	// events
	// true
	// true
	// 20
}
