package collect_test

import (
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/collect"
)

func ExampleNormalize() {
	receivedAt := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)

	envelope, err := collect.Normalize(collect.Request{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "pageview",
		DistinctID: "visitor_1",
	}, receivedAt)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(envelope.EventName)
	fmt.Println(envelope.EventTime.Format(time.RFC3339))

	// Output:
	// pageview
	// 2026-04-30T10:00:00Z
}
