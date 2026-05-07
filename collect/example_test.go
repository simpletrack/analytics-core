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

func ExampleNormalize_eventTimeAndReceivedAt() {
	receivedAt := time.Date(2026, 4, 30, 10, 0, 4, 0, time.UTC)
	eventTime := time.Date(2026, 4, 30, 10, 0, 2, 0, time.UTC)

	envelope, err := collect.Normalize(collect.Request{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "pageview",
		DistinctID: "visitor_1",
		EventTime:  eventTime,
	}, receivedAt)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(envelope.EventTime.Format(time.RFC3339))
	fmt.Println(envelope.ReceivedAt.Format(time.RFC3339))

	// Output:
	// 2026-04-30T10:00:02Z
	// 2026-04-30T10:00:04Z
}

func ExampleNormalize_validationRules() {
	receivedAt := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	request := collect.Request{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "Web",
		EventName:  "checkout completed",
		DistinctID: "visitor_1",
		Properties: map[string]any{
			"page.path": "/pricing",
		},
	}

	_, err := collect.Normalize(request, receivedAt)
	fmt.Println(err)

	request.SourceType = "web"
	_, err = collect.Normalize(request, receivedAt)
	fmt.Println(err)

	request.EventName = "checkout.completed"
	request.Properties["page path"] = "/pricing"
	_, err = collect.Normalize(request, receivedAt)
	fmt.Println(err)

	// Output:
	// source_type: contains unsupported characters
	// event_name: contains unsupported characters
	// properties.page path: key contains unsupported characters
}
