package collect_test

import (
	"context"
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/collect"
	"github.com/simpletrack/analytics-core/eventbus/direct"
)

func ExampleHandler_Handle() {
	bus := direct.New()
	handler, err := collect.NewHandler(bus, func() time.Time {
		return time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	envelope, err := handler.Handle(context.Background(), collect.Request{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "pageview",
		DistinctID: "visitor_1",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(envelope.ID)
	fmt.Println(envelope.ReceivedAt.Format(time.RFC3339))

	// Output:
	// evt_1
	// 2026-04-30T10:00:00Z
}
