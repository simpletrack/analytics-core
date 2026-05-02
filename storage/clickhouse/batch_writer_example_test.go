package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
)

func ExampleBatchWriter_WriteEvent() {
	router, _ := NewTableRouter("events")
	batch := &fakeNativeBatch{}
	writer, _ := newBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return batch, nil
	})

	result, _ := writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		EventName:  "page_view",
		DistinctID: "visitor_1",
		EventTime:  time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 4, 30, 10, 0, 1, 0, time.UTC),
	})

	fmt.Println(result.Inserted)
	fmt.Println(len(batch.appendRows))
	// Output:
	// true
	// 1
}
