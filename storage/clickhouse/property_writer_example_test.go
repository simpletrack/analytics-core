package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/storage"
)

func ExamplePropertyBatchWriter_WriteEventProperties() {
	router, _ := NewTableRouter("events")
	batch := &fakeNativeBatch{}
	writer, _ := newPropertyBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return batch, nil
	})

	result, _ := writer.WriteEventProperties(context.Background(), []storage.EventPropertyRecord{
		{
			EventID:     "evt_1",
			TenantID:    "tenant_1",
			ProjectID:   "project_1",
			SourceID:    "source_1",
			EventTime:   time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC),
			ReceivedAt:  time.Date(2026, 5, 1, 8, 0, 1, 0, time.UTC),
			Scope:       storage.PropertyScopeEvent,
			Name:        "button",
			ValueType:   storage.PropertyValueString,
			StringValue: "hero",
		},
	})

	fmt.Println(result.Rows)
	fmt.Println(len(batch.appendRows))
	// Output:
	// 1
	// 1
}
