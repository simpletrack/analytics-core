package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

func ExamplePropertyIndexingEventWriter_WriteEvent() {
	events := &examplePrimaryEventWriter{}
	properties := &examplePropertyIndexWriter{}
	guard := &examplePropertyWriteGuard{claim: &examplePropertyWriteClaim{}}
	writer, _ := NewPropertyIndexingEventWriter(events, properties, guard)

	_, _ = writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		EventTime:  time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 5, 2, 8, 0, 1, 0, time.UTC),
		Properties: map[string]any{"button": "hero"},
		UserProps:  map[string]any{"plan": "free"},
	})

	fmt.Println(events.writes)
	fmt.Println(properties.rows)

	// Output:
	// 1
	// 2
}

type examplePrimaryEventWriter struct {
	writes int // writes records primary event rows accepted by the example
}

func (w *examplePrimaryEventWriter) WriteEvent(context.Context, contracts.EventEnvelope) (WriteResult, error) {
	// Production writers handle ClickHouse routing and idempotency; this example
	// only proves the decorator calls the primary writer first.
	w.writes++
	return WriteResult{Inserted: true}, nil
}

type examplePropertyIndexWriter struct {
	rows int // rows records property rows accepted by the example
}

func (w *examplePropertyIndexWriter) WriteEventProperties(_ context.Context, records []EventPropertyRecord) (PropertyWriteResult, error) {
	// Production property writers batch by routed ClickHouse table; this example
	// only counts the flattened event and user property rows.
	w.rows += len(records)
	return PropertyWriteResult{Rows: len(records)}, nil
}

type examplePropertyWriteGuard struct {
	claim PropertyWriteClaim // claim is returned for the example property batch
}

func (g *examplePropertyWriteGuard) StartPropertyWrite(context.Context, contracts.EventEnvelope) (PropertyWriteClaim, error) {
	// Production guards persist this checkpoint in MySQL; the example keeps the
	// guard in memory to show the storage decorator shape.
	return g.claim, nil
}

type examplePropertyWriteClaim struct{}

func (c *examplePropertyWriteClaim) AlreadyInserted() bool {
	return false
}

func (c *examplePropertyWriteClaim) Commit(context.Context) error {
	return nil
}

func (c *examplePropertyWriteClaim) Rollback(context.Context, error) error {
	return nil
}
