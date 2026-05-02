package ingestion

import (
	"context"
	"fmt"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/storage"
)

func ExampleProcessor_Run() {
	bus := &exampleBus{envelope: contracts.EventEnvelope{ID: "evt_1"}}
	store := &exampleStore{}
	processor, _ := NewProcessor(
		bus,
		eventbus.ConsumerGroup{Name: "ingestion", Consumer: "worker_1"},
		store,
	)

	_ = processor.Run(context.Background())

	fmt.Println(store.writes)

	// Output:
	// 1
}

type exampleBus struct {
	envelope contracts.EventEnvelope // envelope is delivered once to the subscriber
}

func (b *exampleBus) Publish(context.Context, contracts.EventEnvelope) error {
	return nil
}

func (b *exampleBus) Subscribe(ctx context.Context, _ eventbus.ConsumerGroup, handler eventbus.Handler) error {
	// The example bus delivers exactly one message so the worker wiring stays
	// visible without requiring Redis or Kafka.
	return handler(ctx, eventbus.Message{
		ID:       "message_1",
		Attempt:  1,
		Envelope: b.envelope,
	})
}

type exampleStore struct {
	writes int // writes records how many envelopes reached storage.EventWriter
}

func (s *exampleStore) WriteEvent(context.Context, contracts.EventEnvelope) (storage.WriteResult, error) {
	// The real writer owns ClickHouse append and idempotency; the example only
	// proves Processor.Run delegates to the storage boundary.
	s.writes++
	return storage.WriteResult{Inserted: true}, nil
}
