package ingestion

import (
	"context"
	"errors"
	"testing"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/storage"
)

func TestProcessorTreatsDuplicateEventsAsSuccessfulIngestion(t *testing.T) {
	store := newMemoryStore()
	processor, err := NewProcessor(
		newNoopBus(),
		eventbus.ConsumerGroup{Name: "ingestion", Consumer: "test"},
		store,
	)
	if err != nil {
		t.Fatalf("new processor failed: %v", err)
	}

	envelope := contracts.EventEnvelope{
		ID:        "evt_1",
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		EventName: "pageview",
	}

	msg := eventbus.Message{ID: "message_1", Attempt: 1, Envelope: envelope}
	if err := processor.handle(context.Background(), msg); err != nil {
		t.Fatalf("first ingestion failed: %v", err)
	}
	if err := processor.handle(context.Background(), msg); err != nil {
		t.Fatalf("duplicate ingestion should be acknowledged as success: %v", err)
	}
	if got := store.inserted; got != 1 {
		t.Fatalf("expected one inserted event, got %d", got)
	}
}

func TestProcessorReturnsStoreErrorsForRetry(t *testing.T) {
	store := newMemoryStore()
	store.err = errors.New("storage unavailable")

	processor, err := NewProcessor(
		newNoopBus(),
		eventbus.ConsumerGroup{Name: "ingestion", Consumer: "test"},
		store,
	)
	if err != nil {
		t.Fatalf("new processor failed: %v", err)
	}

	msg := eventbus.Message{
		ID:       "message_1",
		Attempt:  1,
		Envelope: contracts.EventEnvelope{ID: "evt_1"},
	}
	if err := processor.handle(context.Background(), msg); err == nil {
		t.Fatal("expected store error")
	}
}

func TestProcessorRunSubscribesAndWritesEvents(t *testing.T) {
	store := newMemoryStore()
	bus := &scriptedBus{
		messages: []eventbus.Message{
			{ID: "message_1", Envelope: contracts.EventEnvelope{ID: "evt_1"}},
			{ID: "message_2", Envelope: contracts.EventEnvelope{ID: "evt_2"}},
		},
	}

	processor, err := NewProcessor(
		bus,
		eventbus.ConsumerGroup{Name: "ingestion", Consumer: "test"},
		store,
	)
	if err != nil {
		t.Fatalf("new processor failed: %v", err)
	}

	if err := processor.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if got := store.inserted; got != 2 {
		t.Fatalf("expected two inserted events, got %d", got)
	}
	if got := bus.group.Name; got != "ingestion" {
		t.Fatalf("expected ingestion group, got %q", got)
	}
}

type memoryStore struct {
	err      error               // err forces WriteEvent to fail for retry tests
	inserted int                 // inserted records unique events written by the processor
	seen     map[string]struct{} // seen stores event ids for duplicate no-op simulation
}

func newMemoryStore() *memoryStore {
	return &memoryStore{seen: map[string]struct{}{}}
}

func (s *memoryStore) WriteEvent(_ context.Context, envelope contracts.EventEnvelope) (storage.WriteResult, error) {
	if s.err != nil {
		return storage.WriteResult{}, s.err
	}
	if _, ok := s.seen[envelope.ID]; ok {
		return storage.WriteResult{Inserted: false}, nil
	}
	s.seen[envelope.ID] = struct{}{}
	s.inserted++
	return storage.WriteResult{Inserted: true}, nil
}

type noopBus struct{}

func newNoopBus() *noopBus {
	return &noopBus{}
}

func (b *noopBus) Publish(context.Context, contracts.EventEnvelope) error {
	return nil
}

func (b *noopBus) Subscribe(context.Context, eventbus.ConsumerGroup, eventbus.Handler) error {
	return nil
}

type scriptedBus struct {
	group    eventbus.ConsumerGroup // group stores the subscription identity passed by Processor.Run
	messages []eventbus.Message     // messages are delivered synchronously to the handler
}

func (b *scriptedBus) Publish(context.Context, contracts.EventEnvelope) error {
	return nil
}

func (b *scriptedBus) Subscribe(ctx context.Context, group eventbus.ConsumerGroup, handler eventbus.Handler) error {
	// Capture the worker identity so the test proves Run wires the configured
	// consumer group into the EventBus subscription.
	b.group = group
	// Deliver messages synchronously to keep the test deterministic while still
	// exercising Processor.Run instead of the private handler directly.
	for _, msg := range b.messages {
		if err := handler(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}
