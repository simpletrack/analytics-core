package ingestion

import (
	"context"
	"errors"
	"testing"

	"github.com/simpletrack/analytics-core/internal/eventbus"
	"github.com/simpletrack/analytics-core/pkg/contracts"
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

type memoryStore struct {
	err      error
	inserted int
	seen     map[string]struct{}
}

func newMemoryStore() *memoryStore {
	return &memoryStore{seen: map[string]struct{}{}}
}

func (s *memoryStore) WriteEvent(_ context.Context, envelope contracts.EventEnvelope) (StoreResult, error) {
	if s.err != nil {
		return StoreResult{}, s.err
	}
	if _, ok := s.seen[envelope.ID]; ok {
		return StoreResult{Inserted: false}, nil
	}
	s.seen[envelope.ID] = struct{}{}
	s.inserted++
	return StoreResult{Inserted: true}, nil
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
