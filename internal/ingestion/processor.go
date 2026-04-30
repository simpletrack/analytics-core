package ingestion

import (
	"context"
	"errors"

	"github.com/simpletrack/analytics-core/internal/eventbus"
	"github.com/simpletrack/analytics-core/pkg/contracts"
)

// StoreResult reports whether an event write inserted a new row.
type StoreResult struct {
	Inserted bool // false means the event already existed and ingestion stayed idempotent
}

// EventStore writes validated events to the analytics storage backend.
type EventStore interface {
	WriteEvent(context.Context, contracts.EventEnvelope) (StoreResult, error)
}

// Processor consumes events from an EventBus and writes them idempotently.
type Processor struct {
	bus   eventbus.EventBus
	group eventbus.ConsumerGroup
	store EventStore
}

// NewProcessor creates an ingestion processor.
func NewProcessor(bus eventbus.EventBus, group eventbus.ConsumerGroup, store EventStore) (*Processor, error) {
	if bus == nil {
		return nil, errors.New("event bus is required")
	}
	if group.Name == "" {
		return nil, errors.New("consumer group name is required")
	}
	if group.Consumer == "" {
		return nil, errors.New("consumer name is required")
	}
	if store == nil {
		return nil, errors.New("event store is required")
	}
	return &Processor{bus: bus, group: group, store: store}, nil
}

// Run subscribes to the configured bus until ctx is cancelled.
func (p *Processor) Run(ctx context.Context) error {
	return p.bus.Subscribe(ctx, p.group, p.handle)
}

func (p *Processor) handle(ctx context.Context, msg eventbus.Message) error {
	_, err := p.store.WriteEvent(ctx, msg.Envelope)
	return err
}
