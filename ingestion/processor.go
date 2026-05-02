package ingestion

import (
	"context"
	"errors"

	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/storage"
)

// Processor consumes events from an EventBus and writes them idempotently.
//
// In P1 this is the worker boundary: Redis Stream, Kafka, and future queue
// adapters enter through eventbus.EventBus, while durable append and duplicate
// protection stay behind storage.EventWriter.
type Processor struct {
	bus   eventbus.EventBus      // bus supplies queue messages and owns ack/nack semantics
	group eventbus.ConsumerGroup // group identifies this ingestion worker in the queue backend
	store storage.EventWriter    // store persists events and reports duplicate no-op writes
}

// NewProcessor creates an ingestion processor.
func NewProcessor(bus eventbus.EventBus, group eventbus.ConsumerGroup, store storage.EventWriter) (*Processor, error) {
	// Validate queue, worker identity, and storage dependencies at construction
	// time so Run can stay a pure subscription loop.
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
	// Subscribe owns ack/nack behavior; the processor only returns handler
	// errors so the EventBus can retry or dead-letter according to its backend.
	return p.bus.Subscribe(ctx, p.group, p.handle)
}

func (p *Processor) handle(ctx context.Context, msg eventbus.Message) error {
	// EventWriter owns the ClickHouse append plus idempotency guard. Returning
	// its error keeps queue acknowledgement tied to durable write completion.
	_, err := p.store.WriteEvent(ctx, msg.Envelope)
	return err
}
