package eventbus

import (
	"context"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

// ConsumerGroup identifies a logical consumer group for an EventBus subscription.
type ConsumerGroup struct {
	Name     string
	Consumer string
}

// Message wraps an event with queue metadata and acknowledgement hooks.
type Message struct {
	ID       string
	Attempt  int
	Envelope contracts.EventEnvelope
	Ack      func(context.Context) error
	Nack     func(context.Context, error) error
}

// Handler processes one event message. Returning nil means the message can be acknowledged.
type Handler func(context.Context, Message) error

// EventBus hides the concrete queue implementation from collect and ingestion.
type EventBus interface {
	Publish(context.Context, contracts.EventEnvelope) error
	Subscribe(context.Context, ConsumerGroup, Handler) error
}
