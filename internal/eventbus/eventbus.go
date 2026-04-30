package eventbus

import (
	"context"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

// ConsumerGroup identifies a logical consumer group for an EventBus subscription.
type ConsumerGroup struct {
	Name     string // Name is the consumer group shared by one logical pipeline
	Consumer string // Consumer is the concrete consumer instance name inside the group
}

// Message wraps an event with queue metadata and acknowledgement hooks.
type Message struct {
	ID       string                             // ID is the queue-native message identifier
	Attempt  int                                // Attempt is the one-based delivery attempt observed by the adapter
	Envelope contracts.EventEnvelope            // Envelope is the validated analytics event payload
	Ack      func(context.Context) error        // Ack acknowledges successful processing
	Nack     func(context.Context, error) error // Nack records failed processing
}

// Handler processes one event message. Returning nil means the message can be acknowledged.
type Handler func(context.Context, Message) error

// EventBus hides the concrete queue implementation from collect and ingestion.
type EventBus interface {
	// Publish appends one validated event envelope to the queue.
	Publish(context.Context, contracts.EventEnvelope) error
	// Subscribe consumes messages for group and delegates each message to handler.
	Subscribe(context.Context, ConsumerGroup, Handler) error
}
