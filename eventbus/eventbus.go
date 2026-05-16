package eventbus

import (
	"context"

	"github.com/simpletrack/analytics-core/contracts"
)

// ConsumerGroup identifies a logical consumer group for an EventBus subscription.
type ConsumerGroup struct {
	Name     string // Name is the consumer group shared by one logical pipeline
	Consumer string // Consumer is the concrete consumer instance name inside the group
}

// Message wraps an event with queue metadata.
//
// Handlers do not acknowledge messages directly. Returning nil tells the
// provider that processing is complete; returning an error tells the provider
// to retry, dead-letter, or hold queue progress according to backend rules.
type Message struct {
	ID        string                  // ID is the provider-native delivery identifier; event identity stays in Envelope.ID
	Topic     string                  // Topic is the topic, stream, or channel that carried the message
	Partition int32                   // Partition is the Kafka partition; non-partitioned providers leave it zero
	Offset    int64                   // Offset is the Kafka offset; non-offset providers leave it zero
	Key       string                  // Key is the backend routing key when one is available
	Attempt   int                     // Attempt is the one-based delivery attempt observed by the adapter
	Envelope  contracts.EventEnvelope // Envelope is the validated analytics event payload
}

// Handler processes one event message. Returning nil marks the message complete.
type Handler func(context.Context, Message) error

// EventBus hides the concrete queue implementation from collect and ingestion.
type EventBus interface {
	// Publish appends one validated event envelope to the queue.
	Publish(context.Context, contracts.EventEnvelope) error
	// Subscribe consumes messages for group and delegates each message to handler.
	Subscribe(context.Context, ConsumerGroup, Handler) error
}
