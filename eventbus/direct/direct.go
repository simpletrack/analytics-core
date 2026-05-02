package direct

import (
	"context"
	"sync"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

// Bus is an in-process EventBus used for local tests and single-process demos.
type Bus struct {
	mu       sync.RWMutex       // mu protects handler registration and publish snapshots
	handlers []eventbus.Handler // handlers are synchronous in-process subscribers
}

// New creates an in-process event bus.
func New() *Bus {
	return &Bus{}
}

// Publish synchronously delivers envelope to all registered handlers.
func (b *Bus) Publish(ctx context.Context, envelope contracts.EventEnvelope) error {
	b.mu.RLock()
	handlers := append([]eventbus.Handler(nil), b.handlers...)
	b.mu.RUnlock()

	msg := eventbus.Message{
		ID:       envelope.ID,
		Attempt:  1,
		Envelope: envelope,
		Ack:      func(context.Context) error { return nil },
		Nack:     func(context.Context, error) error { return nil },
	}

	for _, handler := range handlers {
		if err := handler(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe registers handler until ctx is cancelled.
func (b *Bus) Subscribe(ctx context.Context, _ eventbus.ConsumerGroup, handler eventbus.Handler) error {
	b.mu.Lock()
	b.handlers = append(b.handlers, handler)
	b.mu.Unlock()

	<-ctx.Done()
	return ctx.Err()
}
