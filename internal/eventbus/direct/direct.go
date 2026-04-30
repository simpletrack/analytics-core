package direct

import (
	"context"
	"sync"

	"github.com/simpletrack/analytics-core/internal/eventbus"
	"github.com/simpletrack/analytics-core/pkg/contracts"
)

// Bus is an in-process EventBus used for local tests and single-process demos.
type Bus struct {
	mu       sync.RWMutex
	handlers []eventbus.Handler
}

func New() *Bus {
	return &Bus{}
}

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

func (b *Bus) Subscribe(ctx context.Context, _ eventbus.ConsumerGroup, handler eventbus.Handler) error {
	b.mu.Lock()
	b.handlers = append(b.handlers, handler)
	b.mu.Unlock()

	<-ctx.Done()
	return ctx.Err()
}
