package collect

import (
	"context"
	"errors"
	"time"

	"github.com/simpletrack/analytics-core/internal/eventbus"
	"github.com/simpletrack/analytics-core/pkg/contracts"
)

// Clock returns the current time for collect processing.
type Clock func() time.Time

// Handler validates collect requests and publishes normalized event envelopes.
//
// Handler is transport-neutral: HTTP adapters, workers, tests, and future SDK
// entrypoints should pass Request values here instead of passing framework
// context objects through the analytics core.
type Handler struct {
	bus eventbus.EventBus
	now Clock
}

// NewHandler creates a collect handler with its required EventBus dependency.
func NewHandler(bus eventbus.EventBus, now Clock) (*Handler, error) {
	if bus == nil {
		return nil, errors.New("event bus is required")
	}
	if now == nil {
		now = time.Now
	}
	return &Handler{bus: bus, now: now}, nil
}

// Handle normalizes request and publishes the resulting event envelope.
//
// The publish step is deliberately after normalization so invalid client input
// cannot enter the queue and force downstream ingestion to rediscover protocol
// errors.
func (h *Handler) Handle(ctx context.Context, request Request) (contracts.EventEnvelope, error) {
	if h == nil {
		return contracts.EventEnvelope{}, errors.New("collect handler is required")
	}

	envelope, err := Normalize(request, h.now())
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	if err := h.bus.Publish(ctx, envelope); err != nil {
		return contracts.EventEnvelope{}, err
	}
	return envelope, nil
}
