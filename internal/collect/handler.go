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
type Handler struct {
	bus eventbus.EventBus
	now Clock
}

// NewHandler creates a collect handler.
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
