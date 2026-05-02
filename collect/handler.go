package collect

import (
	"context"
	"errors"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

// Clock returns the current time for collect processing.
type Clock func() time.Time

// Handler validates collect requests and publishes normalized event envelopes.
//
// Handler is transport-neutral: HTTP adapters, workers, tests, and future SDK
// entrypoints should pass Request values here instead of passing framework
// context objects through the analytics core.
type Handler struct {
	bus    eventbus.EventBus // bus receives normalized envelopes after validation
	now    Clock             // now supplies deterministic server receive time for tests and adapters
	stages []Stage           // stages enrich or reject envelopes before queue publish
}

// NewHandler creates a collect handler with its required EventBus dependency.
func NewHandler(bus eventbus.EventBus, now Clock) (*Handler, error) {
	return NewHandlerWithOptions(bus, now)
}

// NewHandlerWithOptions creates a collect handler and applies optional stages.
func NewHandlerWithOptions(bus eventbus.EventBus, now Clock, opts ...HandlerOption) (*Handler, error) {
	// Validate the durable queue boundary first; a handler without EventBus
	// cannot safely acknowledge collect requests.
	if bus == nil {
		return nil, errors.New("event bus is required")
	}
	if now == nil {
		now = time.Now
	}

	// Apply options during construction so runtime request handling does not
	// need to defend against nil stages or partially built enrichment chains.
	handler := &Handler{bus: bus, now: now}
	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("collect handler option is required")
		}
		if err := opt(handler); err != nil {
			return nil, err
		}
	}
	return handler, nil
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

	receivedAt := h.now()
	envelope, err := Normalize(request, receivedAt)
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	stageInput := StageInput{Request: trimRequest(request), ReceivedAt: receivedAt.UTC()}
	for _, stage := range h.stages {
		// Stages run after protocol validation and before the durable queue. This
		// keeps enrichment, privacy-sensitive session derivation, and filtering
		// outside storage while still preventing rejected traffic from publishing.
		envelope, err = stage.Apply(ctx, stageInput, envelope)
		if err != nil {
			return envelope, err
		}
	}
	if err := h.bus.Publish(ctx, envelope); err != nil {
		return contracts.EventEnvelope{}, err
	}
	return envelope, nil
}
