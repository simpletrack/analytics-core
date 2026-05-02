package collect

import (
	"context"
	"errors"
	"time"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

// StageInput carries normalized collect context into an optional pre-queue stage.
type StageInput struct {
	Request    Request   // Request is the trimmed and validated public collect input plus transient client metadata
	ReceivedAt time.Time // ReceivedAt is the server acceptance timestamp used for deterministic stage behavior
}

// Stage transforms or rejects an event envelope before collect publishes it.
//
// Stage implementations are expected to stay framework-neutral. They may read
// Request.Client for temporary user agent, referrer, or IP context, but they
// should only return EventEnvelope values and typed errors.
type Stage interface {
	// Apply inspects collect context and returns the envelope that should be published.
	Apply(context.Context, StageInput, contracts.EventEnvelope) (contracts.EventEnvelope, error)
}

// StageFunc adapts a function to the Stage interface.
type StageFunc func(context.Context, StageInput, contracts.EventEnvelope) (contracts.EventEnvelope, error)

// Apply executes f as a Stage.
func (f StageFunc) Apply(ctx context.Context, input StageInput, envelope contracts.EventEnvelope) (contracts.EventEnvelope, error) {
	if f == nil {
		return contracts.EventEnvelope{}, errors.New("collect stage is required")
	}
	return f(ctx, input, envelope)
}

// HandlerOption configures a Handler during construction.
type HandlerOption func(*Handler) error

// WithStages appends framework-neutral pre-queue stages to a Handler.
func WithStages(stages ...Stage) HandlerOption {
	return func(handler *Handler) error {
		for _, stage := range stages {
			if stage == nil {
				return errors.New("collect stage is required")
			}
			handler.stages = append(handler.stages, stage)
		}
		return nil
	}
}
