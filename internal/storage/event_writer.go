package storage

import (
	"context"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

// WriteResult reports whether a storage write inserted a new event.
type WriteResult struct {
	Inserted bool // Inserted is false when the event already existed and ingestion stayed idempotent
}

// EventWriter writes validated events to the analytics storage backend.
type EventWriter interface {
	WriteEvent(context.Context, contracts.EventEnvelope) (WriteResult, error)
}
