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
//
// Implementations must keep database-specific batching, routing, retry, and
// idempotency mechanics behind this interface so ingestion can acknowledge the
// queue only after durable storage has either inserted or intentionally skipped
// a duplicate event.
type EventWriter interface {
	// WriteEvent persists one validated event and reports duplicate no-op writes.
	WriteEvent(context.Context, contracts.EventEnvelope) (WriteResult, error)
}
