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

// EventWriteGuard starts and finalizes the durable idempotency record for an event.
//
// The event table is optimized for append-heavy analytics writes, so exact
// duplicate prevention belongs in a small status/checkpoint store outside the
// hot event table. The guard owns that store and lets EventWriter adapters skip
// an already accepted event before they append a second analytics row.
type EventWriteGuard interface {
	// StartEventWrite claims the event id before the storage append starts.
	StartEventWrite(context.Context, contracts.EventEnvelope) (EventWriteClaim, error)
}

// EventWriteClaim is the per-event idempotency claim returned by EventWriteGuard.
//
// A claim should be keyed by tenant_id, project_id, source_id, and event_id. It
// may represent a new write, an in-progress write that this consumer owns, or an
// already inserted duplicate that must be treated as a successful no-op.
type EventWriteClaim interface {
	// AlreadyInserted reports whether the event was previously committed.
	AlreadyInserted() bool
	// Commit marks the claimed event as durably inserted after the event append succeeds.
	Commit(context.Context) error
	// Rollback releases or records the failed claim when the event append fails.
	Rollback(context.Context, error) error
}
