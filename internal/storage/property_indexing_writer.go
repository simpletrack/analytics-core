package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

// PropertyIndexingEventWriter composes event writes with typed property indexing.
//
// The writer keeps ingestion coupled to storage.EventWriter only: queue workers
// pass one EventEnvelope, while this decorator fans out the validated event
// properties to EventPropertyWriter after the primary event row write succeeds.
//
// NOTE: P1 property rows use a separate idempotency checkpoint from the event
// row. This is intentional because a retry may need to repair property rows
// after the primary EventWriter reports Inserted=false for an existing event.
type PropertyIndexingEventWriter struct {
	events     EventWriter         // events writes the primary analytics event row and owns event idempotency
	properties EventPropertyWriter // properties writes flattened event and user property rows
	guard      PropertyWriteGuard  // guard prevents duplicate property batches while allowing partial repair
}

// NewPropertyIndexingEventWriter creates an EventWriter that also indexes properties.
func NewPropertyIndexingEventWriter(events EventWriter, properties EventPropertyWriter, guard PropertyWriteGuard) (*PropertyIndexingEventWriter, error) {
	// Validate both dependencies up front so ingestion startup fails before the
	// worker subscribes and begins acknowledging queue messages.
	if events == nil {
		return nil, errors.New("event writer is required")
	}
	if properties == nil {
		return nil, errors.New("event property writer is required")
	}
	if guard == nil {
		return nil, errors.New("property write guard is required")
	}
	return &PropertyIndexingEventWriter{events: events, properties: properties, guard: guard}, nil
}

// WriteEvent writes the event row and then indexes event and user properties.
func (w *PropertyIndexingEventWriter) WriteEvent(ctx context.Context, envelope contracts.EventEnvelope) (WriteResult, error) {
	if w == nil {
		return WriteResult{}, errors.New("property indexing event writer is required")
	}

	// Flatten first so malformed property values cannot produce an event row
	// that the property indexer can never represent. Collect should reject these
	// earlier; this is the storage boundary's defensive check.
	records, err := FlattenEventProperties(envelope)
	if err != nil {
		return WriteResult{}, err
	}

	// The primary writer owns durable event idempotency. A duplicate event is
	// still allowed to flow into property indexing so retries can repair a prior
	// "event committed, property write failed" partial outcome.
	result, err := w.events.WriteEvent(ctx, envelope)
	if err != nil {
		return WriteResult{}, err
	}
	if len(records) == 0 {
		return result, nil
	}

	// Claim property indexing after the event row is known to exist. This second
	// checkpoint prevents duplicate property batches while still allowing retry
	// repair when the event writer reports Inserted=false on a later delivery.
	claim, err := w.guard.StartPropertyWrite(ctx, envelope)
	if err != nil {
		return WriteResult{}, err
	}
	if claim == nil {
		return WriteResult{}, errors.New("property write guard returned nil claim")
	}
	if claim.AlreadyInserted() {
		return result, nil
	}

	// Property rows are written only after their own claim succeeds. A failure
	// is returned to the EventBus so the message can retry and fill the property
	// index later, even when the primary event is then seen as duplicate.
	if _, err := w.properties.WriteEventProperties(ctx, records); err != nil {
		return WriteResult{}, w.rollbackPropertyWrite(ctx, claim, fmt.Errorf("write event properties: %w", err))
	}
	// A commit failure after ClickHouse Send is deliberately not rolled back:
	// the property rows may already exist. The guard treats the remaining
	// processing status as ambiguous so retries cannot append duplicates.
	if err := claim.Commit(ctx); err != nil {
		return WriteResult{}, err
	}
	return result, nil
}

func (w *PropertyIndexingEventWriter) rollbackPropertyWrite(ctx context.Context, claim PropertyWriteClaim, cause error) error {
	// Preserve the original property batch failure while still surfacing guard
	// rollback problems so the queue retry path has the full storage context.
	if err := claim.Rollback(ctx, cause); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}
