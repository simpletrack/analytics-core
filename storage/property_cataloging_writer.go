package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/simpletrack/analytics-core/contracts"
)

// PropertyCatalogingEventWriter composes event writes with property catalog updates.
//
// The decorator keeps ingestion coupled to storage.EventWriter while allowing
// runtime services to record property metadata for UI suggestions and future
// allowlist workflows. It does not write ClickHouse property rows; combine it
// with PropertyIndexingEventWriter when physical property indexing is enabled.
type PropertyCatalogingEventWriter struct {
	events  EventWriter     // events writes the primary analytics event and any inner decorators
	catalog PropertyCatalog // catalog records observed property selectors and value types
}

// NewPropertyCatalogingEventWriter creates an EventWriter that also updates a property catalog.
func NewPropertyCatalogingEventWriter(events EventWriter, catalog PropertyCatalog) (*PropertyCatalogingEventWriter, error) {
	// Validate dependencies before workers subscribe so configuration errors do
	// not turn into repeated queue nacks.
	if events == nil {
		return nil, errors.New("event writer is required")
	}
	if catalog == nil {
		return nil, errors.New("property catalog is required")
	}
	return &PropertyCatalogingEventWriter{events: events, catalog: catalog}, nil
}

// WriteEvent writes the event and then records observed property catalog entries.
func (w *PropertyCatalogingEventWriter) WriteEvent(ctx context.Context, envelope contracts.EventEnvelope) (WriteResult, error) {
	if w == nil {
		return WriteResult{}, errors.New("property cataloging event writer is required")
	}

	// Build catalog entries before the primary event write. Collect should have
	// already rejected malformed properties, but this keeps the storage boundary
	// from committing an event that metadata governance can never represent.
	records, err := FlattenEventProperties(envelope)
	if err != nil {
		return WriteResult{}, err
	}
	entries, err := BuildPropertyCatalogEntries(records)
	if err != nil {
		return WriteResult{}, err
	}

	// The inner writer owns event idempotency and optional ClickHouse property
	// indexing. Duplicates still flow to the catalog path so retries can repair a
	// prior "event committed, catalog write failed" partial outcome.
	result, err := w.events.WriteEvent(ctx, envelope)
	if err != nil {
		return WriteResult{}, err
	}
	if len(entries) == 0 {
		return result, nil
	}

	// Catalog writes are replay-safe metadata upserts. Returning the error keeps
	// the EventBus message retryable without appending duplicate event rows.
	if _, err := w.catalog.UpsertPropertyCatalogEntries(ctx, entries); err != nil {
		return WriteResult{}, fmt.Errorf("upsert property catalog: %w", err)
	}
	return result, nil
}
