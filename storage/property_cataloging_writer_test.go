package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
)

func TestPropertyCatalogingEventWriterWritesEventThenCatalog(t *testing.T) {
	events := &recordingCatalogEventWriter{result: WriteResult{Inserted: true}}
	catalog := &recordingPropertyCatalog{}
	writer, err := NewPropertyCatalogingEventWriter(events, catalog)
	if err != nil {
		t.Fatalf("new property cataloging writer failed: %v", err)
	}

	result, err := writer.WriteEvent(context.Background(), catalogEnvelope())
	if err != nil {
		t.Fatalf("write event failed: %v", err)
	}
	if !result.Inserted {
		t.Fatal("expected inserted result from inner writer")
	}
	if events.calls != 1 {
		t.Fatalf("inner writer calls = %d, want 1", events.calls)
	}
	if len(catalog.entries) != 2 {
		t.Fatalf("catalog entries = %d, want 2: %#v", len(catalog.entries), catalog.entries)
	}
	if catalog.entries[0].Scope != PropertyScopeEvent || catalog.entries[0].Name != "button" {
		t.Fatalf("first catalog entry = %#v, want event.button", catalog.entries[0])
	}
	if catalog.entries[1].Scope != PropertyScopeUser || catalog.entries[1].Name != "plan" {
		t.Fatalf("second catalog entry = %#v, want user.plan", catalog.entries[1])
	}
}

func TestPropertyCatalogingEventWriterRepairsCatalogForDuplicateEvent(t *testing.T) {
	events := &recordingCatalogEventWriter{result: WriteResult{Inserted: false}}
	catalog := &recordingPropertyCatalog{}
	writer, err := NewPropertyCatalogingEventWriter(events, catalog)
	if err != nil {
		t.Fatalf("new property cataloging writer failed: %v", err)
	}

	result, err := writer.WriteEvent(context.Background(), catalogEnvelope())
	if err != nil {
		t.Fatalf("write duplicate event failed: %v", err)
	}
	if result.Inserted {
		t.Fatal("expected duplicate event result to stay false")
	}
	if len(catalog.entries) != 2 {
		t.Fatalf("duplicate catalog repair entries = %d, want 2", len(catalog.entries))
	}
}

func TestPropertyCatalogingEventWriterSkipsCatalogWhenNoPropertiesExist(t *testing.T) {
	events := &recordingCatalogEventWriter{result: WriteResult{Inserted: true}}
	catalog := &recordingPropertyCatalog{}
	writer, err := NewPropertyCatalogingEventWriter(events, catalog)
	if err != nil {
		t.Fatalf("new property cataloging writer failed: %v", err)
	}

	result, err := writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:         "evt_empty",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		EventTime:  time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 5, 8, 10, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("write event without properties failed: %v", err)
	}
	if !result.Inserted {
		t.Fatal("expected inserted result from inner writer")
	}
	if len(catalog.entries) != 0 {
		t.Fatalf("catalog entries = %d, want 0", len(catalog.entries))
	}
}

func TestPropertyCatalogingEventWriterReturnsCatalogErrorForRetry(t *testing.T) {
	catalogErr := errors.New("catalog unavailable")
	events := &recordingCatalogEventWriter{result: WriteResult{Inserted: true}}
	catalog := &recordingPropertyCatalog{err: catalogErr}
	writer, err := NewPropertyCatalogingEventWriter(events, catalog)
	if err != nil {
		t.Fatalf("new property cataloging writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), catalogEnvelope())
	if !errors.Is(err, catalogErr) {
		t.Fatalf("write error = %v, want catalog error", err)
	}
	if !strings.Contains(err.Error(), "upsert property catalog") {
		t.Fatalf("write error = %v, want catalog context", err)
	}
}

func TestPropertyCatalogingEventWriterDoesNotCatalogWhenEventWriteFails(t *testing.T) {
	eventErr := errors.New("event writer unavailable")
	events := &recordingCatalogEventWriter{err: eventErr}
	catalog := &recordingPropertyCatalog{}
	writer, err := NewPropertyCatalogingEventWriter(events, catalog)
	if err != nil {
		t.Fatalf("new property cataloging writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), catalogEnvelope())
	if !errors.Is(err, eventErr) {
		t.Fatalf("write error = %v, want event error", err)
	}
	if catalog.calls != 0 {
		t.Fatalf("catalog calls = %d, want 0 after event failure", catalog.calls)
	}
}

func TestPropertyCatalogingEventWriterRejectsUnflattenablePropertiesBeforeEventWrite(t *testing.T) {
	events := &recordingCatalogEventWriter{result: WriteResult{Inserted: true}}
	catalog := &recordingPropertyCatalog{}
	writer, err := NewPropertyCatalogingEventWriter(events, catalog)
	if err != nil {
		t.Fatalf("new property cataloging writer failed: %v", err)
	}

	envelope := catalogEnvelope()
	envelope.Properties["nested"] = map[string]any{"unsupported": "shape"}
	_, err = writer.WriteEvent(context.Background(), envelope)
	if err == nil {
		t.Fatal("expected unsupported nested property error")
	}
	if !strings.Contains(err.Error(), "unsupported value type") {
		t.Fatalf("write error = %v, want unsupported value type", err)
	}
	if events.calls != 0 {
		t.Fatalf("event writer calls = %d, want 0 when flattening fails", events.calls)
	}
	if catalog.calls != 0 {
		t.Fatalf("catalog calls = %d, want 0 when flattening fails", catalog.calls)
	}
}

func TestPropertyCatalogingEventWriterRequiresDependencies(t *testing.T) {
	if _, err := NewPropertyCatalogingEventWriter(nil, &recordingPropertyCatalog{}); err == nil {
		t.Fatal("expected missing event writer error")
	}
	if _, err := NewPropertyCatalogingEventWriter(&recordingCatalogEventWriter{}, nil); err == nil {
		t.Fatal("expected missing property catalog error")
	}
}

type recordingCatalogEventWriter struct {
	err    error       // err forces event writing to fail
	result WriteResult // result is returned when err is nil
	calls  int         // calls counts WriteEvent invocations
}

func (w *recordingCatalogEventWriter) WriteEvent(context.Context, contracts.EventEnvelope) (WriteResult, error) {
	w.calls++
	if w.err != nil {
		return WriteResult{}, w.err
	}
	return w.result, nil
}

type recordingPropertyCatalog struct {
	calls   int                    // calls counts catalog upsert attempts
	err     error                  // err forces catalog upsert failure
	entries []PropertyCatalogEntry // entries stores the last upsert batch
}

func (c *recordingPropertyCatalog) UpsertPropertyCatalogEntries(_ context.Context, entries []PropertyCatalogEntry) (PropertyCatalogResult, error) {
	c.calls++
	if c.err != nil {
		return PropertyCatalogResult{}, c.err
	}
	c.entries = append([]PropertyCatalogEntry(nil), entries...)
	return PropertyCatalogResult{Entries: len(entries)}, nil
}

// catalogEnvelope returns one event with event and user properties.
func catalogEnvelope() contracts.EventEnvelope {
	return contracts.EventEnvelope{
		ID:         "evt_catalog",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		EventName:  "cta_click",
		EventTime:  time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 5, 8, 10, 0, 1, 0, time.UTC),
		Properties: map[string]any{
			"button": "hero",
		},
		UserProps: map[string]any{
			"plan": "pro",
		},
	}
}
