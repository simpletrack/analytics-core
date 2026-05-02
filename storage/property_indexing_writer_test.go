package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
)

func TestPropertyIndexingEventWriterWritesEventThenProperties(t *testing.T) {
	events := &recordingEventWriter{result: WriteResult{Inserted: true}}
	properties := &recordingPropertyWriter{}
	guard := newRecordingPropertyGuard(false)
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	result, err := writer.WriteEvent(context.Background(), eventWithProperties())
	if err != nil {
		t.Fatalf("write event failed: %v", err)
	}
	if !result.Inserted {
		t.Fatal("expected inserted event result")
	}
	if got := len(events.envelopes); got != 1 {
		t.Fatalf("event writes = %d, want 1", got)
	}
	if got := len(properties.records); got != 2 {
		t.Fatalf("property rows = %d, want 2: %#v", got, properties.records)
	}
	if properties.records[0].Scope != PropertyScopeEvent || properties.records[0].Name != "button" {
		t.Fatalf("first property row = %#v, want event.button", properties.records[0])
	}
	if properties.records[1].Scope != PropertyScopeUser || properties.records[1].Name != "plan" {
		t.Fatalf("second property row = %#v, want user.plan", properties.records[1])
	}
	if guard.claims != 1 || guard.claim.commitCalls != 1 || guard.claim.rollbackCalls != 0 {
		t.Fatalf("property guard claims/commit/rollback = %d/%d/%d, want 1/1/0", guard.claims, guard.claim.commitCalls, guard.claim.rollbackCalls)
	}
}

func TestPropertyIndexingEventWriterRepairsPropertiesForDuplicateEvent(t *testing.T) {
	events := &recordingEventWriter{result: WriteResult{Inserted: false}}
	properties := &recordingPropertyWriter{}
	guard := newRecordingPropertyGuard(false)
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	result, err := writer.WriteEvent(context.Background(), eventWithProperties())
	if err != nil {
		t.Fatalf("write duplicate event failed: %v", err)
	}
	if result.Inserted {
		t.Fatal("duplicate primary write should keep Inserted=false")
	}
	if got := len(properties.records); got != 2 {
		t.Fatalf("duplicate repair property rows = %d, want 2", got)
	}
	if guard.claim.commitCalls != 1 {
		t.Fatalf("property guard commits = %d, want 1", guard.claim.commitCalls)
	}
}

func TestPropertyIndexingEventWriterSkipsAlreadyIndexedProperties(t *testing.T) {
	events := &recordingEventWriter{result: WriteResult{Inserted: false}}
	properties := &recordingPropertyWriter{}
	guard := newRecordingPropertyGuard(true)
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	result, err := writer.WriteEvent(context.Background(), eventWithProperties())
	if err != nil {
		t.Fatalf("write duplicate event failed: %v", err)
	}
	if result.Inserted {
		t.Fatal("duplicate primary write should keep Inserted=false")
	}
	if got := len(properties.records); got != 0 {
		t.Fatalf("already indexed duplicate property rows = %d, want 0", got)
	}
	if guard.claim.commitCalls != 0 || guard.claim.rollbackCalls != 0 {
		t.Fatalf("already indexed commit/rollback = %d/%d, want 0/0", guard.claim.commitCalls, guard.claim.rollbackCalls)
	}
}

func TestPropertyIndexingEventWriterSkipsPropertiesWhenEnvelopeHasNone(t *testing.T) {
	events := &recordingEventWriter{result: WriteResult{Inserted: true}}
	properties := &recordingPropertyWriter{}
	guard := newRecordingPropertyGuard(false)
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:         "evt_empty",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		EventTime:  time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 5, 2, 8, 0, 1, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("write event without properties failed: %v", err)
	}
	if got := len(events.envelopes); got != 1 {
		t.Fatalf("event writes = %d, want 1", got)
	}
	if got := len(properties.records); got != 0 {
		t.Fatalf("property rows = %d, want 0", got)
	}
	if guard.claims != 0 {
		t.Fatalf("property guard claims = %d, want 0", guard.claims)
	}
}

func TestPropertyIndexingEventWriterReturnsPrimaryWriteError(t *testing.T) {
	writeErr := errors.New("clickhouse unavailable")
	events := &recordingEventWriter{err: writeErr}
	properties := &recordingPropertyWriter{}
	guard := newRecordingPropertyGuard(false)
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), eventWithProperties())
	if !errors.Is(err, writeErr) {
		t.Fatalf("write error = %v, want primary error", err)
	}
	if got := len(properties.records); got != 0 {
		t.Fatalf("property rows = %d, want 0", got)
	}
	if guard.claims != 0 {
		t.Fatalf("property guard claims = %d, want 0", guard.claims)
	}
}

func TestPropertyIndexingEventWriterReturnsPropertyWriteErrorForRetry(t *testing.T) {
	propertyErr := errors.New("property table unavailable")
	events := &recordingEventWriter{result: WriteResult{Inserted: true}}
	properties := &recordingPropertyWriter{err: propertyErr}
	guard := newRecordingPropertyGuard(false)
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), eventWithProperties())
	if !errors.Is(err, propertyErr) {
		t.Fatalf("write error = %v, want property error", err)
	}
	if !strings.Contains(err.Error(), "write event properties") {
		t.Fatalf("write error = %v, want property context", err)
	}
	if guard.claim.rollbackCalls != 1 || guard.claim.commitCalls != 0 {
		t.Fatalf("property guard commit/rollback = %d/%d, want 0/1", guard.claim.commitCalls, guard.claim.rollbackCalls)
	}
}

func TestPropertyIndexingEventWriterReturnsPropertyClaimError(t *testing.T) {
	claimErr := errors.New("property claim unavailable")
	events := &recordingEventWriter{result: WriteResult{Inserted: true}}
	properties := &recordingPropertyWriter{}
	guard := newRecordingPropertyGuard(false)
	guard.err = claimErr
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), eventWithProperties())
	if !errors.Is(err, claimErr) {
		t.Fatalf("write error = %v, want claim error", err)
	}
	if got := len(properties.records); got != 0 {
		t.Fatalf("property rows = %d, want 0", got)
	}
}

func TestPropertyIndexingEventWriterReturnsPropertyCommitError(t *testing.T) {
	commitErr := errors.New("property commit unavailable")
	events := &recordingEventWriter{result: WriteResult{Inserted: true}}
	properties := &recordingPropertyWriter{}
	guard := newRecordingPropertyGuard(false)
	guard.claim.commitErr = commitErr
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), eventWithProperties())
	if !errors.Is(err, commitErr) {
		t.Fatalf("write error = %v, want commit error", err)
	}
	if got := len(properties.records); got != 2 {
		t.Fatalf("property rows = %d, want 2 before commit failure", got)
	}
	if guard.claim.rollbackCalls != 0 {
		t.Fatalf("commit failure must not roll back ambiguous property claim; rollback calls = %d", guard.claim.rollbackCalls)
	}
}

func TestPropertyIndexingEventWriterRejectsUnflattenablePropertiesBeforeEventWrite(t *testing.T) {
	events := &recordingEventWriter{result: WriteResult{Inserted: true}}
	properties := &recordingPropertyWriter{}
	guard := newRecordingPropertyGuard(false)
	writer, err := NewPropertyIndexingEventWriter(events, properties, guard)
	if err != nil {
		t.Fatalf("new property indexing writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:         "evt_bad_property",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		Properties: map[string]any{"nested": map[string]any{"path": "/"}},
	})
	if err == nil || !strings.Contains(err.Error(), "property event.nested has unsupported value type") {
		t.Fatalf("write error = %v, want flatten error", err)
	}
	if got := len(events.envelopes); got != 0 {
		t.Fatalf("event writes = %d, want 0", got)
	}
}

func TestNewPropertyIndexingEventWriterRequiresDependencies(t *testing.T) {
	if _, err := NewPropertyIndexingEventWriter(nil, &recordingPropertyWriter{}, newRecordingPropertyGuard(false)); err == nil {
		t.Fatal("expected missing event writer error")
	}
	if _, err := NewPropertyIndexingEventWriter(&recordingEventWriter{}, nil, newRecordingPropertyGuard(false)); err == nil {
		t.Fatal("expected missing property writer error")
	}
	if _, err := NewPropertyIndexingEventWriter(&recordingEventWriter{}, &recordingPropertyWriter{}, nil); err == nil {
		t.Fatal("expected missing property guard error")
	}
}

type recordingEventWriter struct {
	result    WriteResult               // result is returned from WriteEvent when err is nil
	err       error                     // err forces the primary event write to fail
	envelopes []contracts.EventEnvelope // envelopes records primary event writes
}

func (w *recordingEventWriter) WriteEvent(_ context.Context, envelope contracts.EventEnvelope) (WriteResult, error) {
	w.envelopes = append(w.envelopes, envelope)
	if w.err != nil {
		return WriteResult{}, w.err
	}
	return w.result, nil
}

type recordingPropertyWriter struct {
	err     error                 // err forces property indexing to fail
	records []EventPropertyRecord // records stores flattened property rows passed to the writer
}

func (w *recordingPropertyWriter) WriteEventProperties(_ context.Context, records []EventPropertyRecord) (PropertyWriteResult, error) {
	if w.err != nil {
		return PropertyWriteResult{}, w.err
	}
	w.records = append(w.records, records...)
	return PropertyWriteResult{Rows: len(records)}, nil
}

type recordingPropertyGuard struct {
	err    error                        // err forces property claim creation to fail
	claim  *recordingPropertyWriteClaim // claim is returned to the writer under test
	claims int                          // claims records how often property indexing was claimed
	events []contracts.EventEnvelope    // events records envelopes passed to the guard
}

func newRecordingPropertyGuard(alreadyInserted bool) *recordingPropertyGuard {
	return &recordingPropertyGuard{claim: &recordingPropertyWriteClaim{alreadyInserted: alreadyInserted}}
}

func (g *recordingPropertyGuard) StartPropertyWrite(_ context.Context, envelope contracts.EventEnvelope) (PropertyWriteClaim, error) {
	g.claims++
	g.events = append(g.events, envelope)
	if g.err != nil {
		return nil, g.err
	}
	return g.claim, nil
}

type recordingPropertyWriteClaim struct {
	alreadyInserted bool  // alreadyInserted simulates a completed property index
	commitErr       error // commitErr forces Commit to fail after property batch send
	rollbackErr     error // rollbackErr forces Rollback to fail after property batch failure
	commitCalls     int   // commitCalls records successful property finalization attempts
	rollbackCalls   int   // rollbackCalls records failed property cleanup attempts
	rollbackCause   error // rollbackCause captures the property batch failure passed to Rollback
}

func (c *recordingPropertyWriteClaim) AlreadyInserted() bool {
	return c.alreadyInserted
}

func (c *recordingPropertyWriteClaim) Commit(context.Context) error {
	c.commitCalls++
	return c.commitErr
}

func (c *recordingPropertyWriteClaim) Rollback(_ context.Context, cause error) error {
	c.rollbackCalls++
	c.rollbackCause = cause
	return c.rollbackErr
}

func eventWithProperties() contracts.EventEnvelope {
	return contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "signup_clicked",
		DistinctID: "visitor_1",
		SessionID:  "session_1",
		EventTime:  time.Date(2026, 5, 2, 8, 0, 0, 0, time.UTC),
		ReceivedAt: time.Date(2026, 5, 2, 8, 0, 1, 0, time.UTC),
		Properties: map[string]any{"button": "hero"},
		UserProps:  map[string]any{"plan": "free"},
		Source:     "sdk-js",
	}
}
