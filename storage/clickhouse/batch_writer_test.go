package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/storage"
)

type fakeNativeBatch struct {
	appendErr  error   // appendErr forces Append to fail for rollback tests
	sendErr    error   // sendErr forces Send to fail after a successful append
	abortErr   error   // abortErr lets tests verify joined rollback errors later
	appendRows [][]any // appendRows records rows passed to the native batch
	sendCalls  int     // sendCalls records how often Send was invoked
	abortCalls int     // abortCalls records how often Abort was invoked
}

func (f *fakeNativeBatch) Abort() error {
	f.abortCalls++
	return f.abortErr
}

func (f *fakeNativeBatch) Append(values ...any) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	f.appendRows = append(f.appendRows, append([]any(nil), values...))
	return nil
}

func (f *fakeNativeBatch) Send() error {
	f.sendCalls++
	return f.sendErr
}

type fakeEventWriteGuard struct {
	claim     *fakeEventWriteClaim      // claim is returned to the writer under test
	err       error                     // err forces StartEventWrite to fail
	envelopes []contracts.EventEnvelope // envelopes records claimed events
}

func (f *fakeEventWriteGuard) StartEventWrite(_ context.Context, envelope contracts.EventEnvelope) (storage.EventWriteClaim, error) {
	f.envelopes = append(f.envelopes, envelope)
	if f.err != nil {
		return nil, f.err
	}
	return f.claim, nil
}

type fakeEventWriteClaim struct {
	alreadyInserted bool  // alreadyInserted simulates a previously committed event id
	commitErr       error // commitErr forces Commit to fail after ClickHouse send
	rollbackErr     error // rollbackErr forces Rollback to fail after ClickHouse failure
	commitCalls     int   // commitCalls records successful write finalization attempts
	rollbackCalls   int   // rollbackCalls records failed write cleanup attempts
	rollbackCause   error // rollbackCause captures the ClickHouse failure passed to Rollback
}

func (f *fakeEventWriteClaim) AlreadyInserted() bool {
	return f.alreadyInserted
}

func (f *fakeEventWriteClaim) Commit(context.Context) error {
	f.commitCalls++
	return f.commitErr
}

func (f *fakeEventWriteClaim) Rollback(_ context.Context, cause error) error {
	f.rollbackCalls++
	f.rollbackCause = cause
	return f.rollbackErr
}

func TestBatchWriterWriteEventUsesNativeBatch(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	batch := &fakeNativeBatch{}
	var query string
	writer, err := newBatchWriterWithPreparer(router, func(_ context.Context, insertSQL string) (nativeBatch, error) {
		query = insertSQL
		return batch, nil
	})
	if err != nil {
		t.Fatalf("new batch writer failed: %v", err)
	}

	eventTime := time.Date(2026, 4, 30, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	receivedAt := time.Date(2026, 4, 30, 12, 0, 1, 0, time.FixedZone("CST", 8*60*60))
	result, err := writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "page_view",
		DistinctID: "visitor_1",
		SessionID:  "session_1",
		VisitID:    "visit_1",
		EventTime:  eventTime,
		ReceivedAt: receivedAt,
		Properties: map[string]any{"path": "/"},
		UserProps:  map[string]any{"plan": "free"},
		Source:     "sdk-js",
	})
	if err != nil {
		t.Fatalf("write event failed: %v", err)
	}
	if !result.Inserted {
		t.Fatal("expected inserted result")
	}

	wantTable := "events_" + shortHash("tenant_1") + "_" + shortHash("project_1") + "_" + shortHash("source_1")
	if !strings.HasPrefix(query, "INSERT INTO "+wantTable+" ") {
		t.Fatalf("insert query = %q, want table %q", query, wantTable)
	}
	for _, column := range eventInsertColumns {
		if !strings.Contains(query, "`"+column+"`") {
			t.Fatalf("insert query missing column %q: %s", column, query)
		}
	}
	if batch.sendCalls != 1 {
		t.Fatalf("send calls = %d, want 1", batch.sendCalls)
	}
	if batch.abortCalls != 0 {
		t.Fatalf("abort calls = %d, want 0", batch.abortCalls)
	}

	wantRow := []any{
		"evt_1",
		"tenant_1",
		"project_1",
		"source_1",
		"web",
		"page_view",
		"visitor_1",
		"session_1",
		"visit_1",
		eventTime.UTC(),
		receivedAt.UTC(),
		`{"path":"/"}`,
		`{"plan":"free"}`,
		"sdk-js",
	}
	if !reflect.DeepEqual(batch.appendRows, [][]any{wantRow}) {
		t.Fatalf("appended rows = %#v, want %#v", batch.appendRows, [][]any{wantRow})
	}
}

func TestBatchWriterSkipsAlreadyInsertedEvent(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	prepareCalled := false
	claim := &fakeEventWriteClaim{alreadyInserted: true}
	writer, err := newBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		prepareCalled = true
		return &fakeNativeBatch{}, nil
	}, WithEventWriteGuard(&fakeEventWriteGuard{claim: claim}))
	if err != nil {
		t.Fatalf("new batch writer failed: %v", err)
	}

	result, err := writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:        "evt_1",
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
	})
	if err != nil {
		t.Fatalf("write event failed: %v", err)
	}
	if result.Inserted {
		t.Fatal("duplicate event should report Inserted=false")
	}
	if prepareCalled {
		t.Fatal("duplicate event should not prepare ClickHouse batch")
	}
	if claim.commitCalls != 0 || claim.rollbackCalls != 0 {
		t.Fatalf("duplicate claim commit=%d rollback=%d, want 0/0", claim.commitCalls, claim.rollbackCalls)
	}
}

func TestBatchWriterAbortsAndRollsBackOnAppendFailure(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	appendErr := errors.New("append failed")
	batch := &fakeNativeBatch{appendErr: appendErr}
	claim := &fakeEventWriteClaim{}
	writer, err := newBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return batch, nil
	}, WithEventWriteGuard(&fakeEventWriteGuard{claim: claim}))
	if err != nil {
		t.Fatalf("new batch writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:        "evt_1",
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
	})
	if !errors.Is(err, appendErr) {
		t.Fatalf("write error = %v, want append failed", err)
	}
	if batch.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", batch.abortCalls)
	}
	if batch.sendCalls != 0 {
		t.Fatalf("send calls = %d, want 0", batch.sendCalls)
	}
	if claim.rollbackCalls != 1 || !errors.Is(claim.rollbackCause, appendErr) {
		t.Fatalf("rollback calls/cause = %d/%v, want 1/%v", claim.rollbackCalls, claim.rollbackCause, appendErr)
	}
	if claim.commitCalls != 0 {
		t.Fatalf("commit calls = %d, want 0", claim.commitCalls)
	}
}

func TestBatchWriterAbortsAndRollsBackOnSendFailure(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	sendErr := errors.New("send failed")
	batch := &fakeNativeBatch{sendErr: sendErr}
	claim := &fakeEventWriteClaim{}
	writer, err := newBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return batch, nil
	}, WithEventWriteGuard(&fakeEventWriteGuard{claim: claim}))
	if err != nil {
		t.Fatalf("new batch writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:        "evt_1",
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
	})
	if !errors.Is(err, sendErr) {
		t.Fatalf("write error = %v, want send failed", err)
	}
	if batch.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", batch.abortCalls)
	}
	if batch.sendCalls != 1 {
		t.Fatalf("send calls = %d, want 1", batch.sendCalls)
	}
	if claim.rollbackCalls != 1 || !errors.Is(claim.rollbackCause, sendErr) {
		t.Fatalf("rollback calls/cause = %d/%v, want 1/%v", claim.rollbackCalls, claim.rollbackCause, sendErr)
	}
}

func TestBatchWriterReturnsCommitFailureAfterSend(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	commitErr := errors.New("commit failed")
	batch := &fakeNativeBatch{}
	claim := &fakeEventWriteClaim{commitErr: commitErr}
	writer, err := newBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return batch, nil
	}, WithEventWriteGuard(&fakeEventWriteGuard{claim: claim}))
	if err != nil {
		t.Fatalf("new batch writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:        "evt_1",
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
	})
	if !errors.Is(err, commitErr) {
		t.Fatalf("write error = %v, want commit failed", err)
	}
	if batch.sendCalls != 1 {
		t.Fatalf("send calls = %d, want 1", batch.sendCalls)
	}
	if claim.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", claim.commitCalls)
	}
	if claim.rollbackCalls != 0 {
		t.Fatalf("rollback calls = %d, want 0", claim.rollbackCalls)
	}
}

func TestBatchWriterAbortsOnPropertyMarshalFailure(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	batch := &fakeNativeBatch{}
	writer, err := newBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return batch, nil
	})
	if err != nil {
		t.Fatalf("new batch writer failed: %v", err)
	}

	_, err = writer.WriteEvent(context.Background(), contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		Properties: map[string]any{"bad": func() {}},
	})
	if err == nil || !strings.Contains(err.Error(), "marshal properties") {
		t.Fatalf("write error = %v, want marshal properties error", err)
	}
	if batch.abortCalls != 1 {
		t.Fatalf("abort calls = %d, want 1", batch.abortCalls)
	}
	if batch.sendCalls != 0 {
		t.Fatalf("send calls = %d, want 0", batch.sendCalls)
	}
}

func TestBatchWriterRequiresDependenciesAndEventID(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	if _, err := newBatchWriterWithPreparer(nil, func(context.Context, string) (nativeBatch, error) { return nil, nil }); err == nil {
		t.Fatal("expected missing router error")
	}
	if _, err := newBatchWriterWithPreparer(router, nil); err == nil {
		t.Fatal("expected missing preparer error")
	}

	writer, err := newBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return &fakeNativeBatch{}, nil
	})
	if err != nil {
		t.Fatalf("new batch writer failed: %v", err)
	}
	if _, err := writer.WriteEvent(context.Background(), contracts.EventEnvelope{}); err == nil {
		t.Fatal("expected missing event_id error")
	}
}
