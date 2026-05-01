package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/internal/storage"
)

func TestPropertyBatchWriterWritesNativeBatch(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	batch := &fakeNativeBatch{}
	var query string
	writer, err := newPropertyBatchWriterWithPreparer(router, func(_ context.Context, insertSQL string) (nativeBatch, error) {
		query = insertSQL
		return batch, nil
	})
	if err != nil {
		t.Fatalf("new property writer failed: %v", err)
	}

	eventTime := time.Date(2026, 5, 1, 8, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	receivedAt := eventTime.Add(time.Second)
	records := []storage.EventPropertyRecord{
		validPropertyRecord(eventTime, receivedAt, storage.PropertyScopeEvent, "button"),
		validPropertyRecord(eventTime, receivedAt, storage.PropertyScopeUser, "plan"),
	}
	records[0].ValueType = storage.PropertyValueString
	records[0].StringValue = "hero"
	records[1].ValueType = storage.PropertyValueBool
	records[1].BoolValue = true

	result, err := writer.WriteEventProperties(context.Background(), records)
	if err != nil {
		t.Fatalf("write properties failed: %v", err)
	}

	if result.Rows != 2 {
		t.Fatalf("rows = %d, want 2", result.Rows)
	}
	wantTable := "events_" + shortHash("tenant_1") + "_" + shortHash("project_1") + "_" + shortHash("source_1") + "_properties"
	if !strings.HasPrefix(query, "INSERT INTO "+wantTable+" ") {
		t.Fatalf("insert query = %q, want table %q", query, wantTable)
	}
	for _, column := range propertyInsertColumns {
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

	wantRows := [][]any{
		{
			"evt_1",
			"tenant_1",
			"project_1",
			"source_1",
			"web",
			"signup",
			"visitor_1",
			"session_1",
			eventTime.UTC(),
			receivedAt.UTC(),
			"browser",
			"event",
			"button",
			"string",
			"hero",
			float64(0),
			false,
		},
		{
			"evt_1",
			"tenant_1",
			"project_1",
			"source_1",
			"web",
			"signup",
			"visitor_1",
			"session_1",
			eventTime.UTC(),
			receivedAt.UTC(),
			"browser",
			"user",
			"plan",
			"bool",
			"",
			float64(0),
			true,
		},
	}
	if !reflect.DeepEqual(batch.appendRows, wantRows) {
		t.Fatalf("appended rows = %#v, want %#v", batch.appendRows, wantRows)
	}
}

func TestPropertyBatchWriterSkipsEmptyRecords(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	prepareCalled := false
	writer, err := newPropertyBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		prepareCalled = true
		return &fakeNativeBatch{}, nil
	})
	if err != nil {
		t.Fatalf("new property writer failed: %v", err)
	}

	result, err := writer.WriteEventProperties(context.Background(), nil)
	if err != nil {
		t.Fatalf("write empty properties failed: %v", err)
	}
	if result.Rows != 0 {
		t.Fatalf("rows = %d, want 0", result.Rows)
	}
	if prepareCalled {
		t.Fatal("empty property write should not prepare ClickHouse batch")
	}
}

func TestPropertyBatchWriterReturnsRoutingError(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	writer, err := newPropertyBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return &fakeNativeBatch{}, nil
	})
	if err != nil {
		t.Fatalf("new property writer failed: %v", err)
	}

	_, err = writer.WriteEventProperties(context.Background(), []storage.EventPropertyRecord{
		{
			EventID:   "evt_1",
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			Scope:     storage.PropertyScopeEvent,
			Name:      "button",
			ValueType: storage.PropertyValueString,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "source_id is required") {
		t.Fatalf("error = %v, want missing source id", err)
	}
}

func TestPropertyBatchWriterRejectsInvalidRecord(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	writer, err := newPropertyBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return &fakeNativeBatch{}, nil
	})
	if err != nil {
		t.Fatalf("new property writer failed: %v", err)
	}

	_, err = writer.WriteEventProperties(context.Background(), []storage.EventPropertyRecord{
		{
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			SourceID:  "source_1",
			Scope:     storage.PropertyScopeEvent,
			Name:      "button",
			ValueType: storage.PropertyValueString,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "event_id is required") {
		t.Fatalf("error = %v, want missing event_id", err)
	}
}

func TestPropertyBatchWriterAbortsOnAppendFailure(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	appendErr := errors.New("append failed")
	batch := &fakeNativeBatch{appendErr: appendErr}
	writer, err := newPropertyBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return batch, nil
	})
	if err != nil {
		t.Fatalf("new property writer failed: %v", err)
	}

	_, err = writer.WriteEventProperties(context.Background(), []storage.EventPropertyRecord{
		validPropertyRecord(time.Now().UTC(), time.Now().UTC(), storage.PropertyScopeEvent, "button"),
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
}

func TestPropertyBatchWriterAbortsOnSendFailure(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	sendErr := errors.New("send failed")
	batch := &fakeNativeBatch{sendErr: sendErr}
	writer, err := newPropertyBatchWriterWithPreparer(router, func(context.Context, string) (nativeBatch, error) {
		return batch, nil
	})
	if err != nil {
		t.Fatalf("new property writer failed: %v", err)
	}

	_, err = writer.WriteEventProperties(context.Background(), []storage.EventPropertyRecord{
		validPropertyRecord(time.Now().UTC(), time.Now().UTC(), storage.PropertyScopeEvent, "button"),
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
}

func TestPropertyBatchWriterRequiresDependencies(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	if _, err := NewPropertyBatchWriter(nil, router); err == nil {
		t.Fatal("expected missing connection error")
	}
	if _, err := newPropertyBatchWriterWithPreparer(nil, func(context.Context, string) (nativeBatch, error) { return nil, nil }); err == nil {
		t.Fatal("expected missing router error")
	}
	if _, err := newPropertyBatchWriterWithPreparer(router, nil); err == nil {
		t.Fatal("expected missing preparer error")
	}
}

func validPropertyRecord(eventTime time.Time, receivedAt time.Time, scope storage.PropertyScope, name string) storage.EventPropertyRecord {
	return storage.EventPropertyRecord{
		EventID:    "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "signup",
		DistinctID: "visitor_1",
		SessionID:  "session_1",
		EventTime:  eventTime,
		ReceivedAt: receivedAt,
		Source:     "browser",
		Scope:      scope,
		Name:       name,
		ValueType:  storage.PropertyValueString,
	}
}
