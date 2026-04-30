package collect

import (
	"errors"
	"testing"
	"time"
)

func TestNormalizeReturnsEventEnvelope(t *testing.T) {
	receivedAt := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	eventTime := receivedAt.Add(-time.Minute)

	envelope, err := Normalize(Request{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "page.view",
		DistinctID: "visitor_1",
		SessionID:  "session_1",
		EventTime:  eventTime,
		Properties: map[string]any{"path": "/"},
		UserProps:  map[string]any{"plan": "free"},
		Source:     "browser",
	}, receivedAt)
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}

	if envelope.EventTime != eventTime {
		t.Fatalf("expected event time %s, got %s", eventTime, envelope.EventTime)
	}
	if envelope.ReceivedAt != receivedAt {
		t.Fatalf("expected received time %s, got %s", receivedAt, envelope.ReceivedAt)
	}
	if envelope.Properties["path"] != "/" {
		t.Fatalf("expected property to be copied")
	}
	if envelope.UserProps["plan"] != "free" {
		t.Fatalf("expected user property to be copied")
	}
}

func TestNormalizeDefaultsEventTimeToReceivedAt(t *testing.T) {
	receivedAt := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)

	envelope, err := Normalize(validRequest(), receivedAt)
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}
	if envelope.EventTime != receivedAt {
		t.Fatalf("expected event time to default to receivedAt")
	}
}

func TestNormalizeRejectsUnsupportedEventName(t *testing.T) {
	request := validRequest()
	request.EventName = " checkout completed "

	_, err := Normalize(request, time.Now().UTC())
	if err == nil {
		t.Fatal("expected validation error")
	}

	var validationErr ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if validationErr.Field != "event_name" {
		t.Fatalf("expected event_name error, got %q", validationErr.Field)
	}
}

func TestNormalizeRejectsFutureEventTime(t *testing.T) {
	receivedAt := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	request := validRequest()
	request.EventTime = receivedAt.Add(6 * time.Minute)

	_, err := Normalize(request, receivedAt)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func validRequest() Request {
	return Request{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "pageview",
		DistinctID: "visitor_1",
	}
}
