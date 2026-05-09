package collect

import (
	"errors"
	"fmt"
	"strings"
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
		VisitID:    "visit_1",
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
	if envelope.VisitID != "visit_1" {
		t.Fatalf("expected visit id to be copied")
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

func TestValidateEventNameContractSamples(t *testing.T) {
	accepted := []string{
		"pageview",
		"page.view",
		"checkout.completed",
		"button:click",
		"signup-started",
		"event_123",
		"1st_step",
		"PageView",
		strings.Repeat("a", maxEventNameLength),
	}

	for _, value := range accepted {
		if err := ValidateEventName(value); err != nil {
			t.Fatalf("expected %q to satisfy the canonical event-name contract: %v", value, err)
		}
	}

	rejected := []string{
		"",
		"_pageview",
		".pageview",
		":pageview",
		"-pageview",
		"checkout completed",
		"/pageview",
		"$pageview",
		"event#name",
		strings.Repeat("a", maxEventNameLength+1),
	}

	for _, value := range rejected {
		if err := ValidateEventName(value); err == nil {
			t.Fatalf("expected %q to fail the canonical event-name contract", value)
		}
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

func TestNormalizeRejectsInvalidPropertyKey(t *testing.T) {
	request := validRequest()
	request.Properties = map[string]any{"bad key": "value"}

	_, err := Normalize(request, time.Now().UTC())
	if err == nil {
		t.Fatal("expected validation error")
	}

	var validationErr ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if validationErr.Field != "properties.bad key" {
		t.Fatalf("expected properties.bad key error, got %q", validationErr.Field)
	}
}

func TestNormalizeRejectsUnsupportedPropertyValue(t *testing.T) {
	request := validRequest()
	request.Properties = map[string]any{"nested": map[string]any{"path": "/"}}

	_, err := Normalize(request, time.Now().UTC())
	if err == nil {
		t.Fatal("expected validation error")
	}

	var validationErr ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if validationErr.Field != "properties.nested" {
		t.Fatalf("expected properties.nested error, got %q", validationErr.Field)
	}
}

func TestNormalizeRejectsTooManyUserProperties(t *testing.T) {
	request := validRequest()
	request.UserProps = make(map[string]any, maxPropertyCount+1)
	for idx := 0; idx <= maxPropertyCount; idx++ {
		request.UserProps[fmt.Sprintf("prop_%d", idx)] = "value"
	}

	_, err := Normalize(request, time.Now().UTC())
	if err == nil {
		t.Fatal("expected validation error")
	}

	var validationErr ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if validationErr.Field != "user_properties" {
		t.Fatalf("expected user_properties error, got %q", validationErr.Field)
	}
}

func TestNormalizeRejectsLongPropertyString(t *testing.T) {
	request := validRequest()
	request.Properties = map[string]any{"label": strings.Repeat("x", maxPropertyStrLen+1)}

	_, err := Normalize(request, time.Now().UTC())
	if err == nil {
		t.Fatal("expected validation error")
	}

	var validationErr ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected ValidationError, got %T", err)
	}
	if validationErr.Field != "properties.label" {
		t.Fatalf("expected properties.label error, got %q", validationErr.Field)
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
