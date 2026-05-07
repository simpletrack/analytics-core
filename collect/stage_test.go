package collect

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHandlerDerivesSessionIDWhenMissing(t *testing.T) {
	bus := newRecordingBus()
	now := time.Date(2026, 5, 3, 10, 5, 0, 0, time.UTC)
	resolver, err := NewSessionResolverStage(SessionResolverConfig{
		Salt:   "test-salt",
		Window: 30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("new session resolver failed: %v", err)
	}
	handler, err := NewHandlerWithOptions(bus, func() time.Time { return now }, WithStages(resolver))
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}

	envelope, err := handler.Handle(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if !strings.HasPrefix(envelope.SessionID, "ses_") {
		t.Fatalf("expected derived session id, got %q", envelope.SessionID)
	}
	if len(bus.published) != 1 || bus.published[0].SessionID != envelope.SessionID {
		t.Fatalf("expected derived session id to be published")
	}
}

func TestSessionResolverPreservesExplicitSessionID(t *testing.T) {
	bus := newRecordingBus()
	resolver, err := NewSessionResolverStage(SessionResolverConfig{Salt: "test-salt"})
	if err != nil {
		t.Fatalf("new session resolver failed: %v", err)
	}
	handler, err := NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	}, WithStages(resolver))
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}
	request := validRequest()
	request.SessionID = "sdk_session_1"

	envelope, err := handler.Handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if envelope.SessionID != "sdk_session_1" {
		t.Fatalf("expected explicit session id to win, got %q", envelope.SessionID)
	}
}

func TestHandlerDerivesVisitIDWhenMissing(t *testing.T) {
	bus := newRecordingBus()
	now := time.Date(2026, 5, 3, 10, 5, 0, 0, time.UTC)
	sessionResolver, err := NewSessionResolverStage(SessionResolverConfig{
		Salt:   "session-salt",
		Window: 30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("new session resolver failed: %v", err)
	}
	visitResolver, err := NewVisitResolverStage(VisitResolverConfig{
		Salt:   "visit-salt",
		Window: 30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("new visit resolver failed: %v", err)
	}
	handler, err := NewHandlerWithOptions(bus, func() time.Time { return now }, WithStages(sessionResolver, visitResolver))
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}

	envelope, err := handler.Handle(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if !strings.HasPrefix(envelope.VisitID, "vis_") {
		t.Fatalf("expected derived visit id, got %q", envelope.VisitID)
	}
	if len(bus.published) != 1 || bus.published[0].VisitID != envelope.VisitID {
		t.Fatalf("expected derived visit id to be published")
	}
}

func TestVisitResolverPreservesExplicitVisitID(t *testing.T) {
	bus := newRecordingBus()
	resolver, err := NewVisitResolverStage(VisitResolverConfig{Salt: "visit-salt"})
	if err != nil {
		t.Fatalf("new visit resolver failed: %v", err)
	}
	handler, err := NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	}, WithStages(resolver))
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}
	request := validRequest()
	request.VisitID = "sdk_visit_1"

	envelope, err := handler.Handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if envelope.VisitID != "sdk_visit_1" {
		t.Fatalf("expected explicit visit id to win, got %q", envelope.VisitID)
	}
}

func TestClientEnrichmentAddsDerivedProperties(t *testing.T) {
	bus := newRecordingBus()
	stage, err := NewClientEnrichmentStage(ClientEnrichmentConfig{
		HashSalt:         "hash-salt",
		IncludeUserAgent: true,
		IncludeIPHash:    true,
		IncludeReferrer:  true,
	})
	if err != nil {
		t.Fatalf("new client enrichment stage failed: %v", err)
	}
	handler, err := NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	}, WithStages(stage))
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}
	request := validRequest()
	request.Client = ClientInfo{
		UserAgent: "Mozilla/5.0",
		IP:        "203.0.113.10",
		Referrer:  "https://example.com/docs",
	}

	envelope, err := handler.Handle(context.Background(), request)
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if envelope.Properties[clientUserAgentProperty] != "Mozilla/5.0" {
		t.Fatalf("expected user agent property, got %#v", envelope.Properties[clientUserAgentProperty])
	}
	if envelope.Properties[clientReferrerProperty] != "https://example.com/docs" {
		t.Fatalf("expected referrer property, got %#v", envelope.Properties[clientReferrerProperty])
	}
	ipHash, _ := envelope.Properties[clientIPHashProperty].(string)
	if !strings.HasPrefix(ipHash, "ip_") || strings.Contains(ipHash, "203.0.113.10") {
		t.Fatalf("expected salted IP hash without raw IP, got %q", ipHash)
	}
}

func TestClientEnrichmentDoesNotRejectFullPropertyBag(t *testing.T) {
	stage, err := NewClientEnrichmentStage(ClientEnrichmentConfig{
		HashSalt:      "hash-salt",
		IncludeIPHash: true,
	})
	if err != nil {
		t.Fatalf("new client enrichment stage failed: %v", err)
	}
	envelope, err := Normalize(validRequestWithProperties(maxPropertyCount), time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}

	envelope, err = stage.Apply(context.Background(), StageInput{
		Request: Request{Client: ClientInfo{IP: "203.0.113.10"}},
	}, envelope)
	if err != nil {
		t.Fatalf("stage apply failed: %v", err)
	}

	if len(envelope.Properties) != maxPropertyCount {
		t.Fatalf("expected full property bag to stay bounded, got %d", len(envelope.Properties))
	}
	if _, exists := envelope.Properties[clientIPHashProperty]; exists {
		t.Fatalf("expected generated property to be skipped when bag is full")
	}
}

func TestTrafficFilterDropsBotBeforePublish(t *testing.T) {
	bus := newRecordingBus()
	stage, err := NewTrafficFilterStage(TrafficFilterConfig{})
	if err != nil {
		t.Fatalf("new traffic filter stage failed: %v", err)
	}
	handler, err := NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	}, WithStages(stage))
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}
	request := validRequest()
	request.Client = ClientInfo{UserAgent: "Googlebot/2.1"}

	envelope, err := handler.Handle(context.Background(), request)
	if err == nil {
		t.Fatal("expected filtered error")
	}

	var filteredErr FilteredError
	if !errors.As(err, &filteredErr) {
		t.Fatalf("expected FilteredError, got %T", err)
	}
	if envelope.ID != "evt_1" || filteredErr.Envelope.ID != "evt_1" {
		t.Fatalf("expected filtered envelope to be returned")
	}
	if len(bus.published) != 0 {
		t.Fatalf("filtered traffic should not publish events")
	}
}

func TestTrafficFilterDropsInternalCIDR(t *testing.T) {
	stage, err := NewTrafficFilterStage(TrafficFilterConfig{
		InternalCIDRs: []string{"10.0.0.0/8"},
	})
	if err != nil {
		t.Fatalf("new traffic filter stage failed: %v", err)
	}
	envelope, err := Normalize(validRequest(), time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("normalize failed: %v", err)
	}

	_, err = stage.Apply(context.Background(), StageInput{
		Request: Request{Client: ClientInfo{IP: "10.1.2.3"}},
	}, envelope)
	if err == nil {
		t.Fatal("expected filtered error")
	}

	var filteredErr FilteredError
	if !errors.As(err, &filteredErr) || filteredErr.Reason != "internal ip" {
		t.Fatalf("expected internal ip filter, got %v", err)
	}
}

func TestStageConstructorsRejectInvalidConfig(t *testing.T) {
	if _, err := NewSessionResolverStage(SessionResolverConfig{}); err == nil {
		t.Fatal("expected session resolver to require salt")
	}
	if _, err := NewVisitResolverStage(VisitResolverConfig{}); err == nil {
		t.Fatal("expected visit resolver to require salt")
	}
	if _, err := NewClientEnrichmentStage(ClientEnrichmentConfig{IncludeIPHash: true}); err == nil {
		t.Fatal("expected client enrichment to require hash salt")
	}
	if _, err := NewTrafficFilterStage(TrafficFilterConfig{InternalCIDRs: []string{"not-a-cidr"}}); err == nil {
		t.Fatal("expected traffic filter to reject bad CIDR")
	}
	if _, err := NewHandlerWithOptions(newRecordingBus(), time.Now, WithStages(nil)); err == nil {
		t.Fatal("expected nil stage to be rejected")
	}
}

func validRequestWithProperties(count int) Request {
	request := validRequest()
	request.Properties = make(map[string]any, count)
	for idx := 0; idx < count; idx++ {
		request.Properties["prop_"+strconv.Itoa(idx)] = "value"
	}
	return request
}
