package collect

import (
	"context"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

// benchmarkCollectSink keeps the latest envelope reachable so the compiler
// cannot discard the collect handler result during benchmark optimization.
var benchmarkCollectSink contracts.EventEnvelope

// BenchmarkHandlerHandleNormalizePublish measures the framework-neutral collect
// hot path before optional enrichment stages.
func BenchmarkHandlerHandleNormalizePublish(b *testing.B) {
	bus := &benchmarkBus{}
	handler := newBenchmarkHandler(b, bus)
	ctx := context.Background()
	request := benchmarkRequest()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		// Measure only the normalized collect boundary: validate, clone bounded
		// properties, publish into the EventBus abstraction, and return envelope.
		envelope, err := handler.Handle(ctx, request)
		if err != nil {
			b.Fatalf("handle failed: %v", err)
		}
		benchmarkCollectSink = envelope
	}
	b.StopTimer()

	bus.requirePublished(b, b.N)
}

// BenchmarkHandlerHandleIdentityStages measures collect with deterministic
// session and visit derivation enabled.
func BenchmarkHandlerHandleIdentityStages(b *testing.B) {
	bus := &benchmarkBus{}
	sessionStage, err := NewSessionResolverStage(SessionResolverConfig{
		Salt:   "benchmark-session-salt",
		Window: 30 * time.Minute,
	})
	if err != nil {
		b.Fatalf("new session resolver failed: %v", err)
	}
	visitStage, err := NewVisitResolverStage(VisitResolverConfig{
		Salt:   "benchmark-visit-salt",
		Window: 30 * time.Minute,
	})
	if err != nil {
		b.Fatalf("new visit resolver failed: %v", err)
	}
	handler := newBenchmarkHandler(b, bus, sessionStage, visitStage)
	ctx := context.Background()
	request := benchmarkRequest()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		// Identity stages run after protocol validation and before queue publish,
		// so this benchmark captures the long-term visit/session write contract.
		envelope, err := handler.Handle(ctx, request)
		if err != nil {
			b.Fatalf("handle failed: %v", err)
		}
		benchmarkCollectSink = envelope
	}
	b.StopTimer()

	bus.requirePublished(b, b.N)
	requireIdentityDerived(b, bus.last)
}

// BenchmarkHandlerHandleIdentityAndClientEnrichment measures collect with
// identity derivation plus browser, OS, device, referrer, and IP hash enrichment.
func BenchmarkHandlerHandleIdentityAndClientEnrichment(b *testing.B) {
	bus := &benchmarkBus{}
	sessionStage, err := NewSessionResolverStage(SessionResolverConfig{
		Salt:                     "benchmark-session-salt",
		Window:                   30 * time.Minute,
		IncludeClientFingerprint: true,
	})
	if err != nil {
		b.Fatalf("new session resolver failed: %v", err)
	}
	visitStage, err := NewVisitResolverStage(VisitResolverConfig{
		Salt:   "benchmark-visit-salt",
		Window: 30 * time.Minute,
	})
	if err != nil {
		b.Fatalf("new visit resolver failed: %v", err)
	}
	clientStage, err := NewClientEnrichmentStage(ClientEnrichmentConfig{
		HashSalt:           "benchmark-client-salt",
		IncludeUserAgent:   true,
		IncludeIPHash:      true,
		IncludeReferrer:    true,
		IncludeBrowserInfo: true,
	})
	if err != nil {
		b.Fatalf("new client enrichment failed: %v", err)
	}
	handler := newBenchmarkHandler(b, bus, sessionStage, visitStage, clientStage)
	ctx := context.Background()
	request := benchmarkRequest()
	request.Client = ClientInfo{
		UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
		IP:        "203.0.113.10",
		Referrer:  "https://example.com/docs",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		// Client enrichment should stay bounded and framework-neutral; this
		// benchmark measures derived properties without a real HTTP adapter.
		envelope, err := handler.Handle(ctx, request)
		if err != nil {
			b.Fatalf("handle failed: %v", err)
		}
		benchmarkCollectSink = envelope
	}
	b.StopTimer()

	bus.requirePublished(b, b.N)
	requireIdentityDerived(b, bus.last)
	requireClientEnrichmentDerived(b, bus.last)
}

// benchmarkBus is a count-only EventBus for isolating collect handler cost.
type benchmarkBus struct {
	published int                     // published counts envelopes accepted by collect.Handler
	last      contracts.EventEnvelope // last keeps the publish argument reachable
}

// Publish records that collect reached the durable queue boundary.
func (b *benchmarkBus) Publish(_ context.Context, envelope contracts.EventEnvelope) error {
	b.published++
	b.last = envelope
	return nil
}

// Subscribe exists only to satisfy eventbus.EventBus for handler benchmarks.
func (b *benchmarkBus) Subscribe(context.Context, eventbus.ConsumerGroup, eventbus.Handler) error {
	return nil
}

// requirePublished verifies that benchmark timing did not skip handler work.
func (b *benchmarkBus) requirePublished(tb testing.TB, expected int) {
	tb.Helper()
	if b.published != expected {
		tb.Fatalf("expected %d published events, got %d", expected, b.published)
	}
	if b.last.ID == "" {
		tb.Fatalf("expected last published envelope to be retained")
	}
}

// requireIdentityDerived confirms identity stages stayed inside the measured path.
func requireIdentityDerived(tb testing.TB, envelope contracts.EventEnvelope) {
	tb.Helper()
	if envelope.SessionID == "" {
		tb.Fatalf("expected benchmark envelope to include derived session id")
	}
	if envelope.VisitID == "" {
		tb.Fatalf("expected benchmark envelope to include derived visit id")
	}
}

// requireClientEnrichmentDerived confirms client enrichment stayed inside the measured path.
func requireClientEnrichmentDerived(tb testing.TB, envelope contracts.EventEnvelope) {
	tb.Helper()
	properties := envelope.Properties
	if properties[clientBrowserProperty] != "Chrome" {
		tb.Fatalf("expected browser enrichment, got %#v", properties[clientBrowserProperty])
	}
	if properties[clientOSProperty] != "Windows" {
		tb.Fatalf("expected os enrichment, got %#v", properties[clientOSProperty])
	}
	if properties[clientDeviceProperty] != "desktop" {
		tb.Fatalf("expected device enrichment, got %#v", properties[clientDeviceProperty])
	}
	if properties[clientReferrerProperty] != "https://example.com/docs" {
		tb.Fatalf("expected referrer enrichment, got %#v", properties[clientReferrerProperty])
	}
	if value, _ := properties[clientIPHashProperty].(string); value == "" {
		tb.Fatalf("expected ip hash enrichment")
	}
}

// newBenchmarkHandler constructs a handler outside the measured benchmark body.
func newBenchmarkHandler(tb testing.TB, bus eventbus.EventBus, stages ...Stage) *Handler {
	tb.Helper()
	handler, err := NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	}, WithStages(stages...))
	if err != nil {
		tb.Fatalf("new handler failed: %v", err)
	}
	return handler
}

// benchmarkRequest returns a representative web pageview request with bounded scalar properties.
func benchmarkRequest() Request {
	request := validRequest()
	request.EventTime = time.Date(2026, 5, 8, 9, 59, 30, 0, time.UTC)
	request.Properties = map[string]any{
		"path":     "/docs/quickstart",
		"title":    "Quickstart",
		"referrer": "https://example.com/",
		"utm":      "launch",
		"depth":    int64(2),
	}
	request.UserProps = map[string]any{
		"plan":   "free",
		"region": "apac",
	}
	return request
}
