package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/collect"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/valyala/fasthttp"
)

func TestCollectRouteAcceptsEvent(t *testing.T) {
	handler, bus := newTestHandler(t, nil)

	ctx := serveCollect(handler, fasthttp.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1",
		"properties":{"path":"/"}
	}`)

	if ctx.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", fasthttp.StatusAccepted, ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(string(ctx.Response.Body()), `"id":"evt_1"`) {
		t.Fatalf("expected accepted event id, got %s", ctx.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(bus.published))
	}
}

func TestCollectRouteRejectsInvalidJSON(t *testing.T) {
	handler, bus := newTestHandler(t, nil)

	ctx := serveCollect(handler, fasthttp.MethodPost, "/collect", `{`)

	if ctx.Response.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", fasthttp.StatusBadRequest, ctx.Response.StatusCode())
	}
	if len(bus.published) != 0 {
		t.Fatalf("invalid json should not publish events")
	}
}

func TestCollectRouteReturnsValidationError(t *testing.T) {
	handler, bus := newTestHandler(t, nil)

	ctx := serveCollect(handler, fasthttp.MethodPost, "/collect", `{"id":"evt_1"}`)

	if ctx.Response.StatusCode() != fasthttp.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", fasthttp.StatusBadRequest, ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(string(ctx.Response.Body()), "tenant_id") {
		t.Fatalf("expected tenant_id validation error, got %s", ctx.Response.Body())
	}
	if len(bus.published) != 0 {
		t.Fatalf("invalid event should not publish events")
	}
}

func TestCollectRouteReturnsPublishError(t *testing.T) {
	handler, _ := newTestHandler(t, errors.New("event bus unavailable"))

	ctx := serveCollect(handler, fasthttp.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`)

	if ctx.Response.StatusCode() != fasthttp.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d: %s", fasthttp.StatusInternalServerError, ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if !strings.Contains(string(ctx.Response.Body()), "event bus unavailable") {
		t.Fatalf("expected publish error, got %s", ctx.Response.Body())
	}
}

func TestCollectRouteReturnsAcceptedForFilteredTraffic(t *testing.T) {
	stage, err := collect.NewTrafficFilterStage(collect.TrafficFilterConfig{})
	if err != nil {
		t.Fatalf("new traffic filter stage failed: %v", err)
	}
	handler, bus := newTestHandlerWithOptions(t, nil, collect.WithStages(stage))

	ctx := serveCollectWithHeaders(handler, fasthttp.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`, map[string]string{"User-Agent": "Googlebot/2.1"})

	if ctx.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", fasthttp.StatusAccepted, ctx.Response.StatusCode(), ctx.Response.Body())
	}
	var response AcceptedResponse
	if err := json.Unmarshal(ctx.Response.Body(), &response); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if !response.Filtered {
		t.Fatalf("expected filtered response, got %s", ctx.Response.Body())
	}
	if len(bus.published) != 0 {
		t.Fatalf("filtered traffic should not publish events")
	}
}

func TestCollectRouteIgnoresForwardedHeadersByDefault(t *testing.T) {
	stage, err := collect.NewTrafficFilterStage(collect.TrafficFilterConfig{
		InternalCIDRs: []string{"203.0.113.0/24"},
	})
	if err != nil {
		t.Fatalf("new traffic filter stage failed: %v", err)
	}
	handler, bus := newTestHandlerWithOptions(t, nil, collect.WithStages(stage))

	ctx := serveCollectWithHeaders(handler, fasthttp.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`, map[string]string{"X-Forwarded-For": "203.0.113.10"})

	if ctx.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", fasthttp.StatusAccepted, ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(bus.published))
	}
}

func TestCollectRoutePassesTrustedClientHeadersToStages(t *testing.T) {
	stage, err := collect.NewClientEnrichmentStage(collect.ClientEnrichmentConfig{
		HashSalt:         "hash-salt",
		IncludeUserAgent: true,
		IncludeIPHash:    true,
		IncludeReferrer:  true,
	})
	if err != nil {
		t.Fatalf("new client enrichment stage failed: %v", err)
	}
	handler, bus := newTestHandlerWithCollectAndRouteOptions(t, nil, []collect.HandlerOption{collect.WithStages(stage)}, []CollectHandlerOption{WithTrustedProxyHeaders()})

	ctx := serveCollectWithHeaders(handler, fasthttp.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`, map[string]string{
		"Referer":         "https://example.com/docs",
		"User-Agent":      "Mozilla/5.0",
		"X-Forwarded-For": "203.0.113.10, 10.0.0.1",
	})

	if ctx.Response.StatusCode() != fasthttp.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", fasthttp.StatusAccepted, ctx.Response.StatusCode(), ctx.Response.Body())
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(bus.published))
	}
	properties := bus.published[0].Properties
	if properties["client.user_agent"] != "Mozilla/5.0" {
		t.Fatalf("expected user agent property, got %#v", properties["client.user_agent"])
	}
	if properties["client.referrer"] != "https://example.com/docs" {
		t.Fatalf("expected referrer property, got %#v", properties["client.referrer"])
	}
	ipHash, _ := properties["client.ip_hash"].(string)
	if !strings.HasPrefix(ipHash, "ip_") || strings.Contains(ipHash, "203.0.113.10") {
		t.Fatalf("expected salted IP hash without raw IP, got %q", ipHash)
	}
}

func TestCollectRouteRejectsWrongPath(t *testing.T) {
	handler, _ := newTestHandler(t, nil)

	ctx := serveCollect(handler, fasthttp.MethodPost, "/missing", `{}`)

	if ctx.Response.StatusCode() != fasthttp.StatusNotFound {
		t.Fatalf("expected status %d, got %d", fasthttp.StatusNotFound, ctx.Response.StatusCode())
	}
}

func TestCollectRouteRejectsWrongMethod(t *testing.T) {
	handler, _ := newTestHandler(t, nil)

	ctx := serveCollect(handler, fasthttp.MethodGet, "/collect", ``)

	if ctx.Response.StatusCode() != fasthttp.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", fasthttp.StatusMethodNotAllowed, ctx.Response.StatusCode())
	}
}

func newTestHandler(t *testing.T, publishErr error) (fasthttp.RequestHandler, *recordingBus) {
	t.Helper()

	return newTestHandlerWithOptions(t, publishErr)
}

func newTestHandlerWithOptions(t *testing.T, publishErr error, opts ...collect.HandlerOption) (fasthttp.RequestHandler, *recordingBus) {
	t.Helper()

	return newTestHandlerWithCollectAndRouteOptions(t, publishErr, opts, nil)
}

func newTestHandlerWithCollectAndRouteOptions(t *testing.T, publishErr error, handlerOpts []collect.HandlerOption, routeOpts []CollectHandlerOption) (fasthttp.RequestHandler, *recordingBus) {
	t.Helper()

	bus := &recordingBus{err: publishErr}
	handler, err := collect.NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	}, handlerOpts...)
	if err != nil {
		t.Fatalf("new collect handler failed: %v", err)
	}

	route, err := NewCollectRoute("/collect", handler, routeOpts...)
	if err != nil {
		t.Fatalf("new collect route failed: %v", err)
	}
	return route, bus
}

func serveCollect(handler fasthttp.RequestHandler, method string, path string, body string) *fasthttp.RequestCtx {
	return serveCollectWithHeaders(handler, method, path, body, nil)
}

func serveCollectWithHeaders(handler fasthttp.RequestHandler, method string, path string, body string, headers map[string]string) *fasthttp.RequestCtx {
	var request fasthttp.Request
	request.Header.SetMethod(method)
	request.SetRequestURI(path)
	request.Header.SetContentType(contentTypeJSON)
	request.SetBodyString(body)
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	var ctx fasthttp.RequestCtx
	ctx.Init(&request, nil, nil)
	handler(&ctx)
	return &ctx
}

type recordingBus struct {
	err       error                     // err forces Publish to fail through the HTTP adapter
	published []contracts.EventEnvelope // published records envelopes accepted by collect.Handler
}

func (b *recordingBus) Publish(_ context.Context, envelope contracts.EventEnvelope) error {
	if b.err != nil {
		return b.err
	}
	b.published = append(b.published, envelope)
	return nil
}

func (b *recordingBus) Subscribe(context.Context, eventbus.ConsumerGroup, eventbus.Handler) error {
	return nil
}
