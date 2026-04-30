package httpapi

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/internal/collect"
	"github.com/simpletrack/analytics-core/internal/eventbus"
	"github.com/simpletrack/analytics-core/pkg/contracts"
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

	bus := &recordingBus{err: publishErr}
	handler, err := collect.NewHandler(bus, func() time.Time {
		return time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatalf("new collect handler failed: %v", err)
	}

	route, err := NewCollectRoute("/collect", handler)
	if err != nil {
		t.Fatalf("new collect route failed: %v", err)
	}
	return route, bus
}

func serveCollect(handler fasthttp.RequestHandler, method string, path string, body string) *fasthttp.RequestCtx {
	var request fasthttp.Request
	request.Header.SetMethod(method)
	request.SetRequestURI(path)
	request.Header.SetContentType(contentTypeJSON)
	request.SetBodyString(body)

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
