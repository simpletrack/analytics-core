package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/simpletrack/analytics-core/collect"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

func TestCollectRouteAcceptsEvent(t *testing.T) {
	app, bus := newTestApp(t, nil)

	response := serveCollect(t, app, fiber.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1",
		"properties":{"path":"/"}
	}`, nil)

	if response.StatusCode != fiber.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", fiber.StatusAccepted, response.StatusCode, response.Body)
	}
	if !strings.Contains(string(response.Body), `"id":"evt_1"`) {
		t.Fatalf("expected accepted event id, got %s", response.Body)
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(bus.published))
	}
}

func TestCollectRouteRejectsInvalidJSON(t *testing.T) {
	app, bus := newTestApp(t, nil)

	response := serveCollect(t, app, fiber.MethodPost, "/collect", `{`, nil)

	if response.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", fiber.StatusBadRequest, response.StatusCode)
	}
	if len(bus.published) != 0 {
		t.Fatalf("invalid json should not publish events")
	}
}

func TestCollectRouteReturnsValidationError(t *testing.T) {
	app, bus := newTestApp(t, nil)

	response := serveCollect(t, app, fiber.MethodPost, "/collect", `{"id":"evt_1"}`, nil)

	if response.StatusCode != fiber.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", fiber.StatusBadRequest, response.StatusCode, response.Body)
	}
	if !strings.Contains(string(response.Body), "tenant_id") {
		t.Fatalf("expected tenant_id validation error, got %s", response.Body)
	}
	if len(bus.published) != 0 {
		t.Fatalf("invalid event should not publish events")
	}
}

func TestCollectRouteReturnsPublishError(t *testing.T) {
	app, _ := newTestApp(t, errors.New("event bus unavailable"))

	response := serveCollect(t, app, fiber.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`, nil)

	if response.StatusCode != fiber.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d: %s", fiber.StatusInternalServerError, response.StatusCode, response.Body)
	}
	if !strings.Contains(string(response.Body), "event bus unavailable") {
		t.Fatalf("expected publish error, got %s", response.Body)
	}
}

func TestCollectRouteReturnsAcceptedForFilteredTraffic(t *testing.T) {
	stage, err := collect.NewTrafficFilterStage(collect.TrafficFilterConfig{})
	if err != nil {
		t.Fatalf("new traffic filter stage failed: %v", err)
	}
	app, bus := newTestAppWithOptions(t, nil, collect.WithStages(stage))

	response := serveCollect(t, app, fiber.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`, map[string]string{"User-Agent": "Googlebot/2.1"})

	if response.StatusCode != fiber.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", fiber.StatusAccepted, response.StatusCode, response.Body)
	}
	var accepted AcceptedResponse
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if !accepted.Filtered {
		t.Fatalf("expected filtered response, got %s", response.Body)
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
	app, bus := newTestAppWithOptions(t, nil, collect.WithStages(stage))

	response := serveCollect(t, app, fiber.MethodPost, "/collect", `{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`, map[string]string{"X-Forwarded-For": "203.0.113.10"})

	if response.StatusCode != fiber.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", fiber.StatusAccepted, response.StatusCode, response.Body)
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
	app, bus := newTestAppWithCollectAndRouteOptions(t, nil, []collect.HandlerOption{collect.WithStages(stage)}, []CollectHandlerOption{WithTrustedProxyHeaders()})

	response := serveCollect(t, app, fiber.MethodPost, "/collect", `{
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

	if response.StatusCode != fiber.StatusAccepted {
		t.Fatalf("expected status %d, got %d: %s", fiber.StatusAccepted, response.StatusCode, response.Body)
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
	app, _ := newTestApp(t, nil)

	response := serveCollect(t, app, fiber.MethodPost, "/missing", `{}`, nil)

	if response.StatusCode != fiber.StatusNotFound {
		t.Fatalf("expected status %d, got %d", fiber.StatusNotFound, response.StatusCode)
	}
}

func TestCollectRouteRejectsWrongMethod(t *testing.T) {
	app, _ := newTestApp(t, nil)

	response := serveCollect(t, app, fiber.MethodGet, "/collect", ``, nil)

	if response.StatusCode != fiber.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", fiber.StatusMethodNotAllowed, response.StatusCode)
	}
}

func newTestApp(t *testing.T, publishErr error) (*fiber.App, *recordingBus) {
	t.Helper()

	return newTestAppWithOptions(t, publishErr)
}

func newTestAppWithOptions(t *testing.T, publishErr error, opts ...collect.HandlerOption) (*fiber.App, *recordingBus) {
	t.Helper()

	return newTestAppWithCollectAndRouteOptions(t, publishErr, opts, nil)
}

func newTestAppWithCollectAndRouteOptions(t *testing.T, publishErr error, handlerOpts []collect.HandlerOption, routeOpts []CollectHandlerOption) (*fiber.App, *recordingBus) {
	t.Helper()

	bus := &recordingBus{err: publishErr}
	handler, err := collect.NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	}, handlerOpts...)
	if err != nil {
		t.Fatalf("new collect handler failed: %v", err)
	}

	app, err := NewCollectApp("/collect", handler, routeOpts...)
	if err != nil {
		t.Fatalf("new collect app failed: %v", err)
	}
	return app, bus
}

func serveCollect(t *testing.T, app *fiber.App, method string, path string, body string, headers map[string]string) testResponse {
	t.Helper()

	request, err := http.NewRequest(method, path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	request.Header.Set("Content-Type", contentTypeJSON)
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("serve request failed: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body failed: %v", err)
	}
	return testResponse{
		StatusCode: response.StatusCode,
		Body:       payload,
	}
}

type testResponse struct {
	StatusCode int
	Body       []byte
}

type recordingBus struct {
	err       error                     // err forces Publish to fail through the HTTP adapter.
	published []contracts.EventEnvelope // published records envelopes accepted by collect.Handler.
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
