package collect

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

func TestHandlerPublishesNormalizedEnvelope(t *testing.T) {
	bus := newRecordingBus()
	now := time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	handler, err := NewHandler(bus, func() time.Time { return now })
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}

	envelope, err := handler.Handle(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("handle failed: %v", err)
	}

	if envelope.ReceivedAt != now {
		t.Fatalf("expected receivedAt %s, got %s", now, envelope.ReceivedAt)
	}
	if len(bus.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(bus.published))
	}
	if bus.published[0].ID != "evt_1" {
		t.Fatalf("expected event id evt_1, got %q", bus.published[0].ID)
	}
}

func TestHandlerRejectsInvalidRequestBeforePublish(t *testing.T) {
	bus := newRecordingBus()
	handler, err := NewHandler(bus, func() time.Time { return time.Now().UTC() })
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}
	request := validRequest()
	request.EventName = ""

	_, err = handler.Handle(context.Background(), request)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if len(bus.published) != 0 {
		t.Fatalf("invalid request should not publish events")
	}
}

func TestHandlerReturnsPublishError(t *testing.T) {
	bus := newRecordingBus()
	bus.err = errors.New("event bus unavailable")
	handler, err := NewHandler(bus, func() time.Time { return time.Now().UTC() })
	if err != nil {
		t.Fatalf("new handler failed: %v", err)
	}

	_, err = handler.Handle(context.Background(), validRequest())
	if err == nil {
		t.Fatal("expected publish error")
	}
}

type recordingBus struct {
	err       error                     // err forces Publish to fail for handler error tests
	published []contracts.EventEnvelope // published records envelopes accepted by collect.Handler
}

func newRecordingBus() *recordingBus {
	return &recordingBus{}
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
