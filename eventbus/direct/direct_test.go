package direct_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/eventbus/direct"
)

func TestBusPublishesToSubscriber(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := direct.New()
	received := make(chan contracts.EventEnvelope, 1)

	go func() {
		_ = bus.Subscribe(ctx, eventbus.ConsumerGroup{Name: "test", Consumer: "c1"}, func(_ context.Context, msg eventbus.Message) error {
			received <- msg.Envelope
			return msg.Ack(ctx)
		})
	}()
	time.Sleep(10 * time.Millisecond)

	envelope := contracts.EventEnvelope{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "pageview",
		DistinctID: "user_1",
		EventTime:  time.Now().UTC(),
		ReceivedAt: time.Now().UTC(),
	}

	if err := bus.Publish(ctx, envelope); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	select {
	case got := <-received:
		if got.ID != envelope.ID {
			t.Fatalf("expected event id %q, got %q", envelope.ID, got.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive event")
	}
}

func TestBusReturnsHandlerError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := direct.New()
	want := errors.New("handler failed")

	go func() {
		_ = bus.Subscribe(ctx, eventbus.ConsumerGroup{Name: "test", Consumer: "c1"}, func(context.Context, eventbus.Message) error {
			return want
		})
	}()
	time.Sleep(10 * time.Millisecond)

	err := bus.Publish(ctx, contracts.EventEnvelope{ID: "evt_1"})
	if !errors.Is(err, want) {
		t.Fatalf("expected handler error %v, got %v", want, err)
	}
}
