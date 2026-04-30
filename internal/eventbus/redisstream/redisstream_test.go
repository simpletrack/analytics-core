package redisstream_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/internal/eventbus"
	"github.com/simpletrack/analytics-core/internal/eventbus/redisstream"
	"github.com/simpletrack/analytics-core/pkg/contracts"
)

func TestBusPublishesConsumesAndAcks(t *testing.T) {
	addr := os.Getenv("ANALYTICS_CORE_REDIS_ADDR")
	if addr == "" {
		t.Skip("ANALYTICS_CORE_REDIS_ADDR is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		_ = client.Close()
	})

	stream := fmt.Sprintf("events:test:%d", time.Now().UnixNano())
	group := eventbus.ConsumerGroup{Name: "ingestion", Consumer: "test-worker"}
	t.Cleanup(func() {
		_ = client.Del(context.Background(), stream).Err()
	})

	bus, err := redisstream.New(client, redisstream.Options{
		Stream:         stream,
		Block:          100 * time.Millisecond,
		Count:          1,
		EnsureConsumer: true,
	})
	if err != nil {
		t.Fatalf("new bus failed: %v", err)
	}

	received := make(chan contracts.EventEnvelope, 1)
	done := make(chan error, 1)

	subscribeCtx, stopSubscribe := context.WithCancel(ctx)
	defer stopSubscribe()

	go func() {
		err := bus.Subscribe(subscribeCtx, group, func(_ context.Context, msg eventbus.Message) error {
			received <- msg.Envelope
			return nil
		})
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			done <- nil
			return
		}
		done <- err
	}()

	envelope := contracts.EventEnvelope{
		ID:         "evt_redis_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "pageview",
		DistinctID: "visitor_1",
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
	case <-ctx.Done():
		t.Fatalf("subscriber did not receive event: %v", ctx.Err())
	}

	assertPendingCount(t, client, stream, group.Name, 0)

	stopSubscribe()
	if err := <-done; err != nil {
		t.Fatalf("subscribe returned error: %v", err)
	}
}

func assertPendingCount(t *testing.T, client *redis.Client, stream string, group string, want int64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pending, err := client.XPending(context.Background(), stream, group).Result()
		if err != nil {
			t.Fatalf("xpending failed: %v", err)
		}
		if pending.Count == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	pending, err := client.XPending(context.Background(), stream, group).Result()
	if err != nil {
		t.Fatalf("xpending failed: %v", err)
	}
	t.Fatalf("expected pending count %d, got %d", want, pending.Count)
}
