package redisstream_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
	"github.com/simpletrack/analytics-core/eventbus/redisstream"
)

const redisStreamBenchmarkTimeout = 60 * time.Second

// BenchmarkBusPublish measures the Redis Stream append path used by collect.
//
// The benchmark is opt-in through ANALYTICS_CORE_REDIS_ADDR because it requires
// a real Redis server. It times EventBus.Publish only; stream cleanup and row
// count verification happen outside the timed section.
func BenchmarkBusPublish(b *testing.B) {
	client := openBenchmarkRedis(b)
	defer client.Close()

	stream := fmt.Sprintf("events:bench:publish:%d", time.Now().UnixNano())
	defer cleanupBenchmarkRedisStreams(b, client, stream)
	bus := newBenchmarkRedisBus(b, client, stream, 100, false)
	envelopes := benchmarkRedisEnvelopes(b.N, "publish")

	ctx, cancel := context.WithTimeout(context.Background(), redisStreamBenchmarkTimeout)
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		if err := bus.Publish(ctx, envelopes[idx]); err != nil {
			b.Fatalf("redis stream publish failed: %v", err)
		}
	}
	b.StopTimer()

	assertBenchmarkRedisStreamLength(b, client, stream, int64(b.N))
}

// BenchmarkBusSubscribeAck measures Redis Stream consume, decode, and ack cost.
//
// The benchmark pre-publishes messages before timing starts. The timed section
// starts the normal Subscribe loop and waits until all seeded messages have
// passed through the EventBus handler and Ack callback.
func BenchmarkBusSubscribeAck(b *testing.B) {
	client := openBenchmarkRedis(b)
	defer client.Close()

	stream := fmt.Sprintf("events:bench:consume:%d", time.Now().UnixNano())
	defer cleanupBenchmarkRedisStreams(b, client, stream)
	bus := newBenchmarkRedisBus(b, client, stream, 100, false)
	group := eventbus.ConsumerGroup{Name: "benchmark-ingestion", Consumer: "benchmark-worker"}
	seedBenchmarkRedisStream(b, bus, benchmarkRedisEnvelopes(b.N, "consume"))
	createBenchmarkConsumerGroup(b, client, stream, group.Name)

	consumeCtx, cancelConsume := context.WithCancel(context.Background())
	defer cancelConsume()
	subscribeDone := make(chan error, 1)
	var consumed int64

	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		subscribeDone <- bus.Subscribe(consumeCtx, group, func(_ context.Context, msg eventbus.Message) error {
			atomic.AddInt64(&consumed, 1)
			return nil
		})
	}()

	waitBenchmarkRedisConsumedAndAcked(b, client, stream, group.Name, &consumed, int64(b.N))
	b.StopTimer()

	cancelConsume()
	if err := <-subscribeDone; err != nil && !errors.Is(err, context.Canceled) {
		b.Fatalf("redis stream subscribe stopped with unexpected error: %v", err)
	}
	assertBenchmarkRedisPendingCount(b, client, stream, group.Name, 0)
}

// openBenchmarkRedis returns a connected Redis client for opt-in benchmarks.
func openBenchmarkRedis(b *testing.B) *redis.Client {
	b.Helper()

	addr := os.Getenv("ANALYTICS_CORE_REDIS_ADDR")
	if addr == "" {
		b.Skip("ANALYTICS_CORE_REDIS_ADDR is not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		b.Fatalf("redis benchmark dependency is not ready: %v", err)
	}
	return client
}

// newBenchmarkRedisBus creates a Redis Stream EventBus for benchmark streams.
func newBenchmarkRedisBus(b *testing.B, client *redis.Client, stream string, count int64, ensureConsumer bool) *redisstream.Bus {
	b.Helper()

	bus, err := redisstream.New(client, redisstream.Options{
		Stream:         stream,
		Block:          10 * time.Millisecond,
		Count:          count,
		EnsureConsumer: ensureConsumer,
	})
	if err != nil {
		b.Fatalf("new redis stream benchmark bus failed: %v", err)
	}
	return bus
}

// benchmarkRedisEnvelopes builds stable event envelopes outside timed sections.
func benchmarkRedisEnvelopes(count int, prefix string) []contracts.EventEnvelope {
	envelopes := make([]contracts.EventEnvelope, count)
	baseTime := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	for idx := 0; idx < count; idx++ {
		envelopes[idx] = contracts.EventEnvelope{
			ID:         fmt.Sprintf("evt_%s_%06d", prefix, idx),
			TenantID:   "tenant_benchmark",
			ProjectID:  "project_benchmark",
			SourceID:   "source_benchmark",
			SourceType: "web",
			EventName:  "page_view",
			DistinctID: "visitor_benchmark",
			SessionID:  "session_benchmark",
			VisitID:    "visit_benchmark",
			EventTime:  baseTime.Add(time.Duration(idx) * time.Millisecond),
			ReceivedAt: baseTime.Add(time.Duration(idx)*time.Millisecond + time.Millisecond),
			Properties: map[string]any{"path": "/redis-bench"},
			UserProps:  map[string]any{"tier": "bench"},
			Source:     "redis-benchmark",
		}
	}
	return envelopes
}

// seedBenchmarkRedisStream publishes messages before the timed consume section.
func seedBenchmarkRedisStream(b *testing.B, bus *redisstream.Bus, envelopes []contracts.EventEnvelope) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), redisStreamBenchmarkTimeout)
	defer cancel()
	for _, envelope := range envelopes {
		if err := bus.Publish(ctx, envelope); err != nil {
			b.Fatalf("seed redis stream benchmark message failed: %v", err)
		}
	}
}

// createBenchmarkConsumerGroup prepares Redis consumer group state outside timing.
func createBenchmarkConsumerGroup(b *testing.B, client *redis.Client, stream string, group string) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		b.Fatalf("create redis benchmark consumer group failed: %v", err)
	}
}

// waitBenchmarkRedisConsumedAndAcked waits until handler count and ack state agree.
func waitBenchmarkRedisConsumedAndAcked(b *testing.B, client *redis.Client, stream string, group string, consumed *int64, want int64) {
	b.Helper()

	deadline := time.Now().Add(redisStreamBenchmarkTimeout)
	for time.Now().Before(deadline) {
		// The handler increments consumed before Subscribe calls Ack. Requiring
		// pending=0 after the count reaches want keeps the timed region honest.
		if atomic.LoadInt64(consumed) == want && benchmarkRedisPendingCount(b, client, stream, group) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Fatalf("redis stream consume benchmark timed out after consuming %d/%d messages; pending=%d", atomic.LoadInt64(consumed), want, benchmarkRedisPendingCount(b, client, stream, group))
}

// cleanupBenchmarkRedisStreams removes benchmark streams from Redis.
func cleanupBenchmarkRedisStreams(b *testing.B, client *redis.Client, streams ...string) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Del(ctx, streams...).Err(); err != nil {
		b.Fatalf("cleanup redis benchmark streams failed: %v", err)
	}
}

// assertBenchmarkRedisStreamLength verifies that all timed publishes persisted.
func assertBenchmarkRedisStreamLength(b *testing.B, client *redis.Client, stream string, want int64) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := client.XLen(ctx, stream).Result()
	if err != nil {
		b.Fatalf("redis benchmark xlen failed: %v", err)
	}
	if got != want {
		b.Fatalf("redis benchmark stream length = %d, want %d", got, want)
	}
}

// assertBenchmarkRedisPendingCount verifies that all consumed messages were acked.
func assertBenchmarkRedisPendingCount(b *testing.B, client *redis.Client, stream string, group string, want int64) {
	b.Helper()

	got := benchmarkRedisPendingCount(b, client, stream, group)
	if got != want {
		b.Fatalf("redis benchmark pending count = %d, want %d", got, want)
	}
}

// benchmarkRedisPendingCount returns the current pending count for group.
func benchmarkRedisPendingCount(b *testing.B, client *redis.Client, stream string, group string) int64 {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		b.Fatalf("redis benchmark xpending failed: %v", err)
	}
	return pending.Count
}
