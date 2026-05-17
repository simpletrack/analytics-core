package kafka

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

func BenchmarkKafkaBusPublish(b *testing.B) {
	bus := newBenchmarkBus(b)
	envelope := contracts.EventEnvelope{
		TenantID:  "tenant_bench",
		ProjectID: "project_bench",
		SourceID:  "source_bench",
		EventName: "pageview",
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		envelope.ID = "evt_" + strconv.Itoa(idx)
		if err := bus.Publish(ctx, envelope); err != nil {
			b.Fatalf("publish failed: %v", err)
		}
	}
}

func BenchmarkKafkaProcessNoopHandler(b *testing.B) {
	bus := newBenchmarkBus(b)
	group := eventbus.ConsumerGroup{Name: "group", Consumer: "consumer"}
	message := testConsumerMessageForBenchmark(b)
	handler := func(context.Context, eventbus.Message) error { return nil }
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		message.Offset = int64(idx)
		gate := newBenchmarkRegisteredGate(message.Offset)
		bus.processConsumerMessageWithHandler(ctx, group, handler, message, gate)
	}
}

func BenchmarkKafkaOrderedCommitterRegisterComplete(b *testing.B) {
	committer := newPartitionOrderedCommitter()
	markCount := 0

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		offset := int64(idx)
		committer.Register(offset, 1, func() { markCount++ })
		committer.Complete(offset, 1)
	}
	if markCount != b.N {
		b.Fatalf("marked offsets = %d, want %d", markCount, b.N)
	}
}

func newBenchmarkBus(b *testing.B) *Bus {
	b.Helper()

	opts, err := Options{Brokers: []string{"127.0.0.1:9092"}, RetryBackoff: time.Nanosecond}.normalize()
	if err != nil {
		b.Fatalf("normalize options failed: %v", err)
	}
	pool, err := newDynamicWorkerPool(workerPoolConfig{name: "benchmark", workers: 1, queueSize: 1})
	if err != nil {
		b.Fatalf("new worker pool failed: %v", err)
	}
	b.Cleanup(func() {
		_ = pool.Close()
	})
	return newBusWithDependencies(opts, discardProducer{}, nil, pool)
}

func newBenchmarkRegisteredGate(offset int64) *messageCompletionGate {
	committer := newPartitionOrderedCommitter()
	committer.Register(offset, 1, nil)
	return newMessageCompletionGate(offset, 1, committer, &messageCompletionGateTracker{})
}

func testConsumerMessageForBenchmark(b *testing.B) *sarama.ConsumerMessage {
	b.Helper()

	payload, err := json.Marshal(contracts.EventEnvelope{ID: "evt_benchmark", EventName: "pageview"})
	if err != nil {
		b.Fatalf("marshal envelope failed: %v", err)
	}
	return &sarama.ConsumerMessage{
		Topic:     "analytics.events",
		Partition: 1,
		Offset:    7,
		Key:       []byte("tenant:project:source"),
		Value:     payload,
	}
}

type discardProducer struct{}

func (discardProducer) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, nil
}

func (discardProducer) Close() error {
	return nil
}
