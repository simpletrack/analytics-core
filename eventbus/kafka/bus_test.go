package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

func TestPublishWritesJSONEnvelope(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)

	envelope := contracts.EventEnvelope{
		ID:        "evt_1",
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		EventName: "pageview",
	}
	if err := bus.Publish(context.Background(), envelope); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	if len(producer.messages) != 1 {
		t.Fatalf("published messages = %d, want 1", len(producer.messages))
	}
	got := producer.messages[0]
	if got.Topic != "analytics.events" {
		t.Fatalf("topic = %q, want analytics.events", got.Topic)
	}
	var decoded contracts.EventEnvelope
	value, err := got.Value.Encode()
	if err != nil {
		t.Fatalf("encode value failed: %v", err)
	}
	if err := json.Unmarshal(value, &decoded); err != nil {
		t.Fatalf("unmarshal value failed: %v", err)
	}
	if decoded.ID != envelope.ID {
		t.Fatalf("envelope id = %q, want %q", decoded.ID, envelope.ID)
	}
}

func TestProcessRetriesHandlerAndCompletesAfterSuccess(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)
	bus.opts.MaxAttempts = 3
	bus.opts.RetryBackoff = time.Nanosecond
	gate, completed := newRegisteredGate(40)

	attempts := 0
	message := testConsumerMessage(t, "evt_retry")
	bus.processConsumerMessageWithHandler(context.Background(), eventbus.ConsumerGroup{Name: "g1", Consumer: "c1"}, func(_ context.Context, msg eventbus.Message) error {
		attempts = msg.Attempt
		if msg.Attempt == 1 {
			return errors.New("temporary failure")
		}
		return nil
	}, message, gate)

	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if *completed != 1 {
		t.Fatalf("completed offsets = %d, want 1", *completed)
	}
	if len(producer.messages) != 0 {
		t.Fatalf("unexpected DLQ messages: %d", len(producer.messages))
	}
}

func TestProcessDeadLettersAfterMaxAttemptsThenCompletes(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)
	bus.opts.MaxAttempts = 2
	bus.opts.RetryBackoff = time.Nanosecond
	gate, completed := newRegisteredGate(50)

	message := testConsumerMessage(t, "evt_dlq")
	bus.processConsumerMessageWithHandler(context.Background(), eventbus.ConsumerGroup{Name: "g1", Consumer: "c1"}, func(context.Context, eventbus.Message) error {
		return errors.New("permanent failure")
	}, message, gate)

	if *completed != 1 {
		t.Fatalf("completed offsets = %d, want 1", *completed)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("DLQ messages = %d, want 1", len(producer.messages))
	}
	if producer.messages[0].Topic != "analytics.events.dead" {
		t.Fatalf("DLQ topic = %q, want analytics.events.dead", producer.messages[0].Topic)
	}
}

func TestProcessTreatsHandlerPanicAsRetryableFailure(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)
	bus.opts.MaxAttempts = 2
	bus.opts.RetryBackoff = time.Nanosecond
	gate, completed := newRegisteredGate(55)

	message := testConsumerMessage(t, "evt_panic")
	attempts := 0
	bus.processConsumerMessageWithHandler(context.Background(), eventbus.ConsumerGroup{Name: "g1", Consumer: "c1"}, func(_ context.Context, msg eventbus.Message) error {
		attempts = msg.Attempt
		if msg.ID != "analytics.events:1:7" {
			t.Fatalf("message id = %q, want kafka delivery id", msg.ID)
		}
		panic("boom")
	}, message, gate)

	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if *completed != 1 {
		t.Fatalf("completed offsets = %d, want 1", *completed)
	}
	if len(producer.messages) != 1 {
		t.Fatalf("DLQ messages = %d, want 1", len(producer.messages))
	}
}

func TestProcessDoesNotCompleteWhenDeadLetterWriteFails(t *testing.T) {
	producer := &recordingProducer{err: errors.New("kafka unavailable")}
	bus := newTestBus(t, producer)
	bus.opts.MaxAttempts = 1
	bus.opts.RetryBackoff = time.Nanosecond
	gate, completed := newRegisteredGate(60)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	message := testConsumerMessage(t, "evt_dlq_failed")
	bus.processConsumerMessageWithHandler(ctx, eventbus.ConsumerGroup{Name: "g1", Consumer: "c1"}, func(context.Context, eventbus.Message) error {
		return errors.New("permanent failure")
	}, message, gate)

	if *completed != 0 {
		t.Fatalf("completed offsets = %d, want 0", *completed)
	}
}

func TestPublishDeadLetterIncludesQueueMetadata(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)
	message := testConsumerMessage(t, "evt_meta")

	err := bus.publishDeadLetter(context.Background(), eventbus.ConsumerGroup{Name: "group", Consumer: "consumer"}, message, 3, contracts.EventEnvelope{ID: "evt_meta"}, errors.New("failed"), nil)
	if err != nil {
		t.Fatalf("publish dead letter failed: %v", err)
	}

	value, err := producer.messages[0].Value.Encode()
	if err != nil {
		t.Fatalf("encode DLQ value failed: %v", err)
	}
	var decoded deadLetterRecord
	if err := json.Unmarshal(value, &decoded); err != nil {
		t.Fatalf("unmarshal DLQ value failed: %v", err)
	}
	if decoded.OriginalTopic != "analytics.events" || decoded.OriginalPartition != 1 || decoded.OriginalOffset != 7 {
		t.Fatalf("unexpected source metadata: %#v", decoded)
	}
	if decoded.ConsumerGroup != "group" || decoded.ConsumerID != "consumer" || decoded.Attempt != 3 {
		t.Fatalf("unexpected consumer metadata: %#v", decoded)
	}
}

func newTestBus(t *testing.T, producer *recordingProducer) *Bus {
	t.Helper()

	opts, err := Options{Brokers: []string{"127.0.0.1:9092"}, RetryBackoff: time.Nanosecond}.normalize()
	if err != nil {
		t.Fatalf("normalize options failed: %v", err)
	}
	pool, err := newDynamicWorkerPool(workerPoolConfig{name: "test", workers: 1, queueSize: 1})
	if err != nil {
		t.Fatalf("new worker pool failed: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Close()
	})
	return newBusWithDependencies(opts, producer, nil, pool)
}

func newRegisteredGate(offset int64) (*messageCompletionGate, *int) {
	committer := newPartitionOrderedCommitter()
	completed := 0
	committer.Register(offset, 1, func() { completed++ })
	return newMessageCompletionGate(offset, 1, committer, &messageCompletionGateTracker{}), &completed
}

func testConsumerMessage(t *testing.T, id string) *sarama.ConsumerMessage {
	t.Helper()

	payload, err := json.Marshal(contracts.EventEnvelope{ID: id, EventName: "pageview"})
	if err != nil {
		t.Fatalf("marshal envelope failed: %v", err)
	}
	return &sarama.ConsumerMessage{
		Topic:     "analytics.events",
		Partition: 1,
		Offset:    7,
		Key:       []byte("tenant:project:source"),
		Value:     payload,
	}
}

type recordingProducer struct {
	messages []*sarama.ProducerMessage
	err      error
}

func (p *recordingProducer) SendMessage(message *sarama.ProducerMessage) (int32, int64, error) {
	if p.err != nil {
		return 0, 0, p.err
	}
	p.messages = append(p.messages, message)
	return 0, int64(len(p.messages)), nil
}

func (p *recordingProducer) Close() error {
	return nil
}
