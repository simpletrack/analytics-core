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

func TestNewSaramaConfigUsesDurableProducerDefaults(t *testing.T) {
	opts, err := Options{Brokers: []string{"127.0.0.1:9092"}}.normalize()
	if err != nil {
		t.Fatalf("normalize options failed: %v", err)
	}
	config := newSaramaConfig(opts)

	if config.Producer.RequiredAcks != sarama.WaitForAll {
		t.Fatalf("producer required acks = %v, want WaitForAll", config.Producer.RequiredAcks)
	}
	if config.Producer.Retry.Max != defaultProducerRetryMax {
		t.Fatalf("producer retry max = %d, want %d", config.Producer.Retry.Max, defaultProducerRetryMax)
	}
	if config.Producer.Retry.Backoff != defaultProducerRetryBackoff {
		t.Fatalf("producer retry backoff = %s, want %s", config.Producer.Retry.Backoff, defaultProducerRetryBackoff)
	}
	if !config.Producer.Return.Successes {
		t.Fatal("sync producer requires returned successes")
	}
}

func TestNewSaramaConfigMapsProducerReliabilityOptions(t *testing.T) {
	opts, err := Options{
		Brokers:                  []string{"127.0.0.1:9092"},
		ProducerAcks:             ProducerAcksLeader,
		ProducerRetryMax:         ptrInt(9),
		ProducerRetryBackoff:     ptrDuration(750 * time.Millisecond),
		ProducerFlushBytes:       1024,
		ProducerFlushMessages:    10,
		ProducerFlushFrequency:   time.Second,
		ProducerFlushMaxMessages: 20,
	}.normalize()
	if err != nil {
		t.Fatalf("normalize options failed: %v", err)
	}
	config := newSaramaConfig(opts)

	if config.Producer.RequiredAcks != sarama.WaitForLocal {
		t.Fatalf("producer required acks = %v, want WaitForLocal", config.Producer.RequiredAcks)
	}
	if config.Producer.Retry.Max != 9 || config.Producer.Retry.Backoff != 750*time.Millisecond {
		t.Fatalf("unexpected retry config: max=%d backoff=%s", config.Producer.Retry.Max, config.Producer.Retry.Backoff)
	}
	if config.Producer.Flush.Bytes != 1024 || config.Producer.Flush.Messages != 10 || config.Producer.Flush.Frequency != time.Second || config.Producer.Flush.MaxMessages != 20 {
		t.Fatalf("unexpected flush config: %+v", config.Producer.Flush)
	}
}

func TestNewSaramaConfigPreservesExplicitZeroProducerRetryOptions(t *testing.T) {
	opts, err := Options{
		Brokers:              []string{"127.0.0.1:9092"},
		ProducerRetryMax:     ptrInt(0),
		ProducerRetryBackoff: ptrDuration(0),
	}.normalize()
	if err != nil {
		t.Fatalf("normalize options failed: %v", err)
	}
	config := newSaramaConfig(opts)

	if config.Producer.Retry.Max != 0 {
		t.Fatalf("producer retry max = %d, want explicit zero", config.Producer.Retry.Max)
	}
	if config.Producer.Retry.Backoff != 0 {
		t.Fatalf("producer retry backoff = %s, want explicit zero", config.Producer.Retry.Backoff)
	}
}

func TestNewSaramaConfigMapsNoResponseProducerAcks(t *testing.T) {
	opts, err := Options{Brokers: []string{"127.0.0.1:9092"}, ProducerAcks: ProducerAcksNone}.normalize()
	if err != nil {
		t.Fatalf("normalize options failed: %v", err)
	}
	config := newSaramaConfig(opts)

	if config.Producer.RequiredAcks != sarama.NoResponse {
		t.Fatalf("producer required acks = %v, want NoResponse", config.Producer.RequiredAcks)
	}
}

func TestNewSaramaConfigEnablesIdempotenceWithRequiredSaramaLimits(t *testing.T) {
	opts, err := Options{Brokers: []string{"127.0.0.1:9092"}, IdempotentProducer: true}.normalize()
	if err != nil {
		t.Fatalf("normalize options failed: %v", err)
	}
	config := newSaramaConfig(opts)

	if !config.Producer.Idempotent {
		t.Fatal("idempotent producer is disabled")
	}
	if config.Producer.RequiredAcks != sarama.WaitForAll {
		t.Fatalf("producer required acks = %v, want WaitForAll", config.Producer.RequiredAcks)
	}
	if config.Net.MaxOpenRequests != 1 {
		t.Fatalf("max open requests = %d, want 1", config.Net.MaxOpenRequests)
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("sarama config validate failed: %v", err)
	}
}

func TestNewSaramaConfigKeepsDefaultInFlightLimitWithoutIdempotence(t *testing.T) {
	opts, err := Options{Brokers: []string{"127.0.0.1:9092"}}.normalize()
	if err != nil {
		t.Fatalf("normalize options failed: %v", err)
	}
	config := newSaramaConfig(opts)

	if config.Producer.Idempotent {
		t.Fatal("idempotent producer is enabled by default")
	}
	if config.Net.MaxOpenRequests == 1 {
		t.Fatal("max open requests was tightened without idempotent producer")
	}
}

func TestNormalizeRejectsInvalidProducerReliabilityOptions(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{name: "unknown acks", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerAcks: ProducerAcks("quorum")}},
		{name: "negative retry max", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerRetryMax: ptrInt(-1)}},
		{name: "negative retry backoff", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerRetryBackoff: ptrDuration(-time.Nanosecond)}},
		{name: "negative flush bytes", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerFlushBytes: -1}},
		{name: "negative flush messages", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerFlushMessages: -1}},
		{name: "negative flush frequency", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerFlushFrequency: -time.Nanosecond}},
		{name: "negative flush max messages", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerFlushMaxMessages: -1}},
		{name: "flush max below messages", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerFlushMessages: 10, ProducerFlushMaxMessages: 5}},
		{name: "idempotent without all acks", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerAcks: ProducerAcksLeader, IdempotentProducer: true}},
		{name: "idempotent without producer retry", opts: Options{Brokers: []string{"127.0.0.1:9092"}, ProducerRetryMax: ptrInt(0), IdempotentProducer: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := tt.opts.normalize(); err == nil {
				t.Fatal("normalize succeeded, want error")
			}
		})
	}
}

func ptrInt(value int) *int {
	return &value
}

func ptrDuration(value time.Duration) *time.Duration {
	return &value
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
