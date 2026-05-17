package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"sync"
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

func TestNewSaramaConfigMapsTLSAndSASLPlain(t *testing.T) {
	handshake := false
	tlsConfig := &tls.Config{ServerName: "kafka.example.com", MinVersion: tls.VersionTLS12}
	opts, err := Options{
		Brokers:       []string{"127.0.0.1:9092"},
		TLSEnabled:    true,
		TLSConfig:     tlsConfig,
		SASLEnabled:   true,
		SASLMechanism: SASLMechanismPlain,
		SASLUsername:  "eventbus-user",
		SASLPassword:  "eventbus-password",
		SASLHandshake: &handshake,
	}.normalize()
	if err != nil {
		t.Fatalf("normalize options failed: %v", err)
	}
	config := newSaramaConfig(opts)

	if !config.Net.TLS.Enable || config.Net.TLS.Config != tlsConfig {
		t.Fatalf("unexpected TLS config: enabled=%v config=%p", config.Net.TLS.Enable, config.Net.TLS.Config)
	}
	if !config.Net.SASL.Enable {
		t.Fatal("SASL is disabled")
	}
	if config.Net.SASL.Mechanism != sarama.SASLTypePlaintext {
		t.Fatalf("SASL mechanism = %q, want %q", config.Net.SASL.Mechanism, sarama.SASLTypePlaintext)
	}
	if config.Net.SASL.User != "eventbus-user" || config.Net.SASL.Password != "eventbus-password" {
		t.Fatalf("unexpected SASL credentials: user=%q password=%q", config.Net.SASL.User, config.Net.SASL.Password)
	}
	if config.Net.SASL.Handshake {
		t.Fatal("SASL handshake override was not applied")
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
		{name: "unsupported sasl mechanism", opts: Options{Brokers: []string{"127.0.0.1:9092"}, SASLEnabled: true, SASLMechanism: SASLMechanism("scram-sha-256"), SASLUsername: "user", SASLPassword: "password"}},
		{name: "sasl missing username", opts: Options{Brokers: []string{"127.0.0.1:9092"}, SASLEnabled: true, SASLPassword: "password"}},
		{name: "sasl missing password", opts: Options{Brokers: []string{"127.0.0.1:9092"}, SASLEnabled: true, SASLUsername: "user"}},
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

func TestStatsReportsRetryDLQLagAndPauseState(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)
	bus.opts.MaxAttempts = 2
	bus.opts.RetryBackoff = time.Nanosecond

	committer := bus.commits.Get("analytics.events", 1)
	committer.Register(10, 1, nil)
	committer.Register(11, 1, nil)
	bus.commits.RecordHighWaterMark("analytics.events", 1, 15)
	bus.protector.observe(&fakeConsumerGroup{}, consumptionSnapshot{Commits: []orderedCommitSnapshot{
		{Key: "analytics.events:1", PendingCount: defaultHardPendingCount},
	}})
	message := testConsumerMessage(t, "evt_stats")
	message.Offset = 10
	gate := newMessageCompletionGate(message.Offset, 1, committer, bus.gates)
	bus.processConsumerMessageWithHandler(context.Background(), eventbus.ConsumerGroup{Name: "group", Consumer: "consumer"}, func(context.Context, eventbus.Message) error {
		return errors.New("permanent failure")
	}, message, gate)

	stats := bus.Stats()
	if stats.Topic != "analytics.events" || stats.DeadLetterTopic != "analytics.events.dead" {
		t.Fatalf("unexpected stats topics: %#v", stats)
	}
	if stats.Metrics.HandlerFailureTotal != 2 || stats.Metrics.HandlerRetryTotal != 1 || stats.Metrics.DeadLetterSuccessTotal != 1 {
		t.Fatalf("unexpected retry/DLQ metrics: %#v", stats.Metrics)
	}
	if stats.Metrics.PausedPartitions != 1 || stats.Metrics.PauseTransitionsTotal != 1 {
		t.Fatalf("unexpected pause metrics: %#v paused=%#v", stats.Metrics, stats.Paused)
	}
	if len(stats.Commits) != 1 {
		t.Fatalf("commit stats count = %d, want 1", len(stats.Commits))
	}
	commit := stats.Commits[0]
	if commit.Topic != "analytics.events" || commit.Partition != 1 || commit.NextOffset != 11 || commit.HighWaterMarkOffset != 15 || commit.Lag != 4 {
		t.Fatalf("unexpected commit stats: %#v", commit)
	}
	if stats.CompletionGate.CompletedMessages != 1 {
		t.Fatalf("completed gates = %d, want 1", stats.CompletionGate.CompletedMessages)
	}
}

func TestStatsReportsResumeTransition(t *testing.T) {
	bus := newTestBus(t, &recordingProducer{})
	group := &fakeConsumerGroup{}

	bus.protector.observe(group, consumptionSnapshot{Commits: []orderedCommitSnapshot{
		{Key: "analytics.events:1", PendingCount: defaultHardPendingCount},
	}})
	bus.protector.observe(group, consumptionSnapshot{})

	stats := bus.Stats()
	if stats.Metrics.PausedPartitions != 0 || len(stats.Paused) != 0 {
		t.Fatalf("unexpected paused state after resume: metrics=%#v paused=%#v", stats.Metrics, stats.Paused)
	}
	if stats.Metrics.PauseTransitionsTotal != 1 || stats.Metrics.ResumeTransitionsTotal != 1 {
		t.Fatalf("unexpected pause/resume counters: %#v", stats.Metrics)
	}
}

func TestStatsReportsDLQWriteFailureWithoutCompletion(t *testing.T) {
	producer := &recordingProducer{err: errors.New("kafka unavailable")}
	bus := newTestBus(t, producer)
	bus.opts.MaxAttempts = 1
	bus.opts.RetryBackoff = time.Nanosecond
	gate, completed := newRegisteredGate(60)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	bus.processConsumerMessageWithHandler(ctx, eventbus.ConsumerGroup{Name: "g1", Consumer: "c1"}, func(context.Context, eventbus.Message) error {
		return errors.New("permanent failure")
	}, testConsumerMessage(t, "evt_stats_dlq_failed"), gate)

	stats := bus.Stats()
	if stats.Metrics.DeadLetterFailureTotal == 0 {
		t.Fatalf("expected DLQ failure metric, got %#v", stats.Metrics)
	}
	if *completed != 0 || stats.CompletionGate.CompletedMessages != 0 {
		t.Fatalf("message completed despite DLQ failure: completed=%d stats=%#v", *completed, stats.CompletionGate)
	}
}

func TestConsumeClaimAbortsGenerationWhenClaimCloses(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	adapter := &sessionAdapter{
		bus:   bus,
		group: eventbus.ConsumerGroup{Name: "group", Consumer: "consumer"},
		handler: func(context.Context, eventbus.Message) error {
			close(handlerStarted)
			<-releaseHandler
			return nil
		},
		consumerGroup: &fakeConsumerGroup{},
	}
	session := newFakeConsumerGroupSession(7)
	claim := newFakeConsumerGroupClaim("analytics.events", 1, nil)

	message := testConsumerMessage(t, "evt_claim_closed")
	claim.messages <- message
	close(claim.messages)
	if err := adapter.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("consume claim failed: %v", err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start before claim close")
	}
	if marked := session.markedOffsets(); len(marked) != 0 {
		t.Fatalf("marked offsets after closed claim = %+v, want none", marked)
	}

	committer := bus.commits.Get(message.Topic, message.Partition)
	committer.Complete(message.Offset, session.GenerationID())
	close(releaseHandler)
	waitForPoolStat(t, bus.pool, func(stats workerPoolStats) bool {
		return stats.CompletedTotal == 1
	})
	if marked := session.markedOffsets(); len(marked) != 0 {
		t.Fatalf("marked stale generation after claim close: %+v", marked)
	}
}

func TestSessionAdapterSetupClearsStalePauseWithoutPressure(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)
	group := &fakeConsumerGroup{}
	adapter := &sessionAdapter{bus: bus, consumerGroup: group}

	bus.protector.observe(group, consumptionSnapshot{Commits: []orderedCommitSnapshot{
		{Key: "analytics.events:1", PendingCount: defaultHardPendingCount},
	}})
	bus.commits.Get("analytics.events", 1).AbortGeneration(0)
	if err := adapter.Setup(newFakeConsumerGroupSession(1)); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	resumes := group.resumeSnapshots()
	if len(resumes) != 1 || len(resumes[0]["analytics.events"]) != 1 || resumes[0]["analytics.events"][0] != 1 {
		t.Fatalf("setup resumes = %+v, want analytics.events partition 1", resumes)
	}
	stats := bus.Stats()
	if stats.Metrics.PausedPartitions != 0 || stats.Metrics.ResumeTransitionsTotal != 1 {
		t.Fatalf("unexpected stats after stale pause clear: %#v", stats.Metrics)
	}
}

func TestSessionAdapterSetupRestoresPauseWhenPressureRemains(t *testing.T) {
	producer := &recordingProducer{}
	bus := newTestBus(t, producer)
	group := &fakeConsumerGroup{}
	adapter := &sessionAdapter{bus: bus, consumerGroup: group}

	committer := bus.commits.Get("analytics.events", 1)
	for offset := int64(0); offset < defaultHardPendingCount; offset++ {
		committer.Register(offset, 1, nil)
	}
	bus.protector.observe(group, bus.snapshot())
	group.clear()
	if err := adapter.Setup(newFakeConsumerGroupSession(1)); err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	pauses := group.pauseSnapshots()
	if len(pauses) != 1 || len(pauses[0]["analytics.events"]) != 1 || pauses[0]["analytics.events"][0] != 1 {
		t.Fatalf("restored pauses = %+v, want analytics.events partition 1", pauses)
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

type fakeConsumerGroupSession struct {
	ctx          context.Context
	generationID int32
	mu           sync.Mutex
	marked       []int64
}

func newFakeConsumerGroupSession(generationID int32) *fakeConsumerGroupSession {
	return &fakeConsumerGroupSession{ctx: context.Background(), generationID: generationID}
}

func (s *fakeConsumerGroupSession) Claims() map[string][]int32 { return nil }

func (s *fakeConsumerGroupSession) MemberID() string { return "member" }

func (s *fakeConsumerGroupSession) GenerationID() int32 { return s.generationID }

func (s *fakeConsumerGroupSession) MarkOffset(string, int32, int64, string) {}

func (s *fakeConsumerGroupSession) Commit() {}

func (s *fakeConsumerGroupSession) ResetOffset(string, int32, int64, string) {}

func (s *fakeConsumerGroupSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, message.Offset)
}

func (s *fakeConsumerGroupSession) Context() context.Context { return s.ctx }

func (s *fakeConsumerGroupSession) markedOffsets() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.marked...)
}

type fakeConsumerGroupClaim struct {
	topic     string
	partition int32
	messages  chan *sarama.ConsumerMessage
}

func newFakeConsumerGroupClaim(topic string, partition int32, messages []*sarama.ConsumerMessage) *fakeConsumerGroupClaim {
	claim := &fakeConsumerGroupClaim{topic: topic, partition: partition, messages: make(chan *sarama.ConsumerMessage, len(messages)+1)}
	for _, message := range messages {
		claim.messages <- message
	}
	return claim
}

func (c *fakeConsumerGroupClaim) Topic() string { return c.topic }

func (c *fakeConsumerGroupClaim) Partition() int32 { return c.partition }

func (c *fakeConsumerGroupClaim) InitialOffset() int64 { return 0 }

func (c *fakeConsumerGroupClaim) HighWaterMarkOffset() int64 { return 0 }

func (c *fakeConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

type fakeConsumerGroup struct {
	mu      sync.Mutex
	pauses  []map[string][]int32
	resumes []map[string][]int32
}

func (g *fakeConsumerGroup) Consume(context.Context, []string, sarama.ConsumerGroupHandler) error {
	return nil
}

func (g *fakeConsumerGroup) Errors() <-chan error { return make(chan error) }

func (g *fakeConsumerGroup) Close() error { return nil }

func (g *fakeConsumerGroup) Pause(partitions map[string][]int32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pauses = append(g.pauses, copyPartitions(partitions))
}

func (g *fakeConsumerGroup) Resume(partitions map[string][]int32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.resumes = append(g.resumes, copyPartitions(partitions))
}

func (g *fakeConsumerGroup) PauseAll() {}

func (g *fakeConsumerGroup) ResumeAll() {}

func (g *fakeConsumerGroup) clear() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pauses = nil
	g.resumes = nil
}

func (g *fakeConsumerGroup) pauseSnapshots() []map[string][]int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]map[string][]int32, 0, len(g.pauses))
	for _, pause := range g.pauses {
		out = append(out, copyPartitions(pause))
	}
	return out
}

func (g *fakeConsumerGroup) resumeSnapshots() []map[string][]int32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]map[string][]int32, 0, len(g.resumes))
	for _, resume := range g.resumes {
		out = append(out, copyPartitions(resume))
	}
	return out
}
