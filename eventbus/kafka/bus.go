package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

// messageProducer is the narrow Sarama producer surface used by Bus.
type messageProducer interface {
	// SendMessage writes one Kafka record and returns the broker-assigned location.
	SendMessage(*sarama.ProducerMessage) (partition int32, offset int64, err error)
	// Close releases producer network resources.
	Close() error
}

// consumerGroupFactory opens a Sarama consumer group for Subscribe.
type consumerGroupFactory func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error)

// Bus implements eventbus.EventBus with Kafka producer and consumer groups.
type Bus struct {
	opts             Options                       // opts stores normalized Kafka provider settings
	producer         messageProducer               // producer publishes primary and dead-letter records
	newConsumerGroup consumerGroupFactory          // newConsumerGroup opens Sarama consumer groups
	commits          *orderedCommitManager         // commits owns per-topic-partition ordered offset completion
	gates            *messageCompletionGateTracker // gates reports per-message async completion pressure
	protector        *consumptionProtector         // protector pauses/resumes Kafka claims under local pressure
	pool             *dynamicWorkerPool            // pool bounds concurrent handler execution
	metrics          kafkaMetrics                  // metrics stores provider-owned retry, DLQ, and delivery counters
	closeOnce        sync.Once                     // closeOnce makes Close idempotent across shutdown paths
}

// Stats reports Kafka EventBus runtime state without exposing Sarama internals.
//
// NOTE: Stats is a diagnostic snapshot for operators and tests. It is not a
// billing, audit, or exactly-once accounting surface.
type Stats struct {
	Topic           string               // Topic is the primary analytics event topic
	DeadLetterTopic string               // DeadLetterTopic is the configured DLQ topic
	WorkerPool      WorkerPoolStats      // WorkerPool reports bounded handler execution pressure
	CompletionGate  CompletionGateStats  // CompletionGate reports in-flight message and task pressure
	Commits         []OrderedCommitStats // Commits reports per topic-partition ordered commit state
	Paused          map[string][]int32   // Paused lists partitions currently paused by local backpressure
	Metrics         MetricsStats         // Metrics reports provider-owned delivery, retry, and DLQ counters
}

// WorkerPoolStats reports Kafka handler pool pressure.
type WorkerPoolStats struct {
	Name            string  // Name identifies this pool in diagnostics
	GoroutinesTotal int     // GoroutinesTotal is runtime.NumGoroutine at sampling time
	Queued          int64   // Queued is the current number of queued tasks
	QueueCapacity   int     // QueueCapacity is the bounded work queue capacity
	QueueUsageRatio float64 // QueueUsageRatio is Queued divided by QueueCapacity
	Workers         int     // Workers is the fixed handler worker count
	SubmittedTotal  int64   // SubmittedTotal is the lifetime accepted task count
	CompletedTotal  int64   // CompletedTotal is the lifetime completed task count
	RejectedTotal   int64   // RejectedTotal is the lifetime rejected task count
	Closed          bool    // Closed reports whether shutdown has started
}

// CompletionGateStats reports message completion gate pressure.
type CompletionGateStats struct {
	InFlightMessages  int64 // InFlightMessages is the number of messages not yet completed
	WaitingTasks      int64 // WaitingTasks is the number of unfinished async tasks
	CompletedMessages int64 // CompletedMessages is the lifetime count of completed messages
}

// OrderedCommitStats reports one topic-partition's ordered commit state.
type OrderedCommitStats struct {
	Topic               string // Topic is the Kafka topic name
	Partition           int32  // Partition is the Kafka partition id
	Initialized         bool   // Initialized reports whether this partition has seen any offset
	NextOffset          int64  // NextOffset is the earliest offset still blocking ordered completion
	HighWaterMarkOffset int64  // HighWaterMarkOffset is the latest claim high-water mark observed from Sarama
	Lag                 int64  // Lag estimates unprocessed records as HighWaterMarkOffset minus NextOffset
	PendingCount        int    // PendingCount is the number of registered unmarked offsets
	DoneCount           int    // DoneCount is the number of completed offsets waiting for earlier offsets
	OldestPendingOffset int64  // OldestPendingOffset is the earliest registered offset still pending
	LargestPendingGap   int64  // LargestPendingGap is the largest observed registration gap
}

// MetricsStats reports provider-owned Kafka delivery counters.
type MetricsStats struct {
	ConsumedTotal          int64 // ConsumedTotal counts primary topic records pulled from Kafka
	HandlerSuccessTotal    int64 // HandlerSuccessTotal counts records completed through handler success
	HandlerFailureTotal    int64 // HandlerFailureTotal counts failed handler attempts
	HandlerRetryTotal      int64 // HandlerRetryTotal counts handler attempts scheduled after a previous failure
	MalformedTotal         int64 // MalformedTotal counts records that could not decode as EventEnvelope
	DeadLetterSuccessTotal int64 // DeadLetterSuccessTotal counts successful DLQ writes
	DeadLetterFailureTotal int64 // DeadLetterFailureTotal counts failed DLQ write attempts
	PausedPartitions       int64 // PausedPartitions counts currently paused topic-partitions
	PauseTransitionsTotal  int64 // PauseTransitionsTotal counts protector pause transitions
	ResumeTransitionsTotal int64 // ResumeTransitionsTotal counts protector resume transitions
}

// kafkaMetrics stores atomic counters for provider observability.
type kafkaMetrics struct {
	consumedTotal          int64 // consumedTotal counts messages fetched from Kafka
	handlerSuccessTotal    int64 // handlerSuccessTotal counts handler success completions
	handlerFailureTotal    int64 // handlerFailureTotal counts failed handler attempts
	handlerRetryTotal      int64 // handlerRetryTotal counts retry scheduling decisions
	malformedTotal         int64 // malformedTotal counts decode failures on the primary topic
	deadLetterSuccessTotal int64 // deadLetterSuccessTotal counts successful DLQ publishes
	deadLetterFailureTotal int64 // deadLetterFailureTotal counts failed DLQ publish attempts
	pauseTransitionsTotal  int64 // pauseTransitionsTotal counts local pause transitions
	resumeTransitionsTotal int64 // resumeTransitionsTotal counts local resume transitions
}

// New creates a Kafka EventBus backed by IBM Sarama.
func New(opts Options) (*Bus, error) {
	normalized, err := opts.normalize()
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewSyncProducer(normalized.Brokers, newSaramaConfig(normalized))
	if err != nil {
		return nil, err
	}
	pool, err := newDynamicWorkerPool(workerPoolConfig{
		name:      "kafka-eventbus-handler",
		workers:   normalized.Workers,
		queueSize: normalized.QueueSize,
	})
	if err != nil {
		_ = producer.Close()
		return nil, err
	}
	return newBusWithDependencies(normalized, producer, sarama.NewConsumerGroup, pool), nil
}

// newBusWithDependencies wires testable provider dependencies behind Bus.
func newBusWithDependencies(opts Options, producer messageProducer, factory consumerGroupFactory, pool *dynamicWorkerPool) *Bus {
	bus := &Bus{
		opts:             opts,
		producer:         producer,
		newConsumerGroup: factory,
		commits:          newOrderedCommitManager(),
		gates:            &messageCompletionGateTracker{},
		pool:             pool,
	}
	bus.protector = newConsumptionProtectorWithMetrics(&bus.metrics)
	return bus
}

// Publish appends envelope as JSON to the configured Kafka topic.
func (b *Bus) Publish(ctx context.Context, envelope contracts.EventEnvelope) error {
	if b == nil || b.producer == nil {
		return errors.New("kafka bus is not initialized")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Keep the producer payload identical to other EventBus adapters: the queue
	// body is the stable contracts.EventEnvelope JSON, not a Sarama-specific
	// wrapper or service-local DTO.
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, _, err = b.producer.SendMessage(&sarama.ProducerMessage{
		Topic: b.opts.Topic,
		Key:   sarama.StringEncoder(eventKey(envelope)),
		Value: sarama.ByteEncoder(payload),
	})
	return err
}

// Subscribe consumes Kafka messages for group and delegates them to handler.
func (b *Bus) Subscribe(ctx context.Context, group eventbus.ConsumerGroup, handler eventbus.Handler) error {
	if b == nil {
		return errors.New("kafka bus is nil")
	}
	if group.Name == "" {
		return errors.New("consumer group name is required")
	}
	if group.Consumer == "" {
		return errors.New("consumer name is required")
	}
	if handler == nil {
		return errors.New("handler is required")
	}
	if b.newConsumerGroup == nil {
		b.newConsumerGroup = sarama.NewConsumerGroup
	}

	consumerGroup, err := b.newConsumerGroup(b.opts.Brokers, group.Name, newSaramaConfig(b.opts))
	if err != nil {
		return err
	}

	adapter := &sessionAdapter{
		bus:           b,
		group:         group,
		handler:       handler,
		consumerGroup: consumerGroup,
	}
	errorCtx, stopErrors := context.WithCancel(ctx)
	errorDone := drainConsumerGroupErrors(errorCtx, consumerGroup)
	defer func() {
		stopErrors()
		if errorDone != nil {
			<-errorDone
		}
	}()
	defer consumerGroup.Close()

	for ctx.Err() == nil {
		// Sarama returns from Consume after every rebalance. Re-entering keeps
		// the subscription active while adapter-owned commit/protector state
		// remains outside the public EventBus contract.
		if err := consumerGroup.Consume(ctx, []string{b.opts.Topic}, adapter); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Close releases Kafka producer and worker resources owned by the bus.
func (b *Bus) Close() error {
	if b == nil {
		return nil
	}
	var err error
	b.closeOnce.Do(func() {
		if b.pool != nil {
			err = errors.Join(err, b.pool.Close())
		}
		if b.producer != nil {
			err = errors.Join(err, b.producer.Close())
		}
	})
	return err
}

// Stats returns a diagnostic snapshot of provider-owned Kafka state.
func (b *Bus) Stats() Stats {
	if b == nil {
		return Stats{}
	}
	// Snapshot each internal subsystem before projecting into public structs so
	// callers never receive mutable provider maps or Sarama-owned state.
	snapshot := b.snapshot()
	paused := b.protector.Snapshot()
	return Stats{
		Topic:           b.opts.Topic,
		DeadLetterTopic: b.opts.DeadLetterTopic,
		WorkerPool:      publicWorkerPoolStats(snapshot.Pool),
		CompletionGate:  publicCompletionGateStats(snapshot.Gate),
		Commits:         publicOrderedCommitStats(snapshot.Commits),
		Paused:          paused,
		Metrics:         b.publicMetricsStats(paused),
	}
}

// newSaramaConfig maps provider options into Sarama's producer and consumer behavior.
func newSaramaConfig(opts Options) *sarama.Config {
	// Configure producer acknowledgements for durable broker acceptance before
	// Publish returns success to collect handlers.
	config := sarama.NewConfig()
	config.ClientID = opts.ClientID
	config.Producer.RequiredAcks = saramaRequiredAcks(opts.ProducerAcks)
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = *opts.ProducerRetryMax
	config.Producer.Retry.Backoff = *opts.ProducerRetryBackoff
	config.Producer.Flush.Bytes = opts.ProducerFlushBytes
	config.Producer.Flush.Messages = opts.ProducerFlushMessages
	config.Producer.Flush.Frequency = opts.ProducerFlushFrequency
	config.Producer.Flush.MaxMessages = opts.ProducerFlushMaxMessages
	config.Net.TLS.Enable = opts.TLSEnabled
	config.Net.TLS.Config = opts.TLSConfig
	if opts.SASLEnabled {
		// SASL stays provider-owned just like sessions and offset commits. The
		// public Options type exposes analytics-core names while Sarama's mechanism
		// constants remain below this adapter boundary.
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = opts.SASLUsername
		config.Net.SASL.Password = opts.SASLPassword
		if opts.SASLHandshake != nil {
			config.Net.SASL.Handshake = *opts.SASLHandshake
		}
	}
	if opts.IdempotentProducer {
		// Sarama requires a single in-flight request for idempotent writes. Keep
		// this behind an explicit provider option because it can reduce throughput
		// and changes broker ACL requirements through IdempotentWrite.
		config.Producer.Idempotent = true
		config.Net.MaxOpenRequests = 1
	}

	// Auto commit is intentionally left enabled. The ordered committer controls
	// when MarkMessage is called; Sarama then flushes those marks on interval.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = opts.CommitInterval
	config.Consumer.Return.Errors = true
	return config
}

// saramaRequiredAcks converts public provider acknowledgement names to Sarama values.
func saramaRequiredAcks(acks ProducerAcks) sarama.RequiredAcks {
	switch acks {
	case ProducerAcksNone:
		return sarama.NoResponse
	case ProducerAcksLeader:
		return sarama.WaitForLocal
	default:
		return sarama.WaitForAll
	}
}

// drainConsumerGroupErrors consumes Sarama's error channel to avoid hidden consumer stalls.
func drainConsumerGroupErrors(ctx context.Context, group sarama.ConsumerGroup) <-chan struct{} {
	if group == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-group.Errors():
				if !ok {
					return
				}
			}
		}
	}()
	return done
}

// eventKey returns the stable Kafka key used to keep one source on a partition when possible.
func eventKey(envelope contracts.EventEnvelope) string {
	scope := strings.Join([]string{envelope.TenantID, envelope.ProjectID, envelope.SourceID}, ":")
	if strings.Trim(scope, ":") != "" {
		return scope
	}
	if envelope.ID != "" {
		return envelope.ID
	}
	return envelope.EventName
}

// sessionAdapter keeps Sarama consumer-group callbacks inside the Kafka provider.
type sessionAdapter struct {
	bus           *Bus                   // bus owns producer, commit, gate, pool, and protector state
	group         eventbus.ConsumerGroup // group is the public analytics-core consumer identity
	handler       eventbus.Handler       // handler is the public EventBus callback
	consumerGroup sarama.ConsumerGroup   // consumerGroup is used only for pause/resume and close behavior
}

// Setup restores any provider-owned pause state after a Sarama rebalance.
func (a *sessionAdapter) Setup(sarama.ConsumerGroupSession) error {
	a.bus.protector.reconcile(a.consumerGroup, a.bus.snapshot())
	return nil
}

// Cleanup runs after Sarama finishes a rebalance claim.
func (a *sessionAdapter) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim registers offsets, schedules handler work, and gates ordered marks.
func (a *sessionAdapter) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	committer := a.bus.commits.Get(claim.Topic(), claim.Partition())
	generationID := session.GenerationID()
	defer committer.AbortGeneration(generationID)
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			msg := message
			a.bus.commits.RecordHighWaterMark(msg.Topic, msg.Partition, claim.HighWaterMarkOffset())
			atomic.AddInt64(&a.bus.metrics.consumedTotal, 1)
			committer.Register(msg.Offset, generationID, func() {
				session.MarkMessage(msg, "")
			})
			gate := newMessageCompletionGate(msg.Offset, generationID, committer, a.bus.gates)

			// Submit blocks behind the bounded queue, which is the first local
			// backpressure layer before the protector escalates to Kafka pause.
			if err := a.bus.pool.Submit(session.Context(), func() {
				a.bus.processConsumerMessageWithHandler(session.Context(), a.group, a.handler, msg, gate)
				a.bus.protector.observe(a.consumerGroup, a.bus.snapshot())
			}); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			a.bus.protector.observe(a.consumerGroup, a.bus.snapshot())
		}
	}
}

// processConsumerMessageWithHandler executes retry, DLQ, and ordered-completion semantics.
func (b *Bus) processConsumerMessageWithHandler(ctx context.Context, group eventbus.ConsumerGroup, handler eventbus.Handler, message *sarama.ConsumerMessage, gate *messageCompletionGate) {
	// Decode before invoking user code. Malformed primary records never reach
	// the handler; they are isolated to DLQ under the provider contract.
	var envelope contracts.EventEnvelope
	if err := json.Unmarshal(message.Value, &envelope); err != nil {
		atomic.AddInt64(&b.metrics.malformedTotal, 1)
		if b.deadLetterUntilDone(ctx, group, message, 1, envelope, err, message.Value) {
			gate.NoAsyncTaskCompleteNow()
		}
		return
	}

	gate.AddTask()
	for attempt := 1; ; attempt++ {
		// Observe cancellation before every handler attempt so shutdown does not
		// start new business work after the consumer session is closing.
		select {
		case <-ctx.Done():
			return
		default:
		}

		// The public handler only receives queue metadata and returns an error.
		// Provider-owned retry/DLQ/commit semantics stay below this boundary.
		err := callHandler(ctx, handler, eventbus.Message{
			ID:        messageID(message),
			Topic:     message.Topic,
			Partition: message.Partition,
			Offset:    message.Offset,
			Key:       string(message.Key),
			Attempt:   attempt,
			Envelope:  envelope,
		})
		if err == nil {
			atomic.AddInt64(&b.metrics.handlerSuccessTotal, 1)
			gate.TaskDone()
			return
		}
		atomic.AddInt64(&b.metrics.handlerFailureTotal, 1)
		if attempt >= b.opts.MaxAttempts {
			// Exhausted messages become complete only after the DLQ publish has
			// succeeded, preserving at-least-once visibility for poison records.
			if b.deadLetterUntilDone(ctx, group, message, attempt, envelope, err, nil) {
				gate.TaskDone()
			}
			return
		}
		atomic.AddInt64(&b.metrics.handlerRetryTotal, 1)
		if !sleepContext(ctx, b.opts.RetryBackoff) {
			return
		}
	}
}

// callHandler converts handler panics into retryable provider errors.
func callHandler(ctx context.Context, handler eventbus.Handler, message eventbus.Message) (err error) {
	// Panic recovery belongs at the provider boundary. Treating a handler panic
	// as a failed attempt lets normal retry/DLQ rules run, so the worker pool's
	// last-ditch recover is not responsible for queue correctness.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("kafka handler panic: %v", recovered)
		}
	}()
	return handler(ctx, message)
}

// deadLetterUntilDone retries DLQ publishing until success or context cancellation.
func (b *Bus) deadLetterUntilDone(ctx context.Context, group eventbus.ConsumerGroup, message *sarama.ConsumerMessage, attempt int, envelope contracts.EventEnvelope, cause error, raw []byte) bool {
	for {
		// DLQ is the durable failure boundary. The original offset is not marked
		// complete until this write succeeds; otherwise the message replays after
		// restart instead of disappearing from both primary and dead-letter topics.
		if err := b.publishDeadLetter(ctx, group, message, attempt, envelope, cause, raw); err == nil {
			atomic.AddInt64(&b.metrics.deadLetterSuccessTotal, 1)
			return true
		}
		atomic.AddInt64(&b.metrics.deadLetterFailureTotal, 1)
		if !sleepContext(ctx, b.opts.RetryBackoff) {
			return false
		}
	}
}

// publishDeadLetter writes one diagnostic record to the configured DLQ topic.
func (b *Bus) publishDeadLetter(ctx context.Context, group eventbus.ConsumerGroup, message *sarama.ConsumerMessage, attempt int, envelope contracts.EventEnvelope, cause error, raw []byte) error {
	if b.opts.DeadLetterTopic == "" {
		return errors.New("kafka dead-letter topic is required")
	}
	// Honor cancellation before serializing the diagnostic payload so shutdown
	// can leave the original message unmarked for replay.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	payload, err := json.Marshal(deadLetterRecord{
		Envelope:          envelope,
		Raw:               raw,
		OriginalTopic:     message.Topic,
		OriginalPartition: message.Partition,
		OriginalOffset:    message.Offset,
		OriginalKey:       string(message.Key),
		ConsumerGroup:     group.Name,
		ConsumerID:        group.Consumer,
		Attempt:           attempt,
		Error:             cause.Error(),
		FailedAt:          time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	_, _, err = b.producer.SendMessage(&sarama.ProducerMessage{
		Topic: b.opts.DeadLetterTopic,
		Key:   sarama.ByteEncoder(message.Key),
		Value: sarama.ByteEncoder(payload),
	})
	return err
}

// messageID returns a provider-native delivery identifier for public metadata.
func messageID(message *sarama.ConsumerMessage) string {
	if message == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d:%d", message.Topic, message.Partition, message.Offset)
}

// snapshot returns current pressure signals for pause/resume decisions.
func (b *Bus) snapshot() consumptionSnapshot {
	return consumptionSnapshot{
		Commits: b.commits.Snapshots(),
		Gate:    b.gates.Snapshot(),
		Pool:    b.pool.Stats(),
	}
}

// publicWorkerPoolStats projects internal worker stats into the public snapshot type.
func publicWorkerPoolStats(stats workerPoolStats) WorkerPoolStats {
	return WorkerPoolStats{
		Name:            stats.Name,
		GoroutinesTotal: stats.GoroutinesTotal,
		Queued:          stats.Queued,
		QueueCapacity:   stats.QueueCapacity,
		QueueUsageRatio: stats.QueueUsageRatio,
		Workers:         stats.Workers,
		SubmittedTotal:  stats.SubmittedTotal,
		CompletedTotal:  stats.CompletedTotal,
		RejectedTotal:   stats.RejectedTotal,
		Closed:          stats.Closed,
	}
}

// publicCompletionGateStats projects internal gate stats into the public snapshot type.
func publicCompletionGateStats(stats messageCompletionGateSnapshot) CompletionGateStats {
	return CompletionGateStats{
		InFlightMessages:  stats.InFlightMessages,
		WaitingTasks:      stats.WaitingTasks,
		CompletedMessages: stats.CompletedMessages,
	}
}

// publicOrderedCommitStats projects ordered commit state into a stable diagnostic shape.
func publicOrderedCommitStats(snapshots []orderedCommitSnapshot) []OrderedCommitStats {
	stats := make([]OrderedCommitStats, 0, len(snapshots))
	for _, snapshot := range snapshots {
		topic, partition, ok := splitCommitKey(snapshot.Key)
		if !ok {
			continue
		}
		lag := int64(0)
		if snapshot.HighWaterMarkOffset > snapshot.NextOffset {
			lag = snapshot.HighWaterMarkOffset - snapshot.NextOffset
		}
		stats = append(stats, OrderedCommitStats{
			Topic:               topic,
			Partition:           partition,
			Initialized:         snapshot.Initialized,
			NextOffset:          snapshot.NextOffset,
			HighWaterMarkOffset: snapshot.HighWaterMarkOffset,
			Lag:                 lag,
			PendingCount:        snapshot.PendingCount,
			DoneCount:           snapshot.DoneCount,
			OldestPendingOffset: snapshot.OldestPendingOffset,
			LargestPendingGap:   snapshot.LargestPendingGap,
		})
	}
	return stats
}

// publicMetricsStats reads atomic provider counters into the public snapshot type.
func (b *Bus) publicMetricsStats(paused map[string][]int32) MetricsStats {
	return MetricsStats{
		ConsumedTotal:          atomic.LoadInt64(&b.metrics.consumedTotal),
		HandlerSuccessTotal:    atomic.LoadInt64(&b.metrics.handlerSuccessTotal),
		HandlerFailureTotal:    atomic.LoadInt64(&b.metrics.handlerFailureTotal),
		HandlerRetryTotal:      atomic.LoadInt64(&b.metrics.handlerRetryTotal),
		MalformedTotal:         atomic.LoadInt64(&b.metrics.malformedTotal),
		DeadLetterSuccessTotal: atomic.LoadInt64(&b.metrics.deadLetterSuccessTotal),
		DeadLetterFailureTotal: atomic.LoadInt64(&b.metrics.deadLetterFailureTotal),
		PausedPartitions:       countPartitions(paused),
		PauseTransitionsTotal:  atomic.LoadInt64(&b.metrics.pauseTransitionsTotal),
		ResumeTransitionsTotal: atomic.LoadInt64(&b.metrics.resumeTransitionsTotal),
	}
}

// countPartitions returns the total partition entries in a pause snapshot.
func countPartitions(partitions map[string][]int32) int64 {
	total := int64(0)
	for _, values := range partitions {
		total += int64(len(values))
	}
	return total
}

// sleepContext waits for duration unless ctx is cancelled first.
func sleepContext(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// deadLetterRecord is the JSON payload written to the Kafka DLQ topic.
type deadLetterRecord struct {
	Envelope          contracts.EventEnvelope `json:"envelope"`               // Envelope is the decoded analytics event when decoding succeeded
	Raw               []byte                  `json:"raw,omitempty"`          // Raw is the original payload when decoding failed before handler execution
	OriginalTopic     string                  `json:"original_topic"`         // OriginalTopic is the source Kafka topic
	OriginalPartition int32                   `json:"original_partition"`     // OriginalPartition is the source Kafka partition
	OriginalOffset    int64                   `json:"original_offset"`        // OriginalOffset is the source Kafka offset
	OriginalKey       string                  `json:"original_key,omitempty"` // OriginalKey is the source Kafka record key
	ConsumerGroup     string                  `json:"consumer_group"`         // ConsumerGroup is the public EventBus group name
	ConsumerID        string                  `json:"consumer_id"`            // ConsumerID is the public EventBus consumer identity
	Attempt           int                     `json:"attempt"`                // Attempt is the final handler or decode attempt count
	Error             string                  `json:"error"`                  // Error is the failure summary captured for operators
	FailedAt          time.Time               `json:"failed_at"`              // FailedAt is the UTC time when the DLQ record was produced
}
