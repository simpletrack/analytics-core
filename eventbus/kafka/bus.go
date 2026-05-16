package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	closeOnce        sync.Once                     // closeOnce makes Close idempotent across shutdown paths
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
	return &Bus{
		opts:             opts,
		producer:         producer,
		newConsumerGroup: factory,
		commits:          newOrderedCommitManager(),
		gates:            &messageCompletionGateTracker{},
		protector:        newConsumptionProtector(),
		pool:             pool,
	}
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

// newSaramaConfig maps provider options into Sarama's producer and consumer behavior.
func newSaramaConfig(opts Options) *sarama.Config {
	// Configure producer acknowledgements for durable broker acceptance before
	// Publish returns success to collect handlers.
	config := sarama.NewConfig()
	config.ClientID = opts.ClientID
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 3

	// Auto commit is intentionally left enabled. The ordered committer controls
	// when MarkMessage is called; Sarama then flushes those marks on interval.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = opts.CommitInterval
	config.Consumer.Return.Errors = true
	return config
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
	a.bus.protector.restore(a.consumerGroup)
	return nil
}

// Cleanup runs after Sarama finishes a rebalance claim.
func (a *sessionAdapter) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim registers offsets, schedules handler work, and gates ordered marks.
func (a *sessionAdapter) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	committer := a.bus.commits.Get(claim.Topic(), claim.Partition())
	for {
		select {
		case <-session.Context().Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			msg := message
			committer.Register(msg.Offset, session.GenerationID(), func() {
				session.MarkMessage(msg, "")
			})
			gate := newMessageCompletionGate(msg.Offset, session.GenerationID(), committer, a.bus.gates)

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
			gate.TaskDone()
			return
		}
		if attempt >= b.opts.MaxAttempts {
			// Exhausted messages become complete only after the DLQ publish has
			// succeeded, preserving at-least-once visibility for poison records.
			if b.deadLetterUntilDone(ctx, group, message, attempt, envelope, err, nil) {
				gate.TaskDone()
			}
			return
		}
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
			return true
		}
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
