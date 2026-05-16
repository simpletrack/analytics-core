package redisstream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

const (
	envelopeField           = "envelope"
	deadLetterAttemptField  = "attempt"
	deadLetterConsumerField = "consumer"
	deadLetterErrorField    = "error"
	deadLetterFailedAtField = "failed_at"
	deadLetterGroupField    = "consumer_group"
	deadLetterMessageField  = "original_message_id"
)

// Options configures a Redis Stream EventBus.
type Options struct {
	Stream           string        // Stream is the Redis stream name for accepted events
	Block            time.Duration // Block is the blocking read timeout for new messages
	Count            int64         // Count is the maximum messages read per poll
	EnsureConsumer   bool          // EnsureConsumer creates the consumer group and stream when missing
	MaxAttempts      int           // MaxAttempts is the attempts before dead-lettering; zero means unlimited
	DeadLetterStream string        // DeadLetterStream is the optional stream for exhausted messages
}

// Bus implements EventBus on Redis Streams.
type Bus struct {
	client *redis.Client // client owns Redis Stream network operations
	opts   Options       // opts stores stream, group, retry, and dead-letter settings
}

// New creates a Redis Stream EventBus.
func New(client *redis.Client, opts Options) (*Bus, error) {
	if client == nil {
		return nil, errors.New("redis client is required")
	}
	if opts.Stream == "" {
		return nil, errors.New("redis stream is required")
	}
	if opts.Block <= 0 {
		opts.Block = time.Second
	}
	if opts.Count <= 0 {
		opts.Count = 10
	}
	return &Bus{client: client, opts: opts}, nil
}

// Publish appends envelope to the configured Redis stream.
func (b *Bus) Publish(ctx context.Context, envelope contracts.EventEnvelope) error {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	_, err = b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: b.opts.Stream,
		Values: map[string]any{
			envelopeField: payload,
		},
	}).Result()
	return err
}

// Subscribe consumes new and pending messages for group until ctx is cancelled.
func (b *Bus) Subscribe(ctx context.Context, group eventbus.ConsumerGroup, handler eventbus.Handler) error {
	if group.Name == "" {
		return errors.New("consumer group name is required")
	}
	if group.Consumer == "" {
		return errors.New("consumer name is required")
	}
	if b.opts.EnsureConsumer {
		if err := b.client.XGroupCreateMkStream(ctx, b.opts.Stream, group.Name, "0").Err(); err != nil && !isBusyGroup(err) {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		processed, err := b.consume(ctx, group, "0", 0, handler)
		if err != nil {
			return err
		}
		if processed > 0 {
			continue
		}

		if _, err := b.consume(ctx, group, ">", b.opts.Block, handler); err != nil {
			return err
		}
	}
}

func (b *Bus) consume(ctx context.Context, group eventbus.ConsumerGroup, id string, block time.Duration, handler eventbus.Handler) (int, error) {
	// Read either pending work ("0") or fresh work (">") using the same
	// handler contract so retry and DLQ behavior stays provider-owned.
	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group.Name,
		Consumer: group.Consumer,
		Streams:  []string{b.opts.Stream, id},
		Count:    b.opts.Count,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, err
	}

	processed := 0
	for _, stream := range streams {
		for _, message := range stream.Messages {
			// Decode once and pass stable queue metadata to the handler; Redis
			// acknowledgement remains below this provider boundary.
			msg, payload, err := b.decode(ctx, message, group)
			if err != nil {
				return processed, err
			}
			processed++
			if err := handler(ctx, msg); err != nil {
				if nackErr := b.handleFailure(ctx, group, message.ID, msg.Attempt, payload, err); nackErr != nil {
					return processed, nackErr
				}
				continue
			}
			if err := b.client.XAck(ctx, b.opts.Stream, group.Name, message.ID).Err(); err != nil {
				return processed, err
			}
		}
	}
	return processed, nil
}

// decode converts a Redis Stream entry into the public EventBus message shape.
func (b *Bus) decode(ctx context.Context, message redis.XMessage, group eventbus.ConsumerGroup) (eventbus.Message, []byte, error) {
	raw, ok := message.Values[envelopeField]
	if !ok {
		return eventbus.Message{}, nil, errors.New("redis stream message missing envelope")
	}

	var payload []byte
	switch value := raw.(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
	default:
		return eventbus.Message{}, nil, errors.New("redis stream envelope has unsupported type")
	}

	var envelope contracts.EventEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return eventbus.Message{}, nil, err
	}

	attempt, err := b.deliveryAttempt(ctx, message.ID, group.Consumer, group.Name)
	if err != nil {
		return eventbus.Message{}, nil, err
	}

	return eventbus.Message{
		ID:       message.ID,
		Topic:    b.opts.Stream,
		Key:      envelope.ID,
		Attempt:  attempt,
		Envelope: envelope,
	}, payload, nil
}

// handleFailure either leaves a message pending for retry or moves it to DLQ.
func (b *Bus) handleFailure(ctx context.Context, group eventbus.ConsumerGroup, messageID string, attempt int, payload []byte, cause error) error {
	// Retryable Redis Stream failures stay pending. Subscribe always drains
	// pending entries before reading new messages, so failed storage writes are
	// retried before fresh work can outrun them.
	if b.opts.MaxAttempts <= 0 || attempt < b.opts.MaxAttempts || b.opts.DeadLetterStream == "" {
		return nil
	}

	// Dead-letter first, then acknowledge the original. If the diagnostic write
	// fails, the original message remains pending and will be retried.
	if err := b.deadLetter(ctx, group, messageID, attempt, payload, cause); err != nil {
		return err
	}
	return b.client.XAck(ctx, b.opts.Stream, group.Name, messageID).Err()
}

// deadLetter writes one Redis Stream diagnostic message for an exhausted entry.
func (b *Bus) deadLetter(ctx context.Context, group eventbus.ConsumerGroup, messageID string, attempt int, payload []byte, cause error) error {
	_, err := b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: b.opts.DeadLetterStream,
		Values: map[string]any{
			envelopeField:           payload,
			deadLetterAttemptField:  attempt,
			deadLetterConsumerField: group.Consumer,
			deadLetterErrorField:    cause.Error(),
			deadLetterFailedAtField: time.Now().UTC().Format(time.RFC3339Nano),
			deadLetterGroupField:    group.Name,
			deadLetterMessageField:  messageID,
		},
	}).Result()
	return err
}

// deliveryAttempt reads Redis pending metadata and returns a one-based attempt count.
func (b *Bus) deliveryAttempt(ctx context.Context, messageID string, consumer string, group string) (int, error) {
	pending, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   b.opts.Stream,
		Group:    group,
		Start:    messageID,
		End:      messageID,
		Count:    1,
		Consumer: consumer,
	}).Result()
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 1, nil
	}
	if pending[0].RetryCount <= 0 {
		return 1, nil
	}
	return int(pending[0].RetryCount), nil
}

// isBusyGroup reports whether Redis rejected group creation because it exists.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
