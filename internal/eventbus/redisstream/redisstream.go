package redisstream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/simpletrack/analytics-core/internal/eventbus"
	"github.com/simpletrack/analytics-core/pkg/contracts"
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
	Stream           string        // Redis stream name for accepted events
	Block            time.Duration // blocking read timeout for new messages
	Count            int64         // maximum messages read per poll
	EnsureConsumer   bool          // creates the consumer group and stream when missing
	MaxAttempts      int           // attempts before dead-lettering; zero means unlimited
	DeadLetterStream string        // optional stream for exhausted messages
}

// Bus implements EventBus on Redis Streams.
type Bus struct {
	client *redis.Client
	opts   Options
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
			msg, err := b.decode(ctx, message, group)
			if err != nil {
				return processed, err
			}
			processed++
			if err := handler(ctx, msg); err != nil {
				if nackErr := msg.Nack(ctx, err); nackErr != nil {
					return processed, nackErr
				}
				continue
			}
			if err := msg.Ack(ctx); err != nil {
				return processed, err
			}
		}
	}
	return processed, nil
}

func (b *Bus) decode(ctx context.Context, message redis.XMessage, group eventbus.ConsumerGroup) (eventbus.Message, error) {
	raw, ok := message.Values[envelopeField]
	if !ok {
		return eventbus.Message{}, errors.New("redis stream message missing envelope")
	}

	var payload []byte
	switch value := raw.(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
	default:
		return eventbus.Message{}, errors.New("redis stream envelope has unsupported type")
	}

	var envelope contracts.EventEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return eventbus.Message{}, err
	}

	attempt, err := b.deliveryAttempt(ctx, message.ID, group.Consumer, group.Name)
	if err != nil {
		return eventbus.Message{}, err
	}

	return eventbus.Message{
		ID:       message.ID,
		Attempt:  attempt,
		Envelope: envelope,
		Ack: func(ctx context.Context) error {
			return b.client.XAck(ctx, b.opts.Stream, group.Name, message.ID).Err()
		},
		Nack: func(ctx context.Context, cause error) error {
			if b.opts.MaxAttempts <= 0 || attempt < b.opts.MaxAttempts || b.opts.DeadLetterStream == "" {
				return nil
			}
			if err := b.deadLetter(ctx, group, message.ID, attempt, payload, cause); err != nil {
				return err
			}
			if err := b.client.XAck(ctx, b.opts.Stream, group.Name, message.ID).Err(); err != nil {
				return err
			}
			return nil
		},
	}, nil
}

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

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
