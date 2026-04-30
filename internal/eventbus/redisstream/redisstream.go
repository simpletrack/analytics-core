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

const envelopeField = "envelope"

type Options struct {
	Stream         string
	Block          time.Duration
	Count          int64
	EnsureConsumer bool
}

// Bus implements EventBus on Redis Streams.
type Bus struct {
	client *redis.Client
	opts   Options
}

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

		streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group.Name,
			Consumer: group.Consumer,
			Streams:  []string{b.opts.Stream, ">"},
			Count:    b.opts.Count,
			Block:    b.opts.Block,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return err
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				msg, err := b.decode(message, group)
				if err != nil {
					return err
				}
				if err := handler(ctx, msg); err != nil {
					_ = msg.Nack(ctx, err)
					continue
				}
				if err := msg.Ack(ctx); err != nil {
					return err
				}
			}
		}
	}
}

func (b *Bus) decode(message redis.XMessage, group eventbus.ConsumerGroup) (eventbus.Message, error) {
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

	return eventbus.Message{
		ID:       message.ID,
		Attempt:  1,
		Envelope: envelope,
		Ack: func(ctx context.Context) error {
			return b.client.XAck(ctx, b.opts.Stream, group.Name, message.ID).Err()
		},
		Nack: func(context.Context, error) error {
			return nil
		},
	}, nil
}

func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
