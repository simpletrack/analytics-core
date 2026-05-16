package kafka

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

func TestKafkaIntegrationPublishConsume(t *testing.T) {
	if os.Getenv("ANALYTICS_CORE_KAFKA_INTEGRATION") != "1" {
		t.Skip("set ANALYTICS_CORE_KAFKA_INTEGRATION=1 to run against local Kafka")
	}

	opts, err := Options{
		Brokers:         integrationBrokers(),
		Topic:           "analytics.events.integration." + time.Now().UTC().Format("20060102150405"),
		DeadLetterTopic: "analytics.events.integration.dead." + time.Now().UTC().Format("20060102150405"),
		ClientID:        "analytics-core-kafka-integration",
		MaxAttempts:     2,
		RetryBackoff:    25 * time.Millisecond,
		Workers:         4,
		QueueSize:       8,
		CommitInterval:  100 * time.Millisecond,
	}.normalize()
	if err != nil {
		t.Fatalf("normalize kafka options: %v", err)
	}
	createIntegrationTopic(t, opts)

	bus, err := New(opts)
	if err != nil {
		t.Fatalf("new kafka bus: %v", err)
	}
	t.Cleanup(func() {
		_ = bus.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	received := make(chan eventbus.Message, 1)
	done := make(chan error, 1)
	go func() {
		done <- bus.Subscribe(ctx, eventbus.ConsumerGroup{
			Name:     "analytics-core-kafka-integration-" + time.Now().UTC().Format("150405"),
			Consumer: "consumer-1",
		}, func(_ context.Context, message eventbus.Message) error {
			received <- message
			return nil
		})
	}()

	if err := bus.Publish(ctx, contracts.EventEnvelope{ID: "evt_integration", EventName: "pageview"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case message := <-received:
		if message.Envelope.ID != "evt_integration" || message.Topic != opts.Topic {
			t.Fatalf("unexpected message %#v", message)
		}
	case err := <-done:
		t.Fatalf("subscribe returned before message: %v", err)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for kafka message: %v", ctx.Err())
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe shutdown: %v", err)
	}
}

func integrationBrokers() []string {
	raw := strings.TrimSpace(os.Getenv("ANALYTICS_CORE_KAFKA_BROKERS"))
	if raw == "" {
		return []string{"127.0.0.1:29092"}
	}
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func createIntegrationTopic(t *testing.T, opts Options) {
	t.Helper()

	admin, err := sarama.NewClusterAdmin(opts.Brokers, newSaramaConfig(opts))
	if err != nil {
		t.Fatalf("new kafka admin: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.DeleteTopic(opts.Topic)
		_ = admin.Close()
	})

	err = admin.CreateTopic(opts.Topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		t.Fatalf("create kafka topic %q: %v", opts.Topic, err)
	}
}
