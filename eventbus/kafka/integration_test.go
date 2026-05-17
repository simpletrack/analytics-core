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
	requireKafkaIntegration(t)
	opts := newIntegrationOptions(t, "publish-consume")
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

func TestKafkaIntegrationOrderedCommitWaitsForEarlierOffset(t *testing.T) {
	requireKafkaIntegration(t)
	opts := newIntegrationOptions(t, "ordered-commit")
	createIntegrationTopic(t, opts)

	bus, err := New(opts)
	if err != nil {
		t.Fatalf("new kafka bus: %v", err)
	}
	t.Cleanup(func() {
		_ = bus.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	group := eventbus.ConsumerGroup{
		Name:     "analytics-core-kafka-ordered-" + integrationRunID(),
		Consumer: "consumer-ordered-1",
	}
	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	secondDone := make(chan eventbus.Message, 1)
	done := make(chan error, 1)
	go func() {
		done <- bus.Subscribe(ctx, group, func(ctx context.Context, message eventbus.Message) error {
			// Hold the first offset open so the second offset can complete first.
			if message.Envelope.ID == "evt_ordered_0" {
				firstStarted <- struct{}{}
				select {
				case <-releaseFirst:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if message.Envelope.ID == "evt_ordered_1" {
				secondDone <- message
			}
			return nil
		})
	}()

	// Publish both records to a single-partition topic so their offsets are ordered.
	publishIntegrationEnvelope(ctx, t, bus, "evt_ordered_0")
	publishIntegrationEnvelope(ctx, t, bus, "evt_ordered_1")
	select {
	case <-firstStarted:
	case err := <-done:
		t.Fatalf("subscribe returned before first offset started: %v", err)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for first offset: %v", ctx.Err())
	}
	select {
	case message := <-secondDone:
		if message.Offset <= 0 {
			t.Fatalf("second message offset = %d, want later offset", message.Offset)
		}
	case err := <-done:
		t.Fatalf("subscribe returned before second offset completed: %v", err)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for second offset: %v", ctx.Err())
	}

	// Wait longer than auto-commit so a broken out-of-order mark would be visible.
	time.Sleep(4 * opts.CommitInterval)
	if offset := readCommittedOffset(t, opts, group.Name, 0); offset >= 1 {
		t.Fatalf("committed offset = %d before earlier offset completed, want < 1", offset)
	}

	// Releasing the first offset should let the contiguous range 0..1 commit as 2.
	close(releaseFirst)
	waitForCommittedOffset(t, opts, group.Name, 0, 2, 10*time.Second)
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe shutdown: %v", err)
	}
}

func TestKafkaIntegrationUncommittedMessageReplaysAfterRestart(t *testing.T) {
	requireKafkaIntegration(t)
	opts := newIntegrationOptions(t, "restart-replay")
	createIntegrationTopic(t, opts)
	groupName := "analytics-core-kafka-replay-" + integrationRunID()

	bus, err := New(opts)
	if err != nil {
		t.Fatalf("new first kafka bus: %v", err)
	}
	ctx := context.Background()
	publishIntegrationEnvelope(ctx, t, bus, "evt_replay")

	firstCtx, cancelFirst := context.WithTimeout(ctx, 15*time.Second)
	firstSeen := make(chan eventbus.Message, 1)
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- bus.Subscribe(firstCtx, eventbus.ConsumerGroup{Name: groupName, Consumer: "consumer-replay-1"}, func(ctx context.Context, message eventbus.Message) error {
			// Return only after cancellation so this delivery never reaches ordered completion.
			firstSeen <- message
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case message := <-firstSeen:
		if message.Envelope.ID != "evt_replay" {
			t.Fatalf("first delivery id = %q, want evt_replay", message.Envelope.ID)
		}
	case err := <-firstDone:
		t.Fatalf("first subscribe returned before delivery: %v", err)
	case <-firstCtx.Done():
		t.Fatalf("timed out waiting for first delivery: %v", firstCtx.Err())
	}
	cancelFirst()
	if err := <-firstDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first subscribe shutdown: %v", err)
	}
	if err := bus.Close(); err != nil {
		t.Fatalf("close first bus: %v", err)
	}
	if offset := readCommittedOffset(t, opts, groupName, 0); offset >= 1 {
		t.Fatalf("committed offset = %d after canceled handler, want < 1", offset)
	}

	// A fresh consumer in the same group must replay the uncommitted message.
	replayBus, err := New(opts)
	if err != nil {
		t.Fatalf("new replay kafka bus: %v", err)
	}
	t.Cleanup(func() {
		_ = replayBus.Close()
	})
	replayCtx, cancelReplay := context.WithTimeout(ctx, 15*time.Second)
	defer cancelReplay()
	replayed := make(chan eventbus.Message, 1)
	replayDone := make(chan error, 1)
	go func() {
		replayDone <- replayBus.Subscribe(replayCtx, eventbus.ConsumerGroup{Name: groupName, Consumer: "consumer-replay-2"}, func(_ context.Context, message eventbus.Message) error {
			replayed <- message
			return nil
		})
	}()
	select {
	case message := <-replayed:
		if message.Envelope.ID != "evt_replay" || message.Offset != 0 {
			t.Fatalf("replayed message = %#v, want original offset 0", message)
		}
	case err := <-replayDone:
		t.Fatalf("replay subscribe returned before delivery: %v", err)
	case <-replayCtx.Done():
		t.Fatalf("timed out waiting for replay: %v", replayCtx.Err())
	}
	waitForCommittedOffset(t, opts, groupName, 0, 1, 10*time.Second)
	cancelReplay()
	if err := <-replayDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("replay subscribe shutdown: %v", err)
	}
}

func requireKafkaIntegration(t *testing.T) {
	t.Helper()

	if os.Getenv("ANALYTICS_CORE_KAFKA_INTEGRATION") != "1" {
		t.Skip("set ANALYTICS_CORE_KAFKA_INTEGRATION=1 to run against local Kafka")
	}
}

func newIntegrationOptions(t *testing.T, suffix string) Options {
	t.Helper()

	runID := integrationRunID()
	opts, err := Options{
		Brokers:         integrationBrokers(),
		Topic:           "analytics.events.integration." + suffix + "." + runID,
		DeadLetterTopic: "analytics.events.integration.dead." + suffix + "." + runID,
		ClientID:        "analytics-core-kafka-integration",
		MaxAttempts:     2,
		RetryBackoff:    25 * time.Millisecond,
		Workers:         4,
		QueueSize:       8,
		CommitInterval:  50 * time.Millisecond,
	}.normalize()
	if err != nil {
		t.Fatalf("normalize kafka options: %v", err)
	}
	return opts
}

func integrationRunID() string {
	return time.Now().UTC().Format("20060102150405.000000000")
}

func publishIntegrationEnvelope(ctx context.Context, t *testing.T, bus *Bus, id string) {
	t.Helper()

	if err := bus.Publish(ctx, contracts.EventEnvelope{ID: id, EventName: "pageview"}); err != nil {
		t.Fatalf("publish %s: %v", id, err)
	}
}

func readCommittedOffset(t *testing.T, opts Options, group string, partition int32) int64 {
	t.Helper()

	admin, err := sarama.NewClusterAdmin(opts.Brokers, newSaramaConfig(opts))
	if err != nil {
		t.Fatalf("new kafka admin: %v", err)
	}
	defer func() {
		_ = admin.Close()
	}()
	response, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{opts.Topic: []int32{partition}})
	if err != nil {
		t.Fatalf("list consumer group offsets: %v", err)
	}
	block := response.GetBlock(opts.Topic, partition)
	if block == nil {
		return -1
	}
	if block.Err != sarama.ErrNoError {
		t.Fatalf("offset fetch error for %s/%d: %v", opts.Topic, partition, block.Err)
	}
	return block.Offset
}

func waitForCommittedOffset(t *testing.T, opts Options, group string, partition int32, want int64, timeout time.Duration) int64 {
	t.Helper()

	deadline := time.Now().Add(timeout)
	last := int64(-1)
	for time.Now().Before(deadline) {
		last = readCommittedOffset(t, opts, group, partition)
		if last >= want {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("committed offset for group %s partition %d = %d, want >= %d", group, partition, last, want)
	return last
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
