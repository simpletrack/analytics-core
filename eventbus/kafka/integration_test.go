package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strconv"
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

func TestKafkaReplicatedIntegrationPublishConsume(t *testing.T) {
	requireKafkaIntegration(t)
	requireKafkaReplicatedIntegration(t)
	opts := newIntegrationOptions(t, "replicated-publish-consume")
	createIntegrationTopicWithDetail(t, opts, replicatedIntegrationTopicDetail(t))

	bus, err := New(opts)
	if err != nil {
		t.Fatalf("new kafka bus: %v", err)
	}
	t.Cleanup(func() {
		_ = bus.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	received := make(chan eventbus.Message, 1)
	done := make(chan error, 1)
	go func() {
		done <- bus.Subscribe(ctx, eventbus.ConsumerGroup{
			Name:     "analytics-core-kafka-replicated-" + integrationRunID(),
			Consumer: "consumer-replicated-1",
		}, func(_ context.Context, message eventbus.Message) error {
			received <- message
			return nil
		})
	}()

	if err := bus.Publish(ctx, contracts.EventEnvelope{ID: "evt_replicated", EventName: "pageview"}); err != nil {
		t.Fatalf("publish to replicated topic: %v", err)
	}
	select {
	case message := <-received:
		if message.Envelope.ID != "evt_replicated" || message.Topic != opts.Topic {
			t.Fatalf("unexpected replicated topic message %#v", message)
		}
	case err := <-done:
		t.Fatalf("subscribe returned before replicated topic message: %v", err)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for replicated kafka message: %v", ctx.Err())
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("subscribe shutdown: %v", err)
	}
}

func TestKafkaIntegrationOptionsReadAuthenticatedBrokerEnv(t *testing.T) {
	t.Setenv("ANALYTICS_CORE_KAFKA_TLS_ENABLED", "true")
	t.Setenv("ANALYTICS_CORE_KAFKA_TLS_SERVER_NAME", "kafka.auth.local")
	t.Setenv("ANALYTICS_CORE_KAFKA_TLS_INSECURE_SKIP_VERIFY", "true")
	t.Setenv("ANALYTICS_CORE_KAFKA_SASL_ENABLED", "true")
	t.Setenv("ANALYTICS_CORE_KAFKA_SASL_MECHANISM", "plain")
	t.Setenv("ANALYTICS_CORE_KAFKA_SASL_USERNAME", "simpletrack")
	t.Setenv("ANALYTICS_CORE_KAFKA_SASL_PASSWORD", "secret")
	t.Setenv("ANALYTICS_CORE_KAFKA_SASL_HANDSHAKE", "false")

	opts := newIntegrationOptions(t, "auth-env")

	if !opts.TLSEnabled || opts.TLSConfig == nil || opts.TLSConfig.ServerName != "kafka.auth.local" || !opts.TLSConfig.InsecureSkipVerify {
		t.Fatalf("unexpected TLS integration options: %#v", opts.TLSConfig)
	}
	if !opts.SASLEnabled || opts.SASLMechanism != SASLMechanismPlain || opts.SASLUsername != "simpletrack" || opts.SASLPassword != "secret" {
		t.Fatalf("unexpected SASL integration option mapping")
	}
	if opts.SASLHandshake == nil || *opts.SASLHandshake {
		t.Fatalf("expected integration SASL handshake override to be false, got %#v", opts.SASLHandshake)
	}
}

func TestKafkaIntegrationTopicDetailReadsProductionShape(t *testing.T) {
	t.Setenv("ANALYTICS_CORE_KAFKA_TOPIC_PARTITIONS", "12")
	t.Setenv("ANALYTICS_CORE_KAFKA_TOPIC_REPLICATION_FACTOR", "3")
	t.Setenv("ANALYTICS_CORE_KAFKA_TOPIC_MIN_INSYNC_REPLICAS", "2")

	detail := integrationTopicDetail(t)

	if detail.NumPartitions != 12 || detail.ReplicationFactor != 3 {
		t.Fatalf("unexpected integration topic detail: %#v", detail)
	}
	if detail.ConfigEntries == nil || detail.ConfigEntries["min.insync.replicas"] == nil || *detail.ConfigEntries["min.insync.replicas"] != "2" {
		t.Fatalf("expected min.insync.replicas config entry, got %#v", detail.ConfigEntries)
	}
}

func requireKafkaIntegration(t *testing.T) {
	t.Helper()

	if os.Getenv("ANALYTICS_CORE_KAFKA_INTEGRATION") != "1" {
		t.Skip("set ANALYTICS_CORE_KAFKA_INTEGRATION=1 to run against local Kafka")
	}
}

func requireKafkaReplicatedIntegration(t *testing.T) {
	t.Helper()

	if os.Getenv("ANALYTICS_CORE_KAFKA_REPLICATED_INTEGRATION") != "1" {
		t.Skip("set ANALYTICS_CORE_KAFKA_REPLICATED_INTEGRATION=1 to run replicated-topic Kafka integration tests")
	}
}

// integrationTestLogger is the shared helper surface implemented by testing.T and testing.B.
type integrationTestLogger interface {
	Helper()
	Fatalf(string, ...any)
	Cleanup(func())
}

// newIntegrationOptions builds provider options for gated real-broker tests and benchmarks.
func newIntegrationOptions(t integrationTestLogger, suffix string) Options {
	t.Helper()

	tlsEnabled := integrationBoolEnv(t, "ANALYTICS_CORE_KAFKA_TLS_ENABLED", false)
	tlsConfig := integrationTLSConfig(t, tlsEnabled)
	saslHandshake := integrationBoolPtrEnv(t, "ANALYTICS_CORE_KAFKA_SASL_HANDSHAKE")
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
		TLSEnabled:      tlsEnabled,
		TLSConfig:       tlsConfig,
		SASLEnabled:     integrationBoolEnv(t, "ANALYTICS_CORE_KAFKA_SASL_ENABLED", false),
		SASLMechanism:   SASLMechanism(strings.TrimSpace(os.Getenv("ANALYTICS_CORE_KAFKA_SASL_MECHANISM"))),
		SASLUsername:    os.Getenv("ANALYTICS_CORE_KAFKA_SASL_USERNAME"),
		SASLPassword:    os.Getenv("ANALYTICS_CORE_KAFKA_SASL_PASSWORD"),
		SASLHandshake:   saslHandshake,
	}.normalize()
	if err != nil {
		t.Fatalf("normalize kafka options: %v", err)
	}
	return opts
}

// integrationTLSConfig builds optional TLS material for authenticated Kafka drills.
func integrationTLSConfig(t integrationTestLogger, enabled bool) *tls.Config {
	t.Helper()

	if !enabled {
		return nil
	}
	config := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         strings.TrimSpace(os.Getenv("ANALYTICS_CORE_KAFKA_TLS_SERVER_NAME")),
		InsecureSkipVerify: integrationBoolEnv(t, "ANALYTICS_CORE_KAFKA_TLS_INSECURE_SKIP_VERIFY", false), //nolint:gosec // integration-test escape hatch for local throwaway brokers.
	}
	if caFile := strings.TrimSpace(os.Getenv("ANALYTICS_CORE_KAFKA_TLS_CA_FILE")); caFile != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		pem, err := os.ReadFile(caFile)
		if err != nil {
			t.Fatalf("read Kafka integration CA file: %v", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			t.Fatalf("Kafka integration CA file %q does not contain PEM certificates", caFile)
		}
		config.RootCAs = pool
	}
	certFile := strings.TrimSpace(os.Getenv("ANALYTICS_CORE_KAFKA_TLS_CERT_FILE"))
	keyFile := strings.TrimSpace(os.Getenv("ANALYTICS_CORE_KAFKA_TLS_KEY_FILE"))
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			t.Fatalf("Kafka integration client cert and key must be configured together")
		}
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatalf("load Kafka integration client certificate: %v", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config
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

// createIntegrationTopic creates a disposable topic for one integration run.
func createIntegrationTopic(t integrationTestLogger, opts Options) {
	t.Helper()

	createIntegrationTopicWithDetail(t, opts, integrationTopicDetail(t))
}

// createIntegrationTopicWithDetail creates a disposable topic with explicit broker settings.
func createIntegrationTopicWithDetail(t integrationTestLogger, opts Options, detail *sarama.TopicDetail) {
	t.Helper()

	admin, err := sarama.NewClusterAdmin(opts.Brokers, newSaramaConfig(opts))
	if err != nil {
		t.Fatalf("new kafka admin: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.DeleteTopic(opts.Topic)
		_ = admin.Close()
	})

	err = admin.CreateTopic(opts.Topic, detail, false)
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		t.Fatalf("create kafka topic %q: %v", opts.Topic, err)
	}
}

// integrationTopicDetail reads topic shape overrides for single-broker drills.
func integrationTopicDetail(t integrationTestLogger) *sarama.TopicDetail {
	t.Helper()

	return &sarama.TopicDetail{
		NumPartitions:     int32(integrationIntEnv(t, "ANALYTICS_CORE_KAFKA_TOPIC_PARTITIONS", 1)),
		ReplicationFactor: int16(integrationIntEnv(t, "ANALYTICS_CORE_KAFKA_TOPIC_REPLICATION_FACTOR", 1)),
		ConfigEntries:     integrationTopicConfigEntries(),
	}
}

// replicatedIntegrationTopicDetail enforces the production-shaped replicated topic minimums.
func replicatedIntegrationTopicDetail(t integrationTestLogger) *sarama.TopicDetail {
	t.Helper()

	detail := integrationTopicDetail(t)
	if detail.NumPartitions < 3 {
		detail.NumPartitions = 3
	}
	if detail.ReplicationFactor < 3 {
		detail.ReplicationFactor = 3
	}
	if detail.ConfigEntries == nil {
		detail.ConfigEntries = map[string]*string{}
	}
	if _, ok := detail.ConfigEntries["min.insync.replicas"]; !ok {
		minISR := "2"
		detail.ConfigEntries["min.insync.replicas"] = &minISR
	}
	return detail
}

// integrationTopicConfigEntries reads optional topic-level configs for integration topics.
func integrationTopicConfigEntries() map[string]*string {
	minISR := strings.TrimSpace(os.Getenv("ANALYTICS_CORE_KAFKA_TOPIC_MIN_INSYNC_REPLICAS"))
	if minISR == "" {
		return nil
	}
	return map[string]*string{"min.insync.replicas": &minISR}
}

// integrationIntEnv reads a positive integer environment override for gated tests.
func integrationIntEnv(t integrationTestLogger, key string, fallback int) int {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		t.Fatalf("%s must be a positive integer", key)
	}
	return value
}

// integrationBoolEnv reads a boolean environment override for gated tests.
func integrationBoolEnv(t integrationTestLogger, key string, fallback bool) bool {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		t.Fatalf("%s must be a boolean", key)
	}
	return value
}

// integrationBoolPtrEnv reads an optional boolean environment override for provider pointers.
func integrationBoolPtrEnv(t integrationTestLogger, key string) *bool {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	value := integrationBoolEnv(t, key, false)
	return &value
}
