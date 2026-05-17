package kafka

import (
	"errors"
	"strings"
	"time"
)

const (
	// Default option values keep local Kafka wiring explicit but operator-light.
	defaultClientID             = "analytics-core-eventbus"
	defaultTopic                = "analytics.events"
	defaultDeadLetterTopic      = "analytics.events.dead"
	defaultMaxAttempts          = 5
	defaultRetryBackoff         = 250 * time.Millisecond
	defaultWorkers              = 100
	defaultQueueSize            = 200
	defaultCommitInterval       = time.Second
	defaultProducerAcks         = ProducerAcksAll
	defaultProducerRetryMax     = 5
	defaultProducerRetryBackoff = 100 * time.Millisecond
)

// ProducerAcks names the producer acknowledgement level without exposing Sarama types.
type ProducerAcks string

const (
	// ProducerAcksAll waits for all in-sync replicas before Publish reports success.
	ProducerAcksAll ProducerAcks = "all"
	// ProducerAcksLeader waits only for the partition leader acknowledgement.
	ProducerAcksLeader ProducerAcks = "leader"
	// ProducerAcksNone sends without waiting for broker acknowledgement.
	ProducerAcksNone ProducerAcks = "none"
)

// Options configures the Kafka EventBus provider.
type Options struct {
	Brokers         []string      // Brokers are Kafka bootstrap broker addresses
	Topic           string        // Topic receives accepted analytics event envelopes
	DeadLetterTopic string        // DeadLetterTopic receives exhausted or malformed messages
	ClientID        string        // ClientID identifies this provider instance to Kafka
	MaxAttempts     int           // MaxAttempts is the handler attempt limit before DLQ
	RetryBackoff    time.Duration // RetryBackoff spaces handler and DLQ retries
	Workers         int           // Workers is the fixed handler worker count
	QueueSize       int           // QueueSize is the bounded handler work queue size
	CommitInterval  time.Duration // CommitInterval is Sarama's auto-commit interval

	ProducerAcks             ProducerAcks   // ProducerAcks controls broker acknowledgement durability for Publish
	ProducerRetryMax         *int           // ProducerRetryMax overrides send retries; nil uses the durable default
	ProducerRetryBackoff     *time.Duration // ProducerRetryBackoff overrides retry spacing; nil uses the durable default
	ProducerFlushBytes       int            // ProducerFlushBytes is the best-effort flush byte threshold
	ProducerFlushMessages    int            // ProducerFlushMessages is the best-effort flush message threshold
	ProducerFlushFrequency   time.Duration  // ProducerFlushFrequency is the best-effort time-based flush threshold
	ProducerFlushMaxMessages int            // ProducerFlushMaxMessages caps messages per broker request when positive
	IdempotentProducer       bool           // IdempotentProducer enables broker idempotence and its stricter ordering limits
}

// normalize validates operator input and fills provider defaults.
func (o Options) normalize() (Options, error) {
	// Normalize string inputs before constructing any Sarama clients so invalid
	// broker lists fail before the runtime opens network resources.
	brokers := make([]string, 0, len(o.Brokers))
	for _, broker := range o.Brokers {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	o.Brokers = brokers
	if len(o.Brokers) == 0 {
		return Options{}, errors.New("kafka brokers are required")
	}
	if strings.TrimSpace(o.Topic) == "" {
		o.Topic = defaultTopic
	}
	if strings.TrimSpace(o.DeadLetterTopic) == "" {
		o.DeadLetterTopic = defaultDeadLetterTopic
	}
	if strings.TrimSpace(o.ClientID) == "" {
		o.ClientID = defaultClientID
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = defaultMaxAttempts
	}
	if o.RetryBackoff < 0 {
		o.RetryBackoff = 0
	}
	if o.RetryBackoff == 0 {
		o.RetryBackoff = defaultRetryBackoff
	}
	if o.Workers <= 0 {
		o.Workers = defaultWorkers
	}
	if o.QueueSize <= 0 {
		o.QueueSize = defaultQueueSize
	}
	if o.QueueSize < o.Workers {
		o.QueueSize = o.Workers
	}
	if o.CommitInterval <= 0 {
		o.CommitInterval = defaultCommitInterval
	}
	if o.ProducerAcks == "" {
		o.ProducerAcks = defaultProducerAcks
	}
	switch o.ProducerAcks {
	case ProducerAcksAll, ProducerAcksLeader, ProducerAcksNone:
	default:
		return Options{}, errors.New("kafka producer acks must be one of all, leader, or none")
	}
	if o.ProducerRetryMax == nil {
		producerRetryMax := defaultProducerRetryMax
		o.ProducerRetryMax = &producerRetryMax
	}
	if *o.ProducerRetryMax < 0 {
		return Options{}, errors.New("kafka producer retry max must be >= 0")
	}
	if o.ProducerRetryBackoff == nil {
		producerRetryBackoff := defaultProducerRetryBackoff
		o.ProducerRetryBackoff = &producerRetryBackoff
	}
	if *o.ProducerRetryBackoff < 0 {
		return Options{}, errors.New("kafka producer retry backoff must be >= 0")
	}
	if o.ProducerFlushBytes < 0 {
		return Options{}, errors.New("kafka producer flush bytes must be >= 0")
	}
	if o.ProducerFlushMessages < 0 {
		return Options{}, errors.New("kafka producer flush messages must be >= 0")
	}
	if o.ProducerFlushFrequency < 0 {
		return Options{}, errors.New("kafka producer flush frequency must be >= 0")
	}
	if o.ProducerFlushMaxMessages < 0 {
		return Options{}, errors.New("kafka producer flush max messages must be >= 0")
	}
	if o.ProducerFlushMaxMessages > 0 && o.ProducerFlushMessages > o.ProducerFlushMaxMessages {
		return Options{}, errors.New("kafka producer flush max messages must be >= producer flush messages")
	}
	if o.IdempotentProducer && o.ProducerAcks != ProducerAcksAll {
		return Options{}, errors.New("kafka idempotent producer requires producer acks all")
	}
	if o.IdempotentProducer && *o.ProducerRetryMax == 0 {
		return Options{}, errors.New("kafka idempotent producer requires producer retry max >= 1")
	}
	return o, nil
}
