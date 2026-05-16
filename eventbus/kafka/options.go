package kafka

import (
	"errors"
	"strings"
	"time"
)

const (
	// Default option values keep local Kafka wiring explicit but operator-light.
	defaultClientID        = "analytics-core-eventbus"
	defaultTopic           = "analytics.events"
	defaultDeadLetterTopic = "analytics.events.dead"
	defaultMaxAttempts     = 5
	defaultRetryBackoff    = 250 * time.Millisecond
	defaultWorkers         = 100
	defaultQueueSize       = 200
	defaultCommitInterval  = time.Second
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
	return o, nil
}
