package kafka_test

import (
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/eventbus/kafka"
)

func ExampleOptions() {
	opts := kafka.Options{
		Brokers:         []string{"127.0.0.1:29092"},
		Topic:           "analytics.events",
		DeadLetterTopic: "analytics.events.dead",
		MaxAttempts:     5,
		RetryBackoff:    250 * time.Millisecond,
		Workers:         100,
	}

	fmt.Println(opts.Topic, opts.DeadLetterTopic, opts.MaxAttempts)

	// Output: analytics.events analytics.events.dead 5
}
