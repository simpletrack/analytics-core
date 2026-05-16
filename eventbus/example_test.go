package eventbus_test

import (
	"context"
	"fmt"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/eventbus"
)

func ExampleHandler() {
	handler := eventbus.Handler(func(_ context.Context, message eventbus.Message) error {
		fmt.Println(message.ID, message.Envelope.ID, message.Topic, message.Partition, message.Offset, message.Attempt)
		return nil
	})

	_ = handler(context.Background(), eventbus.Message{
		ID:        "analytics.events:2:42",
		Topic:     "analytics.events",
		Partition: 2,
		Offset:    42,
		Attempt:   1,
		Envelope:  contracts.EventEnvelope{ID: "evt_1"},
	})

	// Output: analytics.events:2:42 evt_1 analytics.events 2 42 1
}
