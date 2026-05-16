package eventbus_test

import (
	"context"
	"fmt"

	"github.com/simpletrack/analytics-core/eventbus"
)

func ExampleHandler() {
	handler := eventbus.Handler(func(_ context.Context, message eventbus.Message) error {
		fmt.Println(message.Topic, message.Partition, message.Offset, message.Attempt)
		return nil
	})

	_ = handler(context.Background(), eventbus.Message{
		Topic:     "analytics.events",
		Partition: 2,
		Offset:    42,
		Attempt:   1,
	})

	// Output: analytics.events 2 42 1
}
