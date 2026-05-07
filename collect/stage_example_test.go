package collect_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/simpletrack/analytics-core/collect"
	"github.com/simpletrack/analytics-core/eventbus/direct"
)

func ExampleNewSessionResolverStage() {
	bus := direct.New()
	stage, _ := collect.NewSessionResolverStage(collect.SessionResolverConfig{
		Salt:   "deployment-secret",
		Window: 30 * time.Minute,
	})
	handler, _ := collect.NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	}, collect.WithStages(stage))

	envelope, _ := handler.Handle(context.Background(), exampleRequest())

	fmt.Println(strings.HasPrefix(envelope.SessionID, "ses_"))

	// Output:
	// true
}

func ExampleNewVisitResolverStage() {
	bus := direct.New()
	sessionStage, _ := collect.NewSessionResolverStage(collect.SessionResolverConfig{
		Salt:   "deployment-session-secret",
		Window: 30 * time.Minute,
	})
	visitStage, _ := collect.NewVisitResolverStage(collect.VisitResolverConfig{
		Salt:   "deployment-visit-secret",
		Window: 30 * time.Minute,
	})
	handler, _ := collect.NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	}, collect.WithStages(sessionStage, visitStage))

	envelope, _ := handler.Handle(context.Background(), exampleRequest())

	fmt.Println(strings.HasPrefix(envelope.VisitID, "vis_"))

	// Output:
	// true
}

func ExampleNewClientEnrichmentStage() {
	bus := direct.New()
	stage, _ := collect.NewClientEnrichmentStage(collect.ClientEnrichmentConfig{
		HashSalt:         "deployment-secret",
		IncludeUserAgent: true,
		IncludeIPHash:    true,
	})
	handler, _ := collect.NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	}, collect.WithStages(stage))
	request := exampleRequest()
	request.Client = collect.ClientInfo{
		UserAgent: "Mozilla/5.0",
		IP:        "203.0.113.10",
	}

	envelope, _ := handler.Handle(context.Background(), request)

	fmt.Println(envelope.Properties["client.user_agent"])
	fmt.Println(strings.HasPrefix(envelope.Properties["client.ip_hash"].(string), "ip_"))

	// Output:
	// Mozilla/5.0
	// true
}

func ExampleNewTrafficFilterStage() {
	bus := direct.New()
	stage, _ := collect.NewTrafficFilterStage(collect.TrafficFilterConfig{})
	handler, _ := collect.NewHandlerWithOptions(bus, func() time.Time {
		return time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	}, collect.WithStages(stage))
	request := exampleRequest()
	request.Client = collect.ClientInfo{UserAgent: "Googlebot/2.1"}

	_, err := handler.Handle(context.Background(), request)

	var filtered collect.FilteredError
	fmt.Println(errors.As(err, &filtered))

	// Output:
	// true
}

func exampleRequest() collect.Request {
	return collect.Request{
		ID:         "evt_1",
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		SourceType: "web",
		EventName:  "pageview",
		DistinctID: "visitor_1",
	}
}
