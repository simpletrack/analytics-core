package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/storage"
)

// BenchmarkEventQueryBuilderReadSideShapes measures the plan-building cost of
// representative low, medium, and high read-side query shapes.
//
// The benchmark is intentionally builder-only. It gives us a stable baseline
// for query evidence shapes without mixing executor or ClickHouse server noise
// into the optimization discussion.
func BenchmarkEventQueryBuilderReadSideShapes(b *testing.B) {
	// Phase 1: build a single shared query builder so the benchmark measures
	// read-side plan construction instead of setup overhead.
	router, err := NewTableRouter("events")
	if err != nil {
		b.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router, WithAllowedPropertyFilters(
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"},
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "plan"},
		storage.PropertySelector{Scope: storage.PropertyScopeUser, Name: "tier"},
	))
	if err != nil {
		b.Fatalf("new event query builder failed: %v", err)
	}

	lowRealtimeQuery := storage.RealtimeQuery{
		TenantID:  "tenant_bench",
		ProjectID: "project_bench",
		SourceID:  "source_bench",
		Since:     time.Date(2026, 5, 1, 8, 30, 0, 0, time.UTC),
		Limit:     50,
	}
	mediumEventsQuery := storage.EventListQuery{
		TenantID:      "tenant_bench",
		ProjectID:     "project_bench",
		SourceID:      "source_bench",
		EventName:     "page_view",
		DistinctID:    "visitor_1",
		From:          time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC),
		To:            time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC),
		SortField:     storage.EventSortByReceivedAt,
		SortDirection: storage.EventSortDescending,
		Limit:         25,
	}
	highEventsQuery := storage.EventListQuery{
		TenantID:      "tenant_bench",
		ProjectID:     "project_bench",
		SourceID:      "source_bench",
		EventName:     "signup_clicked",
		DistinctID:    "visitor_1",
		From:          time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC),
		To:            time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC),
		SortField:     storage.EventSortByReceivedAt,
		SortDirection: storage.EventSortDescending,
		Filters: []storage.EventFilter{
			{Field: storage.EventFilterBySessionID, Operator: storage.EventFilterEquals, Value: "session_1"},
			{Field: storage.EventFilterByVisitID, Operator: storage.EventFilterEquals, Value: "visit_1"},
			{Field: storage.EventFilterBySourceType, Operator: storage.EventFilterNotEquals, Value: "server"},
		},
		PropertyFilters: []storage.EventPropertyFilter{
			{Scope: storage.PropertyScopeEvent, Name: "button", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "hero"},
			{Scope: storage.PropertyScopeEvent, Name: "plan", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "pro"},
			{Scope: storage.PropertyScopeUser, Name: "tier", ValueType: storage.PropertyValueString, Operator: storage.EventFilterEquals, StringValue: "team"},
		},
	}

	// Phase 2: define representative low, medium, and high query shapes with
	// expected evidence so the benchmark labels stay honest over time.
	scenarios := []struct {
		name     string
		build    func(context.Context, *EventQueryBuilder) (storage.EventQueryPlan, error)
		evidence storage.EventQueryEvidence
	}{
		{
			name: "low_realtime",
			build: func(ctx context.Context, builder *EventQueryBuilder) (storage.EventQueryPlan, error) {
				return builder.BuildRealtimeQuery(ctx, lowRealtimeQuery)
			},
			evidence: storage.EventQueryEvidence{
				Family:              storage.EventQueryFamilyRealtime,
				ReadPath:            storage.EventReadPathFactEvents,
				Optimization:        storage.EventQueryOptimizationDirectFactTable,
				ScalarFilterCount:   1,
				PropertyFilterCount: 0,
				UsesPropertyTable:   false,
				SortField:           storage.EventSortByEventTime,
				SortDirection:       storage.EventSortDescending,
			},
		},
		{
			name: "medium_events_scalar",
			build: func(ctx context.Context, builder *EventQueryBuilder) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, mediumEventsQuery)
			},
			evidence: storage.EventQueryEvidence{
				Family:              storage.EventQueryFamilyEvents,
				ReadPath:            storage.EventReadPathFactEvents,
				Optimization:        storage.EventQueryOptimizationDirectFactTable,
				ScalarFilterCount:   4,
				PropertyFilterCount: 0,
				UsesPropertyTable:   false,
				SortField:           storage.EventSortByReceivedAt,
				SortDirection:       storage.EventSortDescending,
			},
		},
		{
			name: "high_events_property",
			build: func(ctx context.Context, builder *EventQueryBuilder) (storage.EventQueryPlan, error) {
				return builder.BuildEventsQuery(ctx, highEventsQuery)
			},
			evidence: storage.EventQueryEvidence{
				Family:              storage.EventQueryFamilyEvents,
				ReadPath:            storage.EventReadPathFactEvents,
				Optimization:        storage.EventQueryOptimizationDirectFactTable,
				ScalarFilterCount:   7,
				PropertyFilterCount: 3,
				UsesPropertyTable:   true,
				SortField:           storage.EventSortByReceivedAt,
				SortDirection:       storage.EventSortDescending,
			},
		},
	}

	// Phase 3: measure only builder execution while pinning each scenario name
	// to the expected query evidence before the timer starts.
	ctx := context.Background()
	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
			assertBenchmarkEvidence(b, scenario.build, scenario.evidence, builder)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				plan, err := scenario.build(ctx, builder)
				if err != nil {
					b.Fatalf("build read-side query failed: %v", err)
				}
				if plan.QueryEvidence().Family == "" {
					b.Fatal("expected query evidence in benchmark plan")
				}
			}
		})
	}
}

// assertBenchmarkEvidence verifies that the benchmark scenario still matches
// the intended read-side evidence before timing begins.
func assertBenchmarkEvidence(
	b *testing.B,
	build func(context.Context, *EventQueryBuilder) (storage.EventQueryPlan, error),
	want storage.EventQueryEvidence,
	builder *EventQueryBuilder,
) {
	b.Helper()

	// Build one plan before ResetTimer so benchmark labels fail fast if a future
	// query-shape edit changes the evidence they are supposed to represent.
	plan, err := build(context.Background(), builder)
	if err != nil {
		b.Fatalf("build read-side query failed: %v", err)
	}
	got := plan.QueryEvidence()
	if got.Family != want.Family ||
		got.ReadPath != want.ReadPath ||
		got.Optimization != want.Optimization ||
		got.ScalarFilterCount != want.ScalarFilterCount ||
		got.PropertyFilterCount != want.PropertyFilterCount ||
		got.UsesPropertyTable != want.UsesPropertyTable ||
		got.SortField != want.SortField ||
		got.SortDirection != want.SortDirection {
		b.Fatalf("unexpected query evidence: got %#v want %#v", got, want)
	}
}
