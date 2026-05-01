package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/internal/storage"
)

func TestEventQueryBuilderBuildsEventsQuery(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	from := time.Date(2026, 5, 1, 8, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	plan, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:   "tenant_1",
		ProjectID:  "project_1",
		SourceID:   "source_1",
		EventName:  "page_view",
		DistinctID: "visitor_1",
		From:       from,
		To:         to,
		Limit:      25,
		Offset:     10,
	})
	if err != nil {
		t.Fatalf("build events query failed: %v", err)
	}

	if plan.LogicalTable != defaultLogicalTable {
		t.Fatalf("expected logical events table, got %q", plan.LogicalTable)
	}
	if !strings.HasPrefix(plan.PhysicalTable, "events_") {
		t.Fatalf("expected routed physical table, got %q", plan.PhysicalTable)
	}
	if strings.Contains(plan.PhysicalTable, "tenant_1") || strings.Contains(plan.SQL, "tenant_1") {
		t.Fatalf("query should bind raw tenant id instead of embedding it: %s", plan.SQL)
	}
	for _, fragment := range []string{
		"FROM `" + plan.PhysicalTable + "`",
		"tenant_id = ? AND project_id = ? AND source_id = ?",
		"event_time >= ?",
		"event_time < ?",
		"event_name = ?",
		"distinct_id = ?",
		"ORDER BY event_time DESC,received_at DESC",
		"LIMIT ?",
		"OFFSET ?",
	} {
		if !strings.Contains(plan.SQL, fragment) {
			t.Fatalf("expected SQL fragment %q in %q", fragment, plan.SQL)
		}
	}
	if len(plan.Args) != 9 {
		t.Fatalf("expected 9 bound args, got %d: %#v", len(plan.Args), plan.Args)
	}
	if plan.Limit != 25 {
		t.Fatalf("expected effective limit 25, got %d", plan.Limit)
	}
}

func TestEventQueryBuilderBuildsRealtimeQuery(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router, WithMaxQueryLimit(75))
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	since := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	plan, err := builder.BuildRealtimeQuery(context.Background(), storage.RealtimeQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Since:     since,
		Limit:     500,
	})
	if err != nil {
		t.Fatalf("build realtime query failed: %v", err)
	}

	if plan.Limit != 75 {
		t.Fatalf("expected max-capped limit 75, got %d", plan.Limit)
	}
	if !strings.Contains(plan.SQL, "event_time >= ?") {
		t.Fatalf("expected realtime since filter in %q", plan.SQL)
	}
	if len(plan.Args) != 5 {
		t.Fatalf("expected tenant/project/source/since/limit args, got %d: %#v", len(plan.Args), plan.Args)
	}
}

func TestEventQueryBuilderUsesSortAllowlist(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	plan, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:      "tenant_1",
		ProjectID:     "project_1",
		SourceID:      "source_1",
		SortField:     storage.EventSortByReceivedAt,
		SortDirection: storage.EventSortAscending,
	})
	if err != nil {
		t.Fatalf("build events query failed: %v", err)
	}

	if !strings.Contains(plan.SQL, "ORDER BY received_at ASC,event_time DESC") {
		t.Fatalf("expected allowlisted received_at sort in %q", plan.SQL)
	}
}

func TestEventQueryBuilderRejectsInvalidQueries(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	if _, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
	}); err == nil {
		t.Fatal("expected missing source_id error")
	}
	if _, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Offset:    -1,
	}); err == nil {
		t.Fatal("expected negative offset error")
	}
	now := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	if _, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		From:      now,
		To:        now,
	}); err == nil {
		t.Fatal("expected invalid time range error")
	}
	if _, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		SortField: storage.EventSortField("bad_field"),
	}); err == nil {
		t.Fatal("expected unsupported sort field error")
	}
	if _, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:      "tenant_1",
		ProjectID:     "project_1",
		SourceID:      "source_1",
		SortDirection: storage.EventSortDirection("sideways"),
	}); err == nil {
		t.Fatal("expected unsupported sort direction error")
	}
}
