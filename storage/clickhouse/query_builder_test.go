package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/simpletrack/analytics-core/storage"
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

func TestEventQueryBuilderUsesFilterAllowlist(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	plan, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		Filters: []storage.EventFilter{
			{
				Field:    storage.EventFilterBySessionID,
				Operator: storage.EventFilterEquals,
				Value:    "session_1",
			},
			{
				Field:    storage.EventFilterByVisitID,
				Operator: storage.EventFilterEquals,
				Value:    "visit_1",
			},
			{
				Field:    storage.EventFilterBySourceType,
				Operator: storage.EventFilterNotEquals,
				Value:    "server",
			},
		},
	})
	if err != nil {
		t.Fatalf("build events query failed: %v", err)
	}

	for _, fragment := range []string{
		"session_id = ?",
		"visit_id = ?",
		"source_type != ?",
	} {
		if !strings.Contains(plan.SQL, fragment) {
			t.Fatalf("expected SQL fragment %q in %q", fragment, plan.SQL)
		}
	}
	if len(plan.Args) != 7 {
		t.Fatalf("expected tenant/project/source/filter/filter/limit args, got %d: %#v", len(plan.Args), plan.Args)
	}
}

func TestEventQueryBuilderBuildsPropertyFilters(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router, WithAllowedPropertyFilters(
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"},
		storage.PropertySelector{Scope: storage.PropertyScopeUser, Name: "plan"},
	))
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	plan, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		PropertyFilters: []storage.EventPropertyFilter{
			{
				Scope:       storage.PropertyScopeEvent,
				Name:        "button",
				ValueType:   storage.PropertyValueString,
				Operator:    storage.EventFilterEquals,
				StringValue: "hero",
			},
			{
				Scope:       storage.PropertyScopeUser,
				Name:        "plan",
				ValueType:   storage.PropertyValueString,
				Operator:    storage.EventFilterNotEquals,
				StringValue: "free",
			},
		},
	})
	if err != nil {
		t.Fatalf("build events query failed: %v", err)
	}

	propertyTable := plan.PhysicalTable + propertyTableSuffix
	for _, fragment := range []string{
		"FROM `" + plan.PhysicalTable + "`",
		"(tenant_id, project_id, source_id, event_id) IN (SELECT tenant_id, project_id, source_id, event_id FROM `" + propertyTable + "`",
		"property_scope = ? AND property_name = ? AND property_type = ? AND string_value = ?",
		"property_scope = ? AND property_name = ? AND property_type = ? AND string_value != ?",
	} {
		if !strings.Contains(plan.SQL, fragment) {
			t.Fatalf("expected SQL fragment %q in %q", fragment, plan.SQL)
		}
	}
	if strings.Contains(plan.SQL, "hero") || strings.Contains(plan.SQL, "free") {
		t.Fatalf("property values should be bound args, not SQL literals: %s", plan.SQL)
	}
	if len(plan.Args) != 18 {
		t.Fatalf("expected tenant/project/source/property/property/limit args, got %d: %#v", len(plan.Args), plan.Args)
	}
}

func TestEventQueryBuilderUsesQueryScopedPropertyAllowlist(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router)
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	plan, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
		AllowedPropertySelectors: []storage.PropertySelector{
			{Scope: storage.PropertyScopeEvent, Name: "button"},
		},
		PropertyFilters: []storage.EventPropertyFilter{
			{
				Scope:       storage.PropertyScopeEvent,
				Name:        "button",
				ValueType:   storage.PropertyValueString,
				Operator:    storage.EventFilterEquals,
				StringValue: "hero",
			},
		},
	})
	if err != nil {
		t.Fatalf("build events query with query-scoped allowlist failed: %v", err)
	}
	if !strings.Contains(plan.SQL, "property_scope = ? AND property_name = ? AND property_type = ? AND string_value = ?") {
		t.Fatalf("expected property predicate in %q", plan.SQL)
	}
}

func TestEventQueryBuilderRejectsInvalidPropertyFilters(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}
	builder, err := NewEventQueryBuilder(router, WithAllowedPropertyFilters(
		storage.PropertySelector{Scope: storage.PropertyScopeEvent, Name: "button"},
	))
	if err != nil {
		t.Fatalf("new event query builder failed: %v", err)
	}

	cases := []struct {
		name   string
		filter storage.EventPropertyFilter
		want   string
	}{
		{
			name: "unsupported scope",
			filter: storage.EventPropertyFilter{
				Scope:     storage.PropertyScope("session"),
				Name:      "button",
				ValueType: storage.PropertyValueString,
				Operator:  storage.EventFilterEquals,
			},
			want: "unsupported property scope",
		},
		{
			name: "not allowlisted",
			filter: storage.EventPropertyFilter{
				Scope:     storage.PropertyScopeEvent,
				Name:      "raw_sql",
				ValueType: storage.PropertyValueString,
				Operator:  storage.EventFilterEquals,
			},
			want: "is not allowlisted",
		},
		{
			name: "unsupported value type",
			filter: storage.EventPropertyFilter{
				Scope:     storage.PropertyScopeEvent,
				Name:      "button",
				ValueType: storage.PropertyValueType("json"),
				Operator:  storage.EventFilterEquals,
			},
			want: "unsupported property value type",
		},
		{
			name: "null not equals",
			filter: storage.EventPropertyFilter{
				Scope:     storage.PropertyScopeEvent,
				Name:      "button",
				ValueType: storage.PropertyValueNull,
				Operator:  storage.EventFilterNotEquals,
			},
			want: "null only supports eq",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := builder.BuildEventsQuery(context.Background(), storage.EventListQuery{
				TenantID:        "tenant_1",
				ProjectID:       "project_1",
				SourceID:        "source_1",
				PropertyFilters: []storage.EventPropertyFilter{tc.filter},
			})
			if err == nil {
				t.Fatal("expected invalid event query error")
			}
			if !errors.Is(err, storage.ErrInvalidEventQuery) {
				t.Fatalf("expected ErrInvalidEventQuery, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
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

	now := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		query storage.EventListQuery
		want  string
	}{
		{
			name: "missing source id",
			query: storage.EventListQuery{
				TenantID:  "tenant_1",
				ProjectID: "project_1",
			},
			want: "source_id is required",
		},
		{
			name: "negative offset",
			query: storage.EventListQuery{
				TenantID:  "tenant_1",
				ProjectID: "project_1",
				SourceID:  "source_1",
				Offset:    -1,
			},
			want: "offset must be non-negative",
		},
		{
			name: "invalid time range",
			query: storage.EventListQuery{
				TenantID:  "tenant_1",
				ProjectID: "project_1",
				SourceID:  "source_1",
				From:      now,
				To:        now,
			},
			want: "from must be before to",
		},
		{
			name: "unsupported sort field",
			query: storage.EventListQuery{
				TenantID:  "tenant_1",
				ProjectID: "project_1",
				SourceID:  "source_1",
				SortField: storage.EventSortField("bad_field"),
			},
			want: "unsupported events sort field",
		},
		{
			name: "unsupported sort direction",
			query: storage.EventListQuery{
				TenantID:      "tenant_1",
				ProjectID:     "project_1",
				SourceID:      "source_1",
				SortDirection: storage.EventSortDirection("sideways"),
			},
			want: "unsupported events sort direction",
		},
		{
			name: "unsupported filter field",
			query: storage.EventListQuery{
				TenantID:  "tenant_1",
				ProjectID: "project_1",
				SourceID:  "source_1",
				Filters: []storage.EventFilter{
					{
						Field:    storage.EventFilterField("raw_sql"),
						Operator: storage.EventFilterEquals,
						Value:    "page_view",
					},
				},
			},
			want: "unsupported events filter field",
		},
		{
			name: "unsupported filter operator",
			query: storage.EventListQuery{
				TenantID:  "tenant_1",
				ProjectID: "project_1",
				SourceID:  "source_1",
				Filters: []storage.EventFilter{
					{
						Field:    storage.EventFilterByEventName,
						Operator: storage.EventFilterOperator("contains"),
						Value:    "page",
					},
				},
			},
			want: "unsupported events filter operator",
		},
		{
			name: "empty filter value",
			query: storage.EventListQuery{
				TenantID:  "tenant_1",
				ProjectID: "project_1",
				SourceID:  "source_1",
				Filters: []storage.EventFilter{
					{
						Field:    storage.EventFilterByEventName,
						Operator: storage.EventFilterEquals,
					},
				},
			},
			want: "event filter 0 value is required",
		},
		{
			name: "too many filters",
			query: storage.EventListQuery{
				TenantID:  "tenant_1",
				ProjectID: "project_1",
				SourceID:  "source_1",
				Filters:   make([]storage.EventFilter, defaultMaxFilters+1),
			},
			want: "too many event filters",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := builder.BuildEventsQuery(context.Background(), tc.query)
			if err == nil {
				t.Fatal("expected invalid event query error")
			}
			if !errors.Is(err, storage.ErrInvalidEventQuery) {
				t.Fatalf("expected ErrInvalidEventQuery, got %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}
