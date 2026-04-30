package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/internal/storage"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

const (
	defaultEventsLimit   = 100
	defaultRealtimeLimit = 50
	defaultMaxQueryLimit = 1000
)

var eventSelectColumns = []string{
	"event_id",
	"tenant_id",
	"project_id",
	"source_id",
	"source_type",
	"event_name",
	"distinct_id",
	"session_id",
	"event_time",
	"received_at",
	"properties",
	"user_properties",
	"source",
}

// EventQueryBuilderOption customizes the ClickHouse query builder.
type EventQueryBuilderOption func(*EventQueryBuilder)

// WithQueryDB replaces the default dry-run GORM ClickHouse database.
func WithQueryDB(db *gorm.DB) EventQueryBuilderOption {
	return func(builder *EventQueryBuilder) {
		builder.db = db
	}
}

// WithMaxQueryLimit caps Events and Realtime query limits.
func WithMaxQueryLimit(max int) EventQueryBuilderOption {
	return func(builder *EventQueryBuilder) {
		builder.maxLimit = max
	}
}

// EventQueryBuilder builds ClickHouse Events and Realtime query plans.
//
// The builder is intentionally plan-only in P1. Query execution can be added
// behind the storage.EventQueryBuilder boundary without exposing dynamic
// physical tables, GORM internals, or ClickHouse driver details to analysis
// modules.
type EventQueryBuilder struct {
	router   *TableRouter // router resolves tenant/project/source to physical event tables
	db       *gorm.DB     // db is the GORM builder used in dry-run mode for SQL and args
	maxLimit int          // maxLimit prevents unbounded product queries
}

// NewEventQueryBuilder creates a ClickHouse event query builder.
func NewEventQueryBuilder(router *TableRouter, opts ...EventQueryBuilderOption) (*EventQueryBuilder, error) {
	if router == nil {
		return nil, errors.New("table router is required")
	}

	// Start with a dry-run ClickHouse dialector so the query path uses GORM's
	// SQL builder while tests and planning do not need a live database.
	db, err := newDryRunQueryDB()
	if err != nil {
		return nil, err
	}
	builder := &EventQueryBuilder{
		router:   router,
		db:       db,
		maxLimit: defaultMaxQueryLimit,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(builder)
		}
	}
	if builder.db == nil {
		return nil, errors.New("gorm query db is required")
	}
	if builder.maxLimit <= 0 {
		return nil, errors.New("max query limit must be positive")
	}
	return builder, nil
}

// BuildEventsQuery builds a paged Events query for one tenant/project/source.
func (b *EventQueryBuilder) BuildEventsQuery(ctx context.Context, query storage.EventListQuery) (storage.EventQueryPlan, error) {
	if b == nil {
		return storage.EventQueryPlan{}, errors.New("event query builder is required")
	}

	// Reject invalid caller windows before table routing so malformed UI/API
	// input cannot produce a broad or ambiguous ClickHouse scan.
	if query.Offset < 0 {
		return storage.EventQueryPlan{}, errors.New("offset must be non-negative")
	}
	if !query.From.IsZero() && !query.To.IsZero() && !query.From.Before(query.To) {
		return storage.EventQueryPlan{}, errors.New("from must be before to")
	}

	// Resolve the physical table before touching GORM so dynamic table routing
	// stays owned by the ClickHouse adapter.
	table, err := b.routeQuery(query.TenantID, query.ProjectID, query.SourceID)
	if err != nil {
		return storage.EventQueryPlan{}, err
	}

	// Normalize limits at the boundary; handlers should not decide ClickHouse
	// safety caps or default pagination behavior.
	limit := b.normalizeLimit(query.Limit, defaultEventsLimit)

	// Compose the query through GORM clauses so filters, ordering, and pagination
	// share one path for Events, Realtime, and later funnel/retention views.
	scope := b.baseScope(ctx, table.Physical).
		Where("tenant_id = ? AND project_id = ? AND source_id = ?", query.TenantID, query.ProjectID, query.SourceID)
	if !query.From.IsZero() {
		scope = scope.Where("event_time >= ?", query.From.UTC())
	}
	if !query.To.IsZero() {
		scope = scope.Where("event_time < ?", query.To.UTC())
	}
	if query.EventName != "" {
		scope = scope.Where("event_name = ?", query.EventName)
	}
	if query.DistinctID != "" {
		scope = scope.Where("distinct_id = ?", query.DistinctID)
	}
	scope = scope.Order("event_time DESC").Order("received_at DESC").Limit(limit)
	if query.Offset > 0 {
		scope = scope.Offset(query.Offset)
	}

	return buildPlan(scope, table, limit)
}

// BuildRealtimeQuery builds the Realtime recent-events query.
func (b *EventQueryBuilder) BuildRealtimeQuery(ctx context.Context, query storage.RealtimeQuery) (storage.EventQueryPlan, error) {
	if b == nil {
		return storage.EventQueryPlan{}, errors.New("event query builder is required")
	}

	// Realtime is the same logical query family as Events, but it defaults to a
	// smaller limit and uses Since as the minimum event time.
	return b.BuildEventsQuery(ctx, storage.EventListQuery{
		TenantID:  query.TenantID,
		ProjectID: query.ProjectID,
		SourceID:  query.SourceID,
		From:      query.Since,
		Limit:     b.normalizeLimit(query.Limit, defaultRealtimeLimit),
	})
}

func (b *EventQueryBuilder) routeQuery(tenantID string, projectID string, sourceID string) (Table, error) {
	// RouteKey shares the write-path table strategy and keeps query builders
	// from constructing table names directly.
	return b.router.RouteKey(RoutingKey{
		TenantID:  tenantID,
		ProjectID: projectID,
		SourceID:  sourceID,
	})
}

func (b *EventQueryBuilder) normalizeLimit(limit int, fallback int) int {
	// Invalid or absent limits fall back to a product-safe default; oversized
	// limits are capped instead of rejected so UI callers can stay simple.
	if limit <= 0 {
		limit = fallback
	}
	if limit > b.maxLimit {
		return b.maxLimit
	}
	return limit
}

func (b *EventQueryBuilder) baseScope(ctx context.Context, tableName string) *gorm.DB {
	// NewDB avoids inheriting previous clauses if the builder is reused, and
	// DryRun keeps Build* methods side-effect free until an executor is added.
	return b.db.WithContext(ctx).
		Session(&gorm.Session{DryRun: true, NewDB: true}).
		Table(tableName).
		Select(eventSelectColumns)
}

func buildPlan(scope *gorm.DB, table Table, limit int) (storage.EventQueryPlan, error) {
	// Find materializes GORM's statement without executing because the session
	// is dry-run; this is the single query-building exit for P1 views.
	stmt := scope.Find(&[]eventRowModel{}).Statement
	if stmt == nil || stmt.SQL.Len() == 0 {
		return storage.EventQueryPlan{}, errors.New("gorm did not build an event query")
	}
	return storage.EventQueryPlan{
		SQL:           stmt.SQL.String(),
		Args:          append([]any(nil), stmt.Vars...),
		LogicalTable:  table.Logical,
		PhysicalTable: table.Physical,
		Limit:         limit,
	}, nil
}

func newDryRunQueryDB() (*gorm.DB, error) {
	// SkipInitializeWithVersion and DisableAutomaticPing let the builder use the
	// ClickHouse dialect without requiring a local ClickHouse server in tests.
	db, err := gorm.Open(clickhouse.New(clickhouse.Config{
		DSN:                       "clickhouse://127.0.0.1:9000/default",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{
		DryRun:               true,
		DisableAutomaticPing: true,
	})
	if err != nil {
		return nil, fmt.Errorf("open dry-run clickhouse gorm db: %w", err)
	}
	return db, nil
}

type eventRowModel struct {
	EventID        string    `gorm:"column:event_id"`        // EventID is the stable event id
	TenantID       string    `gorm:"column:tenant_id"`       // TenantID is the tenant boundary key
	ProjectID      string    `gorm:"column:project_id"`      // ProjectID is the project or website boundary key
	SourceID       string    `gorm:"column:source_id"`       // SourceID is the source boundary key inside the project
	SourceType     string    `gorm:"column:source_type"`     // SourceType is the source category
	EventName      string    `gorm:"column:event_name"`      // EventName is the analytics event name
	DistinctID     string    `gorm:"column:distinct_id"`     // DistinctID is the visitor or user key
	SessionID      string    `gorm:"column:session_id"`      // SessionID is the optional session key
	EventTime      time.Time `gorm:"column:event_time"`      // EventTime is the timestamp produced by the source
	ReceivedAt     time.Time `gorm:"column:received_at"`     // ReceivedAt is the timestamp accepted by collect
	Properties     string    `gorm:"column:properties"`      // Properties is the serialized event property payload
	UserProperties string    `gorm:"column:user_properties"` // UserProperties is the serialized user property payload
	Source         string    `gorm:"column:source"`          // Source is the optional source label for diagnostics
}
