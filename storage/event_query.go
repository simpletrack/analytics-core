package storage

import (
	"context"
	"errors"
	"time"
)

// ErrInvalidEventQuery marks caller-supplied query parameters rejected before storage execution.
var ErrInvalidEventQuery = errors.New("invalid event query")

// EventSortField is the allowlisted Events sort field.
type EventSortField string

const (
	// EventSortByEventTime sorts by the event timestamp produced by the source.
	EventSortByEventTime EventSortField = "event_time"
	// EventSortByReceivedAt sorts by the server-side collect acceptance timestamp.
	EventSortByReceivedAt EventSortField = "received_at"
	// EventSortByEventName sorts by the analytics event name.
	EventSortByEventName EventSortField = "event_name"
)

// EventSortDirection is the allowlisted Events sort direction.
type EventSortDirection string

const (
	// EventSortAscending sorts rows from lowest to highest value.
	EventSortAscending EventSortDirection = "asc"
	// EventSortDescending sorts rows from highest to lowest value.
	EventSortDescending EventSortDirection = "desc"
)

// EventFilterField is the allowlisted Events filter field.
type EventFilterField string

const (
	EventFilterByEventName  EventFilterField = "event_name"  // EventFilterByEventName filters by the analytics event name.
	EventFilterByDistinctID EventFilterField = "distinct_id" // EventFilterByDistinctID filters by the visitor or user identity key.
	EventFilterBySessionID  EventFilterField = "session_id"  // EventFilterBySessionID filters by the optional session key.
	EventFilterByVisitID    EventFilterField = "visit_id"    // EventFilterByVisitID filters by the canonical analytics visit key.
	EventFilterBySourceType EventFilterField = "source_type" // EventFilterBySourceType filters by the source category such as web, server, or mobile.
)

// EventFilterOperator is the allowlisted Events filter operator.
type EventFilterOperator string

const (
	// EventFilterEquals matches rows whose field equals Value.
	EventFilterEquals EventFilterOperator = "eq"
	// EventFilterNotEquals matches rows whose field does not equal Value.
	EventFilterNotEquals EventFilterOperator = "neq"
)

// EventFilter describes one allowlisted Events filter.
type EventFilter struct {
	Field    EventFilterField    // Field chooses one allowlisted event column
	Operator EventFilterOperator // Operator chooses one allowlisted comparison
	Value    string              // Value is always bound as a query argument
}

// PropertySelector identifies one property that may be used in query predicates.
type PropertySelector struct {
	Scope PropertyScope // Scope separates event properties from user properties
	Name  string        // Name is the normalized property key allowed for querying
}

// EventPropertyFilter describes one allowlisted typed property predicate.
type EventPropertyFilter struct {
	Scope       PropertyScope       // Scope chooses event or user properties
	Name        string              // Name is the normalized property key
	ValueType   PropertyValueType   // ValueType selects the typed value slot
	Operator    EventFilterOperator // Operator chooses one allowlisted comparison
	StringValue string              // StringValue is used when ValueType is string
	NumberValue float64             // NumberValue is used when ValueType is number
	BoolValue   bool                // BoolValue is used when ValueType is bool
}

// EventListQuery describes one paged Events table query.
type EventListQuery struct {
	TenantID                 string                // TenantID is the tenant boundary key
	ProjectID                string                // ProjectID is the project or website boundary key
	SourceID                 string                // SourceID is the source boundary key inside the project
	EventName                string                // EventName optionally filters to one analytics event name
	DistinctID               string                // DistinctID optionally filters to one visitor or user key
	From                     time.Time             // From optionally filters events at or after this event time
	To                       time.Time             // To optionally filters events before this event time
	Limit                    int                   // Limit caps returned rows before the builder-level maximum
	Offset                   int                   // Offset skips rows for Events pagination
	SortField                EventSortField        // SortField chooses one allowlisted Events sort field
	SortDirection            EventSortDirection    // SortDirection chooses asc or desc for SortField
	Filters                  []EventFilter         // Filters are extra allowlisted field/operator/value predicates
	PropertyFilters          []EventPropertyFilter // PropertyFilters are extra allowlisted typed property predicates
	AllowedPropertySelectors []PropertySelector    // AllowedPropertySelectors are source-scoped property filter allowlists
}

// RealtimeQuery describes the recent-events query used by Realtime.
type RealtimeQuery struct {
	TenantID  string    // TenantID is the tenant boundary key
	ProjectID string    // ProjectID is the project or website boundary key
	SourceID  string    // SourceID is the source boundary key inside the project
	Since     time.Time // Since filters events at or after this event time
	Limit     int       // Limit caps returned rows before the builder-level maximum
}

// EventQueryFamily identifies which product query produced a plan.
type EventQueryFamily string

const (
	// EventQueryFamilyEvents marks a paged Raw Events query.
	EventQueryFamilyEvents EventQueryFamily = "events"
	// EventQueryFamilyRealtime marks a recent-events Realtime query.
	EventQueryFamilyRealtime EventQueryFamily = "realtime"
)

// EventReadPath identifies the logical read model used by a query plan.
type EventReadPath string

const (
	// EventReadPathFactEvents reads directly from routed event fact tables.
	EventReadPathFactEvents EventReadPath = "fact_events"
)

// EventQueryOptimization identifies the physical optimization used by a query plan.
type EventQueryOptimization string

const (
	// EventQueryOptimizationDirectFactTable means no derived projection, MV, or aggregate table is used.
	EventQueryOptimizationDirectFactTable EventQueryOptimization = "direct_fact_table"
)

// EventQueryEvidence explains the read-side shape chosen by a query builder.
type EventQueryEvidence struct {
	Family              EventQueryFamily       // Family distinguishes Events from Realtime
	ReadPath            EventReadPath          // ReadPath identifies the logical read model
	Optimization        EventQueryOptimization // Optimization identifies the physical acceleration strategy
	ScalarFilterCount   int                    // ScalarFilterCount counts non-property predicates after source routing
	PropertyFilterCount int                    // PropertyFilterCount counts typed property predicates
	UsesPropertyTable   bool                   // UsesPropertyTable reports whether the typed property table participates
	SortField           EventSortField         // SortField records the effective allowlisted sort field
	SortDirection       EventSortDirection     // SortDirection records the effective allowlisted sort direction
}

// EventQueryPlan is the SQL and argument plan generated by a storage adapter.
//
// NOTE: analytics-core is still pre-v1. Builders that need read-side evidence
// must use NewEventQueryPlan; keyed literals that omit the private evidence
// field remain valid for tests or adapters that do not expose evidence.
type EventQueryPlan struct {
	SQL           string             // SQL is the generated query with driver placeholders
	Args          []any              // Args are the bound query values in driver order
	LogicalTable  string             // LogicalTable is the stable model name exposed to analysis modules
	PhysicalTable string             // PhysicalTable is the adapter-owned ClickHouse table name
	Limit         int                // Limit is the effective row cap after defaults and maximums
	evidence      EventQueryEvidence // evidence records the read-side path and guardrails used to build the plan
}

// NewEventQueryPlan creates a storage-neutral query plan with read-side evidence.
func NewEventQueryPlan(sql string, args []any, logicalTable, physicalTable string, limit int, evidence EventQueryEvidence) EventQueryPlan {
	return EventQueryPlan{
		SQL:           sql,
		Args:          append([]any(nil), args...),
		LogicalTable:  logicalTable,
		PhysicalTable: physicalTable,
		Limit:         limit,
		evidence:      evidence,
	}
}

// QueryEvidence returns the structural read-side metadata associated with the plan.
func (p EventQueryPlan) QueryEvidence() EventQueryEvidence {
	return p.evidence
}

// EventQueryResult returns records together with the read-side evidence used to fetch them.
type EventQueryResult struct {
	Records  []EventRecord      // Records are the storage-neutral analytics rows returned by the query
	Evidence EventQueryEvidence // Evidence explains the read path, guardrails, and optimization used by the query
}

// EventRecord is one event row returned by an analytics query.
type EventRecord struct {
	ID             string    // ID is the stable event id used for idempotent ingestion
	TenantID       string    // TenantID is the tenant boundary key
	ProjectID      string    // ProjectID is the project or website boundary key
	SourceID       string    // SourceID is the source boundary key inside the project
	SourceType     string    // SourceType is the source category such as web, server, or mobile
	EventName      string    // EventName is the analytics event name
	DistinctID     string    // DistinctID is the visitor or user identity key
	SessionID      string    // SessionID is the optional session key
	VisitID        string    // VisitID is the canonical analytics visit key read from storage
	EventTime      time.Time // EventTime is the timestamp produced by the source
	ReceivedAt     time.Time // ReceivedAt is the timestamp accepted by collect
	Properties     string    // Properties is the serialized event-scoped JSON payload
	UserProperties string    // UserProperties is the serialized user-scoped JSON payload
	Source         string    // Source is the optional source label for diagnostics
}

// EventQueryBuilder builds storage-specific event query plans.
type EventQueryBuilder interface {
	// BuildEventsQuery builds the paged Events table query.
	BuildEventsQuery(context.Context, EventListQuery) (EventQueryPlan, error)
	// BuildRealtimeQuery builds the recent-events Realtime query.
	BuildRealtimeQuery(context.Context, RealtimeQuery) (EventQueryPlan, error)
}

// EventReader executes event queries against the storage backend.
type EventReader interface {
	// ListEvents returns paged Events rows for one tenant/project/source.
	ListEvents(context.Context, EventListQuery) ([]EventRecord, error)
	// ListRealtime returns recent Realtime rows for one tenant/project/source.
	ListRealtime(context.Context, RealtimeQuery) ([]EventRecord, error)
}

// EventReaderWithEvidence executes event queries and returns read-side planning evidence.
//
// Services can use this optional interface to surface query-plan metadata to
// internal APIs or operators without coupling HTTP handlers to ClickHouse SQL.
type EventReaderWithEvidence interface {
	EventReader
	// ListEventsWithEvidence returns Events rows and the query evidence used to fetch them.
	ListEventsWithEvidence(context.Context, EventListQuery) (EventQueryResult, error)
	// ListRealtimeWithEvidence returns Realtime rows and the query evidence used to fetch them.
	ListRealtimeWithEvidence(context.Context, RealtimeQuery) (EventQueryResult, error)
}
