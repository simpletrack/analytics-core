package clickhouse

import (
	"context"
	"errors"

	"github.com/simpletrack/analytics-core/storage"
	"gorm.io/gorm"
)

// EventReader executes ClickHouse Events and Realtime queries.
//
// Query construction stays delegated to EventQueryBuilder so execution cannot
// bypass physical table routing, field allowlists, pagination caps, or future
// permission filters.
type EventReader struct {
	db      *gorm.DB           // db executes ClickHouse query plans through GORM Raw
	builder *EventQueryBuilder // builder owns routed SQL plans and query guardrails
}

// NewEventReader creates a ClickHouse event reader.
func NewEventReader(db *gorm.DB, builder *EventQueryBuilder) (*EventReader, error) {
	if db == nil {
		return nil, errors.New("gorm db is required")
	}
	if builder == nil {
		return nil, errors.New("event query builder is required")
	}
	return &EventReader{db: db, builder: builder}, nil
}

// ListEvents executes the paged Events query.
func (r *EventReader) ListEvents(ctx context.Context, query storage.EventListQuery) ([]storage.EventRecord, error) {
	result, err := r.ListEventsWithEvidence(ctx, query)
	if err != nil {
		return nil, err
	}
	return result.Records, nil
}

// ListEventsWithEvidence executes the paged Events query and returns read-side evidence.
func (r *EventReader) ListEventsWithEvidence(ctx context.Context, query storage.EventListQuery) (storage.EventQueryResult, error) {
	if r == nil {
		return storage.EventQueryResult{}, errors.New("event reader is required")
	}

	// Build the routed plan first so execution uses the same SQL path tested by
	// Realtime, Events, and future analysis modules.
	plan, err := r.builder.BuildEventsQuery(ctx, query)
	if err != nil {
		return storage.EventQueryResult{}, err
	}
	records, err := r.executePlan(ctx, plan)
	if err != nil {
		return storage.EventQueryResult{}, err
	}
	return storage.EventQueryResult{
		Records:  records,
		Evidence: plan.QueryEvidence(),
	}, nil
}

// ListRealtime executes the recent-events Realtime query.
func (r *EventReader) ListRealtime(ctx context.Context, query storage.RealtimeQuery) ([]storage.EventRecord, error) {
	result, err := r.ListRealtimeWithEvidence(ctx, query)
	if err != nil {
		return nil, err
	}
	return result.Records, nil
}

// ListRealtimeWithEvidence executes the recent-events Realtime query and returns read-side evidence.
func (r *EventReader) ListRealtimeWithEvidence(ctx context.Context, query storage.RealtimeQuery) (storage.EventQueryResult, error) {
	if r == nil {
		return storage.EventQueryResult{}, errors.New("event reader is required")
	}

	// Realtime reuses the same execution method as Events, which keeps result
	// scanning and error handling identical across P1 query views.
	plan, err := r.builder.BuildRealtimeQuery(ctx, query)
	if err != nil {
		return storage.EventQueryResult{}, err
	}
	records, err := r.executePlan(ctx, plan)
	if err != nil {
		return storage.EventQueryResult{}, err
	}
	return storage.EventQueryResult{
		Records:  records,
		Evidence: plan.QueryEvidence(),
	}, nil
}

func (r *EventReader) executePlan(ctx context.Context, plan storage.EventQueryPlan) ([]storage.EventRecord, error) {
	var rows []eventRowModel

	// Raw executes the already-built query plan; dynamic table names and filters
	// cannot be changed here because they were sealed by EventQueryBuilder.
	if err := r.db.WithContext(ctx).Raw(plan.SQL, plan.Args...).Scan(&rows).Error; err != nil {
		return nil, err
	}

	// Convert adapter scan models to storage-neutral records so analysis modules
	// do not import GORM tags or ClickHouse-specific row structs.
	records := make([]storage.EventRecord, 0, len(rows))
	for _, row := range rows {
		records = append(records, row.toRecord())
	}
	return records, nil
}

func (row eventRowModel) toRecord() storage.EventRecord {
	return storage.EventRecord{
		ID:             row.EventID,
		TenantID:       row.TenantID,
		ProjectID:      row.ProjectID,
		SourceID:       row.SourceID,
		SourceType:     row.SourceType,
		EventName:      row.EventName,
		DistinctID:     row.DistinctID,
		SessionID:      row.SessionID,
		VisitID:        row.VisitID,
		EventTime:      row.EventTime,
		ReceivedAt:     row.ReceivedAt,
		Properties:     row.Properties,
		UserProperties: row.UserProperties,
		Source:         row.Source,
	}
}
