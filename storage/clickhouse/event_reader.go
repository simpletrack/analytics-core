package clickhouse

import (
	"context"
	"errors"
	"strings"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
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

// CountEvents executes a bounded event-count query.
func (r *EventReader) CountEvents(ctx context.Context, query storage.EventCountQuery) (int64, error) {
	result, err := r.CountEventsWithEvidence(ctx, query)
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

// CountEventsWithEvidence executes a bounded event-count query and returns read-side evidence.
func (r *EventReader) CountEventsWithEvidence(ctx context.Context, query storage.EventCountQuery) (storage.EventCountResult, error) {
	if r == nil {
		return storage.EventCountResult{}, errors.New("event reader is required")
	}

	// Goal summaries need exact counts, but they must still pass through the
	// same routed table and allowlisted predicate builder as Events readback.
	plan, err := r.builder.BuildEventCountQuery(ctx, query)
	if err != nil {
		return storage.EventCountResult{}, err
	}
	count, err := r.executeCountPlan(ctx, plan)
	if err != nil {
		return storage.EventCountResult{}, err
	}
	return storage.EventCountResult{
		Count:    count,
		Evidence: plan.QueryEvidence(),
	}, nil
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
		if isMissingClickHouseTableError(err) {
			return []storage.EventRecord{}, nil
		}
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

func (r *EventReader) executeCountPlan(ctx context.Context, plan storage.EventQueryPlan) (int64, error) {
	var rows []eventCountRowModel

	// Raw executes a sealed count plan. The count route is used by Goal P1, so
	// it intentionally avoids scanning event rows just to compute a total.
	if err := r.db.WithContext(ctx).Raw(plan.SQL, plan.Args...).Scan(&rows).Error; err != nil {
		if isMissingClickHouseTableError(err) {
			return 0, nil
		}
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Count, nil
}

func isMissingClickHouseTableError(err error) bool {
	var exception *clickhouseproto.Exception
	if !errors.As(err, &exception) {
		return false
	}
	if exception.Code != 60 {
		return false
	}
	message := strings.ToLower(exception.Message)
	return strings.Contains(message, "unknown table") || strings.Contains(message, "doesn't exist")
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
