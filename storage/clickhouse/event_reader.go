package clickhouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

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
	if r == nil {
		return nil, errors.New("event reader is required")
	}

	// Build the routed plan first so execution uses the same SQL path tested by
	// Realtime, Events, and future analysis modules.
	plan, err := r.builder.BuildEventsQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	return r.executePlan(ctx, plan)
}

// ListRealtime executes the recent-events Realtime query.
func (r *EventReader) ListRealtime(ctx context.Context, query storage.RealtimeQuery) ([]storage.EventRecord, error) {
	if r == nil {
		return nil, errors.New("event reader is required")
	}

	// Realtime reuses the same execution method as Events, which keeps result
	// scanning and error handling identical across P1 query views.
	plan, err := r.builder.BuildRealtimeQuery(ctx, query)
	if err != nil {
		return nil, err
	}
	return r.executePlan(ctx, plan)
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
		EventTime:      row.EventTime,
		ReceivedAt:     row.ReceivedAt,
		Properties:     row.Properties,
		UserProperties: row.UserProperties,
		Source:         row.Source,
		VisitID:        deriveVisitID(row.SessionID, row.EventTime),
	}
}

// deriveVisitID reconstructs a stable visit key from the session key and the
// current 30-minute collect window used by the provisional session resolver.
//
// NOTE: This helper is readback-only. It stays independent from table-routing
// hashes so a future routing change cannot alter the public visit_id format.
func deriveVisitID(sessionID string, eventTime time.Time) string {
	if sessionID == "" || eventTime.IsZero() {
		return ""
	}
	bucket := eventTime.UTC().Truncate(30 * time.Minute).Unix()
	return "vis_" + visitDigest(sessionID, bucket)
}

// visitDigest returns a dedicated 128-bit digest for provisional visit IDs.
func visitDigest(sessionID string, bucket int64) string {
	sum := sha256.Sum256([]byte(sessionID + ":" + strconv.FormatInt(bucket, 10)))
	return hex.EncodeToString(sum[:16])
}
