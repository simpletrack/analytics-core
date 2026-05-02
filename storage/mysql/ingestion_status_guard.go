package mysql

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/storage"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ingestionStatusProcessing = "processing"
	ingestionStatusInserted   = "inserted"
	ingestionStatusFailed     = "failed"
)

// Clock returns the current time for status rows.
type Clock func() time.Time

// IngestionStatus records the idempotency state for one analytics event.
//
// The composite primary key is the business-neutral event identity used across
// Redis Stream and Kafka deliveries. It intentionally avoids queue offsets so a
// replay through another EventBus implementation still maps to the same guard.
type IngestionStatus struct {
	TenantID   string    `gorm:"column:tenant_id;primaryKey;size:128"`  // TenantID is the tenant boundary key
	ProjectID  string    `gorm:"column:project_id;primaryKey;size:128"` // ProjectID is the project or product boundary key
	SourceID   string    `gorm:"column:source_id;primaryKey;size:128"`  // SourceID is the source boundary key inside the project
	EventID    string    `gorm:"column:event_id;primaryKey;size:128"`   // EventID is the stable event id supplied by collect
	Status     string    `gorm:"column:status;size:32;not null"`        // Status is processing, inserted, or failed
	Attempt    int       `gorm:"column:attempt;not null;default:1"`     // Attempt counts write claims for diagnostics and retry decisions
	LastError  string    `gorm:"column:last_error;type:text"`           // LastError stores the most recent append failure summary
	ReceivedAt time.Time `gorm:"column:received_at;not null"`           // ReceivedAt is the collect acceptance timestamp for the event
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime"`      // CreatedAt is maintained by GORM when the row is created
	UpdatedAt  time.Time `gorm:"column:updated_at;autoUpdateTime"`      // UpdatedAt is maintained by GORM when the row changes
}

// TableName returns the stable ingestion status table name.
func (IngestionStatus) TableName() string {
	return "ingestion_status"
}

// IngestionStatusGuard implements storage.EventWriteGuard with GORM.
//
// The guard records event write state in MySQL before BatchWriter appends to
// ClickHouse. It does not make MySQL and ClickHouse transactional together; it
// narrows duplicate windows for at-least-once delivery and gives retries a
// durable checkpoint to consult.
type IngestionStatusGuard struct {
	db  *gorm.DB // db executes status-row inserts and updates
	now Clock    // now supplies deterministic timestamps in tests
}

// GuardOption customizes IngestionStatusGuard dependencies.
type GuardOption func(*IngestionStatusGuard)

// WithClock sets the clock used for status row updates.
func WithClock(now Clock) GuardOption {
	return func(guard *IngestionStatusGuard) {
		// The clock option makes retry and rollback tests deterministic without
		// pushing test-only time control into production code.
		guard.now = now
	}
}

// NewIngestionStatusGuard creates a GORM-backed EventWriteGuard.
func NewIngestionStatusGuard(db *gorm.DB, opts ...GuardOption) (*IngestionStatusGuard, error) {
	if db == nil {
		return nil, errors.New("gorm db is required")
	}

	// The guard performs one-row checkpoint updates; disabling GORM's default
	// transaction avoids wrapping each small status mutation in extra SQL.
	guard := &IngestionStatusGuard{
		db:  db.Session(&gorm.Session{SkipDefaultTransaction: true}),
		now: time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(guard)
		}
	}
	if guard.now == nil {
		// Options may intentionally pass nil through configuration layers; fall
		// back to wall-clock time rather than leaving a nil function pointer.
		guard.now = time.Now
	}
	return guard, nil
}

// AutoMigrate creates or updates the ingestion_status table.
func (g *IngestionStatusGuard) AutoMigrate(ctx context.Context) error {
	if g == nil {
		return errors.New("ingestion status guard is required")
	}
	// Migration stays in the MySQL adapter so ClickHouse writers only depend on
	// the storage.EventWriteGuard interface, not on GORM.
	return g.db.WithContext(ctx).AutoMigrate(&IngestionStatus{})
}

// StartEventWrite claims the event id before the analytics event append starts.
func (g *IngestionStatusGuard) StartEventWrite(ctx context.Context, envelope contracts.EventEnvelope) (storage.EventWriteClaim, error) {
	if g == nil {
		return nil, errors.New("ingestion status guard is required")
	}
	if err := validateStatusEnvelope(envelope); err != nil {
		return nil, err
	}

	// First try to claim the event with an insert-only write. The composite
	// primary key lets MySQL decide whether another delivery already owns it.
	row := IngestionStatus{
		TenantID:   envelope.TenantID,
		ProjectID:  envelope.ProjectID,
		SourceID:   envelope.SourceID,
		EventID:    envelope.ID,
		Status:     ingestionStatusProcessing,
		Attempt:    1,
		ReceivedAt: envelope.ReceivedAt.UTC(),
	}
	result := g.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if result.Error != nil {
		return nil, result.Error
	}

	// The claim object is returned even for duplicate rows so BatchWriter can
	// make one uniform AlreadyInserted / Commit / Rollback decision.
	claim := &ingestionStatusClaim{
		db:  g.db,
		key: statusKeyFromEnvelope(envelope),
		now: g.now,
	}
	if result.RowsAffected > 0 {
		return claim, nil
	}

	// A conflicting insert means this event id was seen before. Read the row to
	// decide whether this is a committed duplicate or a retryable failed claim.
	existing, err := g.findStatus(ctx, envelope)
	if err != nil {
		return nil, err
	}
	if existing.Status == ingestionStatusInserted {
		claim.alreadyInserted = true
		return claim, nil
	}
	// Failed or stale processing rows are reclaimed by bumping attempt and
	// clearing last_error before ClickHouse is tried again.
	if err := g.reclaimStatus(ctx, envelope); err != nil {
		return nil, err
	}
	claim.alreadyInserted = false
	return claim, nil
}

func (g *IngestionStatusGuard) findStatus(ctx context.Context, envelope contracts.EventEnvelope) (IngestionStatus, error) {
	var row IngestionStatus
	// Use the same composite key as the insert path so Redis Stream and Kafka
	// retries converge on one status row regardless of queue-specific metadata.
	err := g.db.WithContext(ctx).
		Where(statusKeyFromEnvelope(envelope)).
		First(&row).
		Error
	if err != nil {
		return IngestionStatus{}, err
	}
	return row, nil
}

func (g *IngestionStatusGuard) reclaimStatus(ctx context.Context, envelope contracts.EventEnvelope) error {
	// Reclaiming does not delete the row; the attempt counter and timestamp are
	// useful for diagnostics, backoff policies, and future dead-letter tooling.
	return g.db.WithContext(ctx).
		Model(&IngestionStatus{}).
		Where(statusKeyFromEnvelope(envelope)).
		Updates(map[string]any{
			"status":     ingestionStatusProcessing,
			"attempt":    gorm.Expr("attempt + ?", 1),
			"last_error": "",
			"updated_at": g.now().UTC(),
		}).
		Error
}

type ingestionStatusClaim struct {
	db              *gorm.DB  // db updates the claimed status row
	key             statusKey // key identifies the claimed event row
	now             Clock     // now supplies deterministic update timestamps
	alreadyInserted bool      // alreadyInserted reports an existing committed duplicate
}

// AlreadyInserted reports whether the event was previously committed.
func (c *ingestionStatusClaim) AlreadyInserted() bool {
	// A nil claim should never be returned by the guard, but this keeps callers
	// defensive when tests construct claims directly.
	return c != nil && c.alreadyInserted
}

// Commit marks the claimed event as inserted.
func (c *ingestionStatusClaim) Commit(ctx context.Context) error {
	if c == nil {
		return errors.New("ingestion status claim is required")
	}
	if c.alreadyInserted {
		// Duplicate deliveries are already durable; committing again would only
		// churn UpdatedAt and hide the original insertion time.
		return nil
	}

	// Commit is the durable marker that lets later queue replays become no-op
	// successes instead of duplicate ClickHouse appends.
	return c.db.WithContext(ctx).
		Model(&IngestionStatus{}).
		Where(c.key).
		Updates(map[string]any{
			"status":     ingestionStatusInserted,
			"last_error": "",
			"updated_at": c.now().UTC(),
		}).
		Error
}

// Rollback records the failed append reason for the claimed event.
func (c *ingestionStatusClaim) Rollback(ctx context.Context, cause error) error {
	if c == nil {
		return errors.New("ingestion status claim is required")
	}
	if c.alreadyInserted {
		// A committed duplicate has nothing to roll back; the event row already
		// exists and should remain the source of truth.
		return nil
	}

	// Rollback preserves the status row as failed instead of deleting it so the
	// next delivery can reclaim the same key and retain diagnostics.
	return c.db.WithContext(ctx).
		Model(&IngestionStatus{}).
		Where(c.key).
		Updates(map[string]any{
			"status":     ingestionStatusFailed,
			"last_error": truncateStatusError(cause),
			"updated_at": c.now().UTC(),
		}).
		Error
}

type statusKey struct {
	TenantID  string `gorm:"column:tenant_id"`  // TenantID is the tenant boundary key
	ProjectID string `gorm:"column:project_id"` // ProjectID is the project boundary key
	SourceID  string `gorm:"column:source_id"`  // SourceID is the source boundary key
	EventID   string `gorm:"column:event_id"`   // EventID is the stable event id
}

func statusKeyFromEnvelope(envelope contracts.EventEnvelope) statusKey {
	// Queue offsets are deliberately excluded: Redis and Kafka deliveries of the
	// same event must share the same idempotency row.
	return statusKey{
		TenantID:  envelope.TenantID,
		ProjectID: envelope.ProjectID,
		SourceID:  envelope.SourceID,
		EventID:   envelope.ID,
	}
}

func validateStatusEnvelope(envelope contracts.EventEnvelope) error {
	// The guard only validates the idempotency key; collect.Normalize remains
	// responsible for full protocol validation before events reach ingestion.
	if strings.TrimSpace(envelope.TenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if strings.TrimSpace(envelope.ProjectID) == "" {
		return errors.New("project_id is required")
	}
	if strings.TrimSpace(envelope.SourceID) == "" {
		return errors.New("source_id is required")
	}
	if strings.TrimSpace(envelope.ID) == "" {
		return errors.New("event_id is required")
	}
	return nil
}

func truncateStatusError(cause error) string {
	if cause == nil {
		return ""
	}

	message := cause.Error()
	const maxStatusErrorLength = 2048
	if len(message) <= maxStatusErrorLength {
		return message
	}
	// Keep the status row bounded because LastError is diagnostics, not a log
	// sink for full stack traces or large ClickHouse exception payloads.
	return message[:maxStatusErrorLength]
}
