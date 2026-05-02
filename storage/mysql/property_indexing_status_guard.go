package mysql

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/storage"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	propertyIndexingStatusProcessing = "processing"
	propertyIndexingStatusInserted   = "inserted"
	propertyIndexingStatusFailed     = "failed"
)

// PropertyIndexingStatus records the idempotency state for one event's property rows.
//
// The table is separate from ingestion_status because property indexing can
// fail after the primary event row is already committed. Keeping a second
// checkpoint lets retries repair property rows without rewriting event rows.
type PropertyIndexingStatus struct {
	TenantID   string    `gorm:"column:tenant_id;primaryKey;size:128"`  // TenantID is the tenant boundary key
	ProjectID  string    `gorm:"column:project_id;primaryKey;size:128"` // ProjectID is the project or product boundary key
	SourceID   string    `gorm:"column:source_id;primaryKey;size:128"`  // SourceID is the source boundary key inside the project
	EventID    string    `gorm:"column:event_id;primaryKey;size:128"`   // EventID is the stable event id supplied by collect
	Status     string    `gorm:"column:status;size:32;not null"`        // Status is processing, inserted, or failed
	Attempt    int       `gorm:"column:attempt;not null;default:1"`     // Attempt counts property write claims for diagnostics
	LastError  string    `gorm:"column:last_error;type:text"`           // LastError stores the most recent property write failure
	ReceivedAt time.Time `gorm:"column:received_at;not null"`           // ReceivedAt is the collect acceptance timestamp for the event
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime"`      // CreatedAt is maintained by GORM when the row is created
	UpdatedAt  time.Time `gorm:"column:updated_at;autoUpdateTime"`      // UpdatedAt is maintained by GORM when the row changes
}

// TableName returns the stable property indexing status table name.
func (PropertyIndexingStatus) TableName() string {
	return "property_indexing_status"
}

// PropertyIndexingStatusGuard implements storage.PropertyWriteGuard with GORM.
//
// The guard does not make ClickHouse property batches transactional with MySQL.
// It records whether property rows were already indexed so duplicate deliveries
// can either skip completed work or retry a previously failed property batch.
type PropertyIndexingStatusGuard struct {
	db *gorm.DB // db executes status-row inserts and updates
}

// NewPropertyIndexingStatusGuard creates a GORM-backed PropertyWriteGuard.
func NewPropertyIndexingStatusGuard(db *gorm.DB) (*PropertyIndexingStatusGuard, error) {
	if db == nil {
		return nil, errors.New("gorm db is required")
	}

	// The guard performs one-row checkpoint updates; disabling GORM's default
	// transaction avoids wrapping each small status mutation in extra SQL.
	return &PropertyIndexingStatusGuard{db: db.Session(&gorm.Session{SkipDefaultTransaction: true})}, nil
}

// AutoMigrate creates or updates the property_indexing_status table.
func (g *PropertyIndexingStatusGuard) AutoMigrate(ctx context.Context) error {
	if g == nil {
		return errors.New("property indexing status guard is required")
	}
	// Migration stays in the MySQL adapter so storage decorators only depend on
	// the storage.PropertyWriteGuard interface, not on GORM.
	return g.db.WithContext(ctx).AutoMigrate(&PropertyIndexingStatus{})
}

// StartPropertyWrite claims the event id before property indexing starts.
func (g *PropertyIndexingStatusGuard) StartPropertyWrite(ctx context.Context, envelope contracts.EventEnvelope) (storage.PropertyWriteClaim, error) {
	if g == nil {
		return nil, errors.New("property indexing status guard is required")
	}
	if err := validateStatusEnvelope(envelope); err != nil {
		return nil, err
	}

	// Insert a property checkpoint after the primary event row is known to
	// exist. The composite key lets duplicate deliveries converge on one status.
	row := PropertyIndexingStatus{
		TenantID:   envelope.TenantID,
		ProjectID:  envelope.ProjectID,
		SourceID:   envelope.SourceID,
		EventID:    envelope.ID,
		Status:     propertyIndexingStatusProcessing,
		Attempt:    1,
		ReceivedAt: envelope.ReceivedAt.UTC(),
	}
	result := g.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if result.Error != nil {
		return nil, result.Error
	}

	claim := &propertyIndexingStatusClaim{
		db:  g.db,
		key: statusKeyFromEnvelope(envelope),
	}
	if result.RowsAffected > 0 {
		return claim, nil
	}

	// A conflicting insert means property indexing was seen before. Inserted
	// rows become duplicate no-ops; only explicit failed rows can be reclaimed.
	// Processing is ambiguous because ClickHouse may have accepted the batch
	// before a later MySQL commit failure.
	existing, err := g.findStatus(ctx, envelope)
	if err != nil {
		return nil, err
	}
	if existing.Status == propertyIndexingStatusInserted {
		claim.alreadyInserted = true
		return claim, nil
	}
	if existing.Status != propertyIndexingStatusFailed {
		return nil, fmt.Errorf("property indexing status %q is not reclaimable", existing.Status)
	}
	if err := g.reclaimFailedStatus(ctx, envelope); err != nil {
		return nil, err
	}
	claim.alreadyInserted = false
	return claim, nil
}

func (g *PropertyIndexingStatusGuard) findStatus(ctx context.Context, envelope contracts.EventEnvelope) (PropertyIndexingStatus, error) {
	var row PropertyIndexingStatus
	// Use the same composite event key as the primary ingestion guard so event
	// and property checkpoints can be correlated during diagnostics.
	err := g.db.WithContext(ctx).
		Where(statusKeyFromEnvelope(envelope)).
		First(&row).
		Error
	if err != nil {
		return PropertyIndexingStatus{}, err
	}
	return row, nil
}

func (g *PropertyIndexingStatusGuard) reclaimFailedStatus(ctx context.Context, envelope contracts.EventEnvelope) error {
	// Reclaim only explicit failed rows. Processing rows are ambiguous because a
	// previous ClickHouse batch may have succeeded while the MySQL commit failed;
	// retrying them automatically could append duplicate property rows.
	result := g.db.WithContext(ctx).
		Model(&PropertyIndexingStatus{}).
		Where(statusKeyFromEnvelope(envelope)).
		Where("status = ?", propertyIndexingStatusFailed).
		Updates(map[string]any{
			"status":     propertyIndexingStatusProcessing,
			"attempt":    gorm.Expr("attempt + ?", 1),
			"last_error": "",
			"updated_at": time.Now().UTC(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("property indexing status was not reclaimed")
	}
	return nil
}

type propertyIndexingStatusClaim struct {
	db              *gorm.DB  // db updates the claimed property status row
	key             statusKey // key identifies the claimed event row
	alreadyInserted bool      // alreadyInserted reports an existing committed property index
}

// AlreadyInserted reports whether the property rows were previously committed.
func (c *propertyIndexingStatusClaim) AlreadyInserted() bool {
	// A nil claim should never be returned by the guard, but this keeps callers
	// defensive when tests construct claims directly.
	return c != nil && c.alreadyInserted
}

// Commit marks the claimed property rows as inserted.
func (c *propertyIndexingStatusClaim) Commit(ctx context.Context) error {
	if c == nil {
		return errors.New("property indexing status claim is required")
	}
	if c.alreadyInserted {
		// Duplicate deliveries are already durable; committing again would only
		// churn UpdatedAt and hide the original indexing time.
		return nil
	}

	// Commit is the durable marker that lets later duplicate deliveries skip the
	// property batch instead of appending duplicate property rows.
	return c.db.WithContext(ctx).
		Model(&PropertyIndexingStatus{}).
		Where(c.key).
		Updates(map[string]any{
			"status":     propertyIndexingStatusInserted,
			"last_error": "",
			"updated_at": time.Now().UTC(),
		}).
		Error
}

// Rollback records the failed property write reason for the claimed event.
func (c *propertyIndexingStatusClaim) Rollback(ctx context.Context, cause error) error {
	if c == nil {
		return errors.New("property indexing status claim is required")
	}
	if c.alreadyInserted {
		// A committed property index has nothing to roll back; duplicate retries
		// should leave the original property rows as the source of truth.
		return nil
	}

	// Rollback preserves the row as failed so the next delivery can repair the
	// property index without writing another event row.
	return c.db.WithContext(ctx).
		Model(&PropertyIndexingStatus{}).
		Where(c.key).
		Updates(map[string]any{
			"status":     propertyIndexingStatusFailed,
			"last_error": truncateStatusError(cause),
			"updated_at": time.Now().UTC(),
		}).
		Error
}
