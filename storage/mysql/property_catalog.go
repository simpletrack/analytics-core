package mysql

import (
	"context"
	"errors"
	"time"

	"github.com/simpletrack/analytics-core/storage"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// PropertyCatalogEntry stores one observed property selector and type in MySQL.
//
// The row is keyed by tenant/project/source/scope/name/type so event and user
// properties can later become source-scoped filter suggestions without scanning
// ClickHouse property rows.
type PropertyCatalogEntry struct {
	TenantID    string    `gorm:"column:tenant_id;primaryKey;size:128"`     // TenantID is the tenant boundary key
	ProjectID   string    `gorm:"column:project_id;primaryKey;size:128"`    // ProjectID is the project or product boundary key
	SourceID    string    `gorm:"column:source_id;primaryKey;size:128"`     // SourceID is the source boundary key inside the project
	Scope       string    `gorm:"column:property_scope;primaryKey;size:32"` // Scope is event or user
	Name        string    `gorm:"column:property_name;primaryKey;size:128"` // Name is the normalized property key
	ValueType   string    `gorm:"column:property_type;primaryKey;size:32"`  // ValueType is null, string, number, or bool
	FirstSeenAt time.Time `gorm:"column:first_seen_at;not null"`            // FirstSeenAt is the earliest observed event time
	LastSeenAt  time.Time `gorm:"column:last_seen_at;not null"`             // LastSeenAt is the latest observed event time
	CreatedAt   time.Time `gorm:"column:created_at;autoCreateTime"`         // CreatedAt is maintained by GORM when the row is created
	UpdatedAt   time.Time `gorm:"column:updated_at;autoUpdateTime"`         // UpdatedAt is maintained by GORM when the row changes
}

// TableName returns the stable property catalog table name.
func (PropertyCatalogEntry) TableName() string {
	return "property_catalog"
}

// PropertyCatalog implements storage.PropertyCatalog with GORM/MySQL.
type PropertyCatalog struct {
	db *gorm.DB // db executes property catalog upserts
}

// NewPropertyCatalog creates a GORM-backed property catalog.
func NewPropertyCatalog(db *gorm.DB) (*PropertyCatalog, error) {
	if db == nil {
		return nil, errors.New("gorm db is required")
	}

	// Catalog upserts are single-table metadata writes. Disable default
	// transactions to match the lightweight status-guard adapters.
	return &PropertyCatalog{db: db.Session(&gorm.Session{SkipDefaultTransaction: true})}, nil
}

// AutoMigrate creates or updates the property_catalog table.
func (c *PropertyCatalog) AutoMigrate(ctx context.Context) error {
	if c == nil {
		return errors.New("property catalog is required")
	}
	// This is an initialization path for the new project stage, not a historical
	// migration framework.
	return c.db.WithContext(ctx).AutoMigrate(&PropertyCatalogEntry{})
}

// UpsertPropertyCatalogEntries records observed property selectors and value types.
func (c *PropertyCatalog) UpsertPropertyCatalogEntries(ctx context.Context, entries []storage.PropertyCatalogEntry) (storage.PropertyCatalogResult, error) {
	if c == nil {
		return storage.PropertyCatalogResult{}, errors.New("property catalog is required")
	}
	if len(entries) == 0 {
		return storage.PropertyCatalogResult{}, nil
	}

	// Convert and validate before sending SQL so a single malformed entry cannot
	// produce a partial metadata batch.
	rows := make([]PropertyCatalogEntry, 0, len(entries))
	for _, entry := range entries {
		row, err := propertyCatalogRowFromEntry(entry)
		if err != nil {
			return storage.PropertyCatalogResult{}, err
		}
		rows = append(rows, row)
	}

	// Upsert keeps first_seen_at monotonic toward the past and last_seen_at
	// monotonic toward the future. Counts and latest event names are omitted
	// because at-least-once queue retries can replay the same observation.
	result := c.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tenant_id"},
				{Name: "project_id"},
				{Name: "source_id"},
				{Name: "property_scope"},
				{Name: "property_name"},
				{Name: "property_type"},
			},
			DoUpdates: clause.Assignments(map[string]any{
				"first_seen_at": gorm.Expr("LEAST(first_seen_at, VALUES(first_seen_at))"),
				"last_seen_at":  gorm.Expr("GREATEST(last_seen_at, VALUES(last_seen_at))"),
				"updated_at":    gorm.Expr("VALUES(updated_at)"),
			}),
		}).
		Create(&rows)
	if result.Error != nil {
		return storage.PropertyCatalogResult{}, result.Error
	}
	return storage.PropertyCatalogResult{Entries: len(rows)}, nil
}

// propertyCatalogRowFromEntry maps the storage-neutral catalog entry into the MySQL row model.
func propertyCatalogRowFromEntry(entry storage.PropertyCatalogEntry) (PropertyCatalogEntry, error) {
	if err := validatePropertyCatalogEntry(entry); err != nil {
		return PropertyCatalogEntry{}, err
	}
	return PropertyCatalogEntry{
		TenantID:    entry.TenantID,
		ProjectID:   entry.ProjectID,
		SourceID:    entry.SourceID,
		Scope:       string(entry.Scope),
		Name:        entry.Name,
		ValueType:   string(entry.ValueType),
		FirstSeenAt: entry.FirstSeenAt.UTC(),
		LastSeenAt:  entry.LastSeenAt.UTC(),
	}, nil
}

// validatePropertyCatalogEntry rejects incomplete catalog metadata before SQL execution.
func validatePropertyCatalogEntry(entry storage.PropertyCatalogEntry) error {
	// Keep this validation at the MySQL adapter boundary too. Callers may build
	// entries without using storage.BuildPropertyCatalogEntries.
	if entry.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if entry.ProjectID == "" {
		return errors.New("project_id is required")
	}
	if entry.SourceID == "" {
		return errors.New("source_id is required")
	}
	if entry.Scope == "" {
		return errors.New("property scope is required")
	}
	if entry.Name == "" {
		return errors.New("property name is required")
	}
	if entry.ValueType == "" {
		return errors.New("property value type is required")
	}
	if entry.FirstSeenAt.IsZero() {
		return errors.New("first_seen_at is required")
	}
	if entry.LastSeenAt.IsZero() {
		return errors.New("last_seen_at is required")
	}
	return nil
}
