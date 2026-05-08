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
	db *gorm.DB // db executes property catalog reads and upserts
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

// ListPropertyCatalogEntries returns observed selectors for one source boundary.
func (c *PropertyCatalog) ListPropertyCatalogEntries(ctx context.Context, query storage.PropertyCatalogQuery) ([]storage.PropertyCatalogEntry, error) {
	if c == nil {
		return nil, errors.New("property catalog is required")
	}
	if err := storage.ValidatePropertyCatalogQuery(query); err != nil {
		return nil, err
	}

	// Keep catalog reads source-scoped and deterministic. This route will back UI
	// filter suggestions, so it should never merge selectors across tenants,
	// projects, or sources.
	db := c.db.WithContext(ctx).
		Where("tenant_id = ? AND project_id = ? AND source_id = ?", query.TenantID, query.ProjectID, query.SourceID)
	if query.Scope != "" {
		db = db.Where("property_scope = ?", string(query.Scope))
	}
	if query.Limit > 0 {
		db = db.Limit(query.Limit)
	}
	db = db.Order("property_scope ASC").Order("property_name ASC").Order("property_type ASC")

	var rows []PropertyCatalogEntry
	if err := db.Find(&rows).Error; err != nil {
		return nil, err
	}

	// Convert rows back into the storage-neutral contract so callers do not
	// depend on the MySQL row shape or GORM tags.
	entries := make([]storage.PropertyCatalogEntry, 0, len(rows))
	for _, row := range rows {
		entry, err := propertyCatalogEntryFromRow(row)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// propertyCatalogRowFromEntry maps the storage-neutral catalog entry into the MySQL row model.
func propertyCatalogRowFromEntry(entry storage.PropertyCatalogEntry) (PropertyCatalogEntry, error) {
	if err := storage.ValidatePropertyCatalogEntry(entry); err != nil {
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

// propertyCatalogEntryFromRow maps one MySQL row into the storage-neutral catalog entry.
func propertyCatalogEntryFromRow(row PropertyCatalogEntry) (storage.PropertyCatalogEntry, error) {
	entry := storage.PropertyCatalogEntry{
		TenantID:    row.TenantID,
		ProjectID:   row.ProjectID,
		SourceID:    row.SourceID,
		Scope:       storage.PropertyScope(row.Scope),
		Name:        row.Name,
		ValueType:   storage.PropertyValueType(row.ValueType),
		FirstSeenAt: row.FirstSeenAt.UTC(),
		LastSeenAt:  row.LastSeenAt.UTC(),
	}
	if err := storage.ValidatePropertyCatalogEntry(entry); err != nil {
		return storage.PropertyCatalogEntry{}, err
	}
	return entry, nil
}
