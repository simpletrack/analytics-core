package storage

import (
	"context"
	"errors"
	"sort"
	"time"
)

// PropertyCatalogEntry is one observed property definition for metadata governance.
//
// The catalog entry is intentionally storage-neutral. It captures the stable
// source boundary, property selector, value type, and observation window without
// depending on ClickHouse property tables.
type PropertyCatalogEntry struct {
	TenantID    string            // TenantID is the tenant boundary key
	ProjectID   string            // ProjectID is the project or website boundary key
	SourceID    string            // SourceID is the source boundary key inside the project
	Scope       PropertyScope     // Scope separates event properties from user properties
	Name        string            // Name is the normalized property key
	ValueType   PropertyValueType // ValueType is the observed scalar type for this property key
	FirstSeenAt time.Time         // FirstSeenAt is the earliest event time observed in this batch
	LastSeenAt  time.Time         // LastSeenAt is the latest event time observed in this batch
}

// PropertyCatalogResult reports how many property definitions were upserted.
type PropertyCatalogResult struct {
	Entries int // Entries is the number of unique dictionary entries accepted
}

// PropertyCatalogQuery selects one source-scoped property catalog slice.
type PropertyCatalogQuery struct {
	TenantID  string        // TenantID is the tenant boundary key
	ProjectID string        // ProjectID is the project or website boundary key
	SourceID  string        // SourceID is the source boundary key inside the project
	Scope     PropertyScope // Scope optionally narrows the catalog to event or user properties
	Limit     int           // Limit optionally caps returned rows; zero means no explicit cap
}

// PropertyCatalogWriter stores observed property definitions for governance and UI allowlists.
type PropertyCatalogWriter interface {
	// UpsertPropertyCatalogEntries records observed property selectors and types.
	UpsertPropertyCatalogEntries(context.Context, []PropertyCatalogEntry) (PropertyCatalogResult, error)
}

// PropertyCatalogReader reads observed property definitions for source-scoped UI allowlists.
type PropertyCatalogReader interface {
	// ListPropertyCatalogEntries returns observed selectors for one tenant/project/source boundary.
	ListPropertyCatalogEntries(context.Context, PropertyCatalogQuery) ([]PropertyCatalogEntry, error)
}

// PropertyCatalog combines property catalog reads and writes.
type PropertyCatalog interface {
	PropertyCatalogWriter
	PropertyCatalogReader
}

// BuildPropertyCatalogEntries condenses typed property rows into dictionary entries.
func BuildPropertyCatalogEntries(records []EventPropertyRecord) ([]PropertyCatalogEntry, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Deduplicate by the governance key instead of event_id. One event can
	// produce multiple properties, while the catalog only needs one row per
	// tenant/project/source/scope/name/type selector.
	entriesByKey := make(map[propertyCatalogKey]PropertyCatalogEntry)
	for _, record := range records {
		entry, err := propertyCatalogEntryFromRecord(record)
		if err != nil {
			return nil, err
		}
		key := propertyCatalogKeyFromEntry(entry)
		current, exists := entriesByKey[key]
		if !exists {
			entriesByKey[key] = entry
			continue
		}

		// Merge repeated observations in one batch without losing the earliest
		// and latest timestamps needed by metadata UIs and pruning jobs. Counts
		// are intentionally omitted because queue retries can replay the same
		// logical observation.
		if entry.FirstSeenAt.Before(current.FirstSeenAt) {
			current.FirstSeenAt = entry.FirstSeenAt
		}
		if entry.LastSeenAt.After(current.LastSeenAt) {
			current.LastSeenAt = entry.LastSeenAt
		}
		entriesByKey[key] = current
	}

	// Sort output so tests, backfills, and future catalog repair jobs are
	// deterministic even when the input records arrived from a Go map.
	keys := make([]propertyCatalogKey, 0, len(entriesByKey))
	for key := range entriesByKey {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left int, right int) bool {
		return keys[left].Less(keys[right])
	})

	entries := make([]PropertyCatalogEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, entriesByKey[key])
	}
	return entries, nil
}

type propertyCatalogKey struct {
	TenantID  string            // TenantID is the tenant boundary key
	ProjectID string            // ProjectID is the project or website boundary key
	SourceID  string            // SourceID is the source boundary key inside the project
	Scope     PropertyScope     // Scope separates event properties from user properties
	Name      string            // Name is the normalized property key
	ValueType PropertyValueType // ValueType is the observed scalar type
}

// Less reports whether the key should sort before another key.
func (k propertyCatalogKey) Less(other propertyCatalogKey) bool {
	if k.TenantID != other.TenantID {
		return k.TenantID < other.TenantID
	}
	if k.ProjectID != other.ProjectID {
		return k.ProjectID < other.ProjectID
	}
	if k.SourceID != other.SourceID {
		return k.SourceID < other.SourceID
	}
	if k.Scope != other.Scope {
		return k.Scope < other.Scope
	}
	if k.Name != other.Name {
		return k.Name < other.Name
	}
	return k.ValueType < other.ValueType
}

// propertyCatalogEntryFromRecord converts one typed property row into one catalog observation.
func propertyCatalogEntryFromRecord(record EventPropertyRecord) (PropertyCatalogEntry, error) {
	if err := validatePropertyCatalogRecord(record); err != nil {
		return PropertyCatalogEntry{}, err
	}
	observedAt := propertyCatalogObservedAt(record)
	return PropertyCatalogEntry{
		TenantID:    record.TenantID,
		ProjectID:   record.ProjectID,
		SourceID:    record.SourceID,
		Scope:       record.Scope,
		Name:        record.Name,
		ValueType:   record.ValueType,
		FirstSeenAt: observedAt,
		LastSeenAt:  observedAt,
	}, nil
}

// validatePropertyCatalogRecord rejects incomplete property rows before catalog upsert.
func validatePropertyCatalogRecord(record EventPropertyRecord) error {
	// The catalog key must be as strict as the property table key; allowing a
	// partial key would make later source-scoped allowlists ambiguous.
	if record.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if record.ProjectID == "" {
		return errors.New("project_id is required")
	}
	if record.SourceID == "" {
		return errors.New("source_id is required")
	}
	if record.Scope == "" {
		return errors.New("property scope is required")
	}
	if record.Name == "" {
		return errors.New("property name is required")
	}
	if record.ValueType == "" {
		return errors.New("property value type is required")
	}
	if propertyCatalogObservedAt(record).IsZero() {
		return errors.New("property observed time is required")
	}
	return nil
}

// ValidatePropertyCatalogEntry rejects incomplete or unsupported catalog metadata.
func ValidatePropertyCatalogEntry(entry PropertyCatalogEntry) error {
	// Keep catalog entries strict before they become UI filter suggestions.
	// Returning unsupported enum values would widen the public readback contract
	// beyond event/user scopes and null/string/number/bool value types.
	if entry.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if entry.ProjectID == "" {
		return errors.New("project_id is required")
	}
	if entry.SourceID == "" {
		return errors.New("source_id is required")
	}
	if err := validatePropertyScope(entry.Scope); err != nil {
		return err
	}
	if entry.Name == "" {
		return errors.New("property name is required")
	}
	if err := validatePropertyValueType(entry.ValueType); err != nil {
		return err
	}
	if entry.FirstSeenAt.IsZero() {
		return errors.New("first_seen_at is required")
	}
	if entry.LastSeenAt.IsZero() {
		return errors.New("last_seen_at is required")
	}
	return nil
}

// propertyCatalogObservedAt chooses the analytics timestamp used by the catalog.
func propertyCatalogObservedAt(record EventPropertyRecord) time.Time {
	// EventTime is the analytics time users reason about. ReceivedAt is only a
	// fallback for server-side events that omit a source timestamp.
	if !record.EventTime.IsZero() {
		return record.EventTime.UTC()
	}
	if !record.ReceivedAt.IsZero() {
		return record.ReceivedAt.UTC()
	}
	return time.Time{}
}

// validatePropertyScope rejects scopes outside the stable catalog/query contract.
func validatePropertyScope(scope PropertyScope) error {
	switch scope {
	case "":
		return errors.New("property scope is required")
	case PropertyScopeEvent, PropertyScopeUser:
		return nil
	default:
		return errors.New("property scope must be event or user")
	}
}

// validatePropertyValueType rejects value types outside the scalar property contract.
func validatePropertyValueType(valueType PropertyValueType) error {
	switch valueType {
	case "":
		return errors.New("property value type is required")
	case PropertyValueNull, PropertyValueString, PropertyValueNumber, PropertyValueBool:
		return nil
	default:
		return errors.New("property value type must be null, string, number, or bool")
	}
}

// propertyCatalogKeyFromEntry returns the dictionary key for one catalog entry.
func propertyCatalogKeyFromEntry(entry PropertyCatalogEntry) propertyCatalogKey {
	return propertyCatalogKey{
		TenantID:  entry.TenantID,
		ProjectID: entry.ProjectID,
		SourceID:  entry.SourceID,
		Scope:     entry.Scope,
		Name:      entry.Name,
		ValueType: entry.ValueType,
	}
}

// ValidatePropertyCatalogQuery rejects incomplete source-scoped catalog reads.
func ValidatePropertyCatalogQuery(query PropertyCatalogQuery) error {
	// Reads must stay source-scoped so governance and future filter builders do
	// not accidentally merge unrelated tenant or project metadata.
	if query.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if query.ProjectID == "" {
		return errors.New("project_id is required")
	}
	if query.SourceID == "" {
		return errors.New("source_id is required")
	}
	if query.Scope != "" {
		if err := validatePropertyScope(query.Scope); err != nil {
			return err
		}
	}
	if query.Limit < 0 {
		return errors.New("limit must be greater than or equal to 0")
	}
	return nil
}
