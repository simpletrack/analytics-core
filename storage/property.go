package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
)

// PropertyScope identifies where a flattened property originated.
type PropertyScope string

const (
	// PropertyScopeEvent marks properties attached to one analytics event.
	PropertyScopeEvent PropertyScope = "event"
	// PropertyScopeUser marks user properties attached to the event identity.
	PropertyScopeUser PropertyScope = "user"
)

// PropertyValueType identifies the scalar type stored for one property.
type PropertyValueType string

const (
	// PropertyValueNull stores a deliberately present null value.
	PropertyValueNull PropertyValueType = "null"
	// PropertyValueString stores a string property value.
	PropertyValueString PropertyValueType = "string"
	// PropertyValueNumber stores an integer or floating-point property value.
	PropertyValueNumber PropertyValueType = "number"
	// PropertyValueBool stores a boolean property value.
	PropertyValueBool PropertyValueType = "bool"
)

// EventPropertyRecord is the typed-row view of one event or user property.
//
// The record is storage-neutral: ClickHouse may persist it as a dedicated
// property table, a materialized view, or a projection later. P1 keeps this
// shape as the single expansion contract so writers and query builders do not
// reinterpret raw JSON maps independently.
type EventPropertyRecord struct {
	EventID     string            // EventID is the stable event id that produced the property
	TenantID    string            // TenantID is the tenant boundary key
	ProjectID   string            // ProjectID is the project or website boundary key
	SourceID    string            // SourceID is the source boundary key inside the project
	SourceType  string            // SourceType is the source category such as web, server, or mobile
	EventName   string            // EventName is the analytics event name
	DistinctID  string            // DistinctID is the visitor or user identity key
	SessionID   string            // SessionID is the optional session key
	VisitID     string            // VisitID is the canonical analytics visit key
	EventTime   time.Time         // EventTime is the timestamp produced by the source
	ReceivedAt  time.Time         // ReceivedAt is the timestamp accepted by collect
	Source      string            // Source is the optional source label for diagnostics
	Scope       PropertyScope     // Scope separates event properties from user properties
	Name        string            // Name is the normalized property key
	ValueType   PropertyValueType // ValueType selects which typed value field is meaningful
	StringValue string            // StringValue stores string property values
	NumberValue float64           // NumberValue stores numeric property values
	BoolValue   bool              // BoolValue stores boolean property values
}

// PropertyWriteResult reports how many property rows were appended.
type PropertyWriteResult struct {
	Rows int // Rows is the number of property records accepted by the storage adapter
}

// EventPropertyWriter writes typed event property rows to the analytics storage backend.
//
// Implementations own the physical property table, batching protocol, and
// adapter-specific retry behavior. Event ingestion should pass records produced
// by FlattenEventProperties instead of reparsing EventEnvelope maps.
type EventPropertyWriter interface {
	// WriteEventProperties persists typed event property rows.
	WriteEventProperties(context.Context, []EventPropertyRecord) (PropertyWriteResult, error)
}

// PropertyWriteGuard starts and finalizes the durable idempotency record for property indexing.
//
// Property indexing needs its own checkpoint because the event row may already
// be committed even when the property batch fails. The guard lets retries repair
// that partial outcome without rewriting already indexed properties.
type PropertyWriteGuard interface {
	// StartPropertyWrite claims the event id before the property batch starts.
	StartPropertyWrite(context.Context, contracts.EventEnvelope) (PropertyWriteClaim, error)
}

// PropertyWriteClaim is the per-event idempotency claim returned by PropertyWriteGuard.
//
// A claim should be keyed by tenant_id, project_id, source_id, and event_id so
// property retries converge on the same checkpoint as the event writer.
type PropertyWriteClaim interface {
	// AlreadyInserted reports whether the property rows were previously committed.
	AlreadyInserted() bool
	// Commit marks the claimed property index as durably inserted after the batch succeeds.
	Commit(context.Context) error
	// Rollback records the failed property batch so the next retry can reclaim it.
	Rollback(context.Context, error) error
}

// FlattenEventProperties converts event and user property maps into typed rows.
//
// Flattening is deterministic: event properties are emitted before user
// properties, and keys are sorted inside each scope. Unsupported nested values
// return an error because collect is expected to reject them before ingestion.
func FlattenEventProperties(envelope contracts.EventEnvelope) ([]EventPropertyRecord, error) {
	base := propertyBase{
		EventID:    envelope.ID,
		TenantID:   envelope.TenantID,
		ProjectID:  envelope.ProjectID,
		SourceID:   envelope.SourceID,
		SourceType: envelope.SourceType,
		EventName:  envelope.EventName,
		DistinctID: envelope.DistinctID,
		SessionID:  envelope.SessionID,
		VisitID:    envelope.VisitID,
		EventTime:  envelope.EventTime.UTC(),
		ReceivedAt: envelope.ReceivedAt.UTC(),
		Source:     envelope.Source,
	}

	// Build event and user scopes through the same scalar conversion path so
	// later property dictionaries can apply one type system to both sources.
	records, err := flattenPropertyMap(base, PropertyScopeEvent, envelope.Properties)
	if err != nil {
		return nil, err
	}
	userRecords, err := flattenPropertyMap(base, PropertyScopeUser, envelope.UserProps)
	if err != nil {
		return nil, err
	}
	records = append(records, userRecords...)
	return records, nil
}

type propertyBase struct {
	EventID    string    // EventID is copied to every flattened property row
	TenantID   string    // TenantID is copied to every flattened property row
	ProjectID  string    // ProjectID is copied to every flattened property row
	SourceID   string    // SourceID is copied to every flattened property row
	SourceType string    // SourceType is copied to every flattened property row
	EventName  string    // EventName is copied to every flattened property row
	DistinctID string    // DistinctID is copied to every flattened property row
	SessionID  string    // SessionID is copied to every flattened property row
	VisitID    string    // VisitID is copied to every flattened property row
	EventTime  time.Time // EventTime is copied to every flattened property row
	ReceivedAt time.Time // ReceivedAt is copied to every flattened property row
	Source     string    // Source is copied to every flattened property row
}

func flattenPropertyMap(base propertyBase, scope PropertyScope, values map[string]any) ([]EventPropertyRecord, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Sorting keys makes tests, backfills, and future metadata upserts
	// repeatable even though Go map iteration order is intentionally random.
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	records := make([]EventPropertyRecord, 0, len(keys))
	for _, key := range keys {
		record, err := newPropertyRecord(base, scope, key, values[key])
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func newPropertyRecord(base propertyBase, scope PropertyScope, name string, value any) (EventPropertyRecord, error) {
	record := EventPropertyRecord{
		EventID:    base.EventID,
		TenantID:   base.TenantID,
		ProjectID:  base.ProjectID,
		SourceID:   base.SourceID,
		SourceType: base.SourceType,
		EventName:  base.EventName,
		DistinctID: base.DistinctID,
		SessionID:  base.SessionID,
		VisitID:    base.VisitID,
		EventTime:  base.EventTime,
		ReceivedAt: base.ReceivedAt,
		Source:     base.Source,
		Scope:      scope,
		Name:       name,
	}

	// Store each scalar in a predictable typed slot. String mirrors let query
	// builders add dictionary-backed filters later without reparsing raw JSON.
	switch typed := value.(type) {
	case nil:
		record.ValueType = PropertyValueNull
	case bool:
		record.ValueType = PropertyValueBool
		record.BoolValue = typed
	case string:
		record.ValueType = PropertyValueString
		record.StringValue = typed
	case int:
		record = setNumberProperty(record, float64(typed))
	case int8:
		record = setNumberProperty(record, float64(typed))
	case int16:
		record = setNumberProperty(record, float64(typed))
	case int32:
		record = setNumberProperty(record, float64(typed))
	case int64:
		record = setNumberProperty(record, float64(typed))
	case uint:
		record = setNumberProperty(record, float64(typed))
	case uint8:
		record = setNumberProperty(record, float64(typed))
	case uint16:
		record = setNumberProperty(record, float64(typed))
	case uint32:
		record = setNumberProperty(record, float64(typed))
	case uint64:
		record = setNumberProperty(record, float64(typed))
	case float32:
		record = setNumberProperty(record, float64(typed))
	case float64:
		record = setNumberProperty(record, typed)
	case json.Number:
		number, err := strconv.ParseFloat(typed.String(), 64)
		if err != nil {
			return EventPropertyRecord{}, fmt.Errorf("property %s.%s has invalid number: %w", scope, name, err)
		}
		record = setNumberProperty(record, number)
	default:
		return EventPropertyRecord{}, fmt.Errorf("property %s.%s has unsupported value type %T", scope, name, value)
	}
	if record.ValueType == PropertyValueNumber && (math.IsNaN(record.NumberValue) || math.IsInf(record.NumberValue, 0)) {
		return EventPropertyRecord{}, fmt.Errorf("property %s.%s has invalid number", scope, name)
	}
	if record.ValueType == "" {
		return EventPropertyRecord{}, errors.New("property value type is required")
	}
	return record, nil
}

func setNumberProperty(record EventPropertyRecord, value float64) EventPropertyRecord {
	record.ValueType = PropertyValueNumber
	record.NumberValue = value
	return record
}
