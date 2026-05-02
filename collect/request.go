package collect

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
)

const (
	maxClockSkew       = 5 * time.Minute
	maxEventNameLength = 128
	maxIDLength        = 128
	maxPropertyCount   = 64
	maxPropertyKeyLen  = 128
	maxPropertyStrLen  = 2048
	maxSourceTypeLen   = 32
)

var (
	eventNamePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]*$`)
	identifierPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]*$`)
	propertyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]*$`)
	sourceTypePattern  = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
)

// Request is the normalized collect input before validation.
//
// Request carries the JSON field names of the public collect protocol, but it
// stays independent from HTTP framework types so collect.Handler can be reused
// outside fasthttp.
type Request struct {
	ID         string         `json:"id"`                        // ID is the stable event id used for idempotent ingestion.
	TenantID   string         `json:"tenant_id"`                 // TenantID is the tenant boundary key.
	ProjectID  string         `json:"project_id"`                // ProjectID is the project or website boundary key.
	SourceID   string         `json:"source_id"`                 // SourceID is the source boundary key inside the project.
	SourceType string         `json:"source_type"`               // SourceType is the source category such as web, server, or mobile.
	EventName  string         `json:"event_name"`                // EventName is the analytics event name.
	DistinctID string         `json:"distinct_id"`               // DistinctID is the visitor or user identity key.
	SessionID  string         `json:"session_id,omitempty"`      // SessionID is the optional session key.
	EventTime  time.Time      `json:"event_time,omitempty"`      // EventTime is the timestamp produced by the source.
	Properties map[string]any `json:"properties,omitempty"`      // Properties are event-scoped properties.
	UserProps  map[string]any `json:"user_properties,omitempty"` // UserProps are user-scoped properties.
	Source     string         `json:"source,omitempty"`          // Source is an optional diagnostic source label.
	Client     ClientInfo     `json:"-"`                         // Client carries transient transport metadata for collect stages.
}

// ValidationError describes a rejected collect field.
type ValidationError struct {
	Field  string // Field is the rejected field name.
	Reason string // Reason is the validation failure summary.
}

// Error returns a stable human-readable validation error.
func (e ValidationError) Error() string {
	if e.Field == "" {
		return e.Reason
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Reason)
}

// Normalize validates request and returns a stable event envelope.
//
// Normalize is the framework-neutral boundary between collect transports and
// the queue/storage pipeline. It must not accept HTTP context types because the
// same event protocol can arrive through HTTP, workers, tests, or future SDK
// adapters.
func Normalize(request Request, receivedAt time.Time) (contracts.EventEnvelope, error) {
	if receivedAt.IsZero() {
		return contracts.EventEnvelope{}, errors.New("receivedAt is required")
	}

	request = trimRequest(request)
	if err := validateIdentifier("id", request.ID); err != nil {
		return contracts.EventEnvelope{}, err
	}
	if err := validateIdentifier("tenant_id", request.TenantID); err != nil {
		return contracts.EventEnvelope{}, err
	}
	if err := validateIdentifier("project_id", request.ProjectID); err != nil {
		return contracts.EventEnvelope{}, err
	}
	if err := validateIdentifier("source_id", request.SourceID); err != nil {
		return contracts.EventEnvelope{}, err
	}
	if err := validateSourceType(request.SourceType); err != nil {
		return contracts.EventEnvelope{}, err
	}
	if err := validateEventName(request.EventName); err != nil {
		return contracts.EventEnvelope{}, err
	}
	if err := validateIdentifier("distinct_id", request.DistinctID); err != nil {
		return contracts.EventEnvelope{}, err
	}
	if request.SessionID != "" {
		if err := validateIdentifier("session_id", request.SessionID); err != nil {
			return contracts.EventEnvelope{}, err
		}
	}
	if err := validateProperties("properties", request.Properties); err != nil {
		return contracts.EventEnvelope{}, err
	}
	if err := validateProperties("user_properties", request.UserProps); err != nil {
		return contracts.EventEnvelope{}, err
	}

	eventTime := request.EventTime
	if eventTime.IsZero() {
		eventTime = receivedAt
	}
	if eventTime.After(receivedAt.Add(maxClockSkew)) {
		return contracts.EventEnvelope{}, ValidationError{Field: "event_time", Reason: "too far in the future"}
	}

	return contracts.EventEnvelope{
		ID:         request.ID,
		TenantID:   request.TenantID,
		ProjectID:  request.ProjectID,
		SourceID:   request.SourceID,
		SourceType: request.SourceType,
		EventName:  request.EventName,
		DistinctID: request.DistinctID,
		SessionID:  request.SessionID,
		EventTime:  eventTime.UTC(),
		ReceivedAt: receivedAt.UTC(),
		Properties: cloneMap(request.Properties),
		UserProps:  cloneMap(request.UserProps),
		Source:     request.Source,
	}, nil
}

func trimRequest(request Request) Request {
	request.ID = strings.TrimSpace(request.ID)
	request.TenantID = strings.TrimSpace(request.TenantID)
	request.ProjectID = strings.TrimSpace(request.ProjectID)
	request.SourceID = strings.TrimSpace(request.SourceID)
	request.SourceType = strings.TrimSpace(request.SourceType)
	request.EventName = strings.TrimSpace(request.EventName)
	request.DistinctID = strings.TrimSpace(request.DistinctID)
	request.SessionID = strings.TrimSpace(request.SessionID)
	request.Source = strings.TrimSpace(request.Source)
	request.Client = normalizeClientInfo(request.Client)
	return request
}

func validateIdentifier(field string, value string) error {
	if value == "" {
		return ValidationError{Field: field, Reason: "is required"}
	}
	if len(value) > maxIDLength {
		return ValidationError{Field: field, Reason: "is too long"}
	}
	if !identifierPattern.MatchString(value) {
		return ValidationError{Field: field, Reason: "contains unsupported characters"}
	}
	return nil
}

func validateEventName(value string) error {
	if value == "" {
		return ValidationError{Field: "event_name", Reason: "is required"}
	}
	if len(value) > maxEventNameLength {
		return ValidationError{Field: "event_name", Reason: "is too long"}
	}
	if !eventNamePattern.MatchString(value) {
		return ValidationError{Field: "event_name", Reason: "contains unsupported characters"}
	}
	return nil
}

func validateSourceType(value string) error {
	if value == "" {
		return ValidationError{Field: "source_type", Reason: "is required"}
	}
	if len(value) > maxSourceTypeLen {
		return ValidationError{Field: "source_type", Reason: "is too long"}
	}
	if !sourceTypePattern.MatchString(value) {
		return ValidationError{Field: "source_type", Reason: "contains unsupported characters"}
	}
	return nil
}

func validateProperties(field string, values map[string]any) error {
	// Keep open-ended event data bounded before it reaches the queue. Storage
	// adapters can still choose JSON, Map, or typed rows later, but P1 does not
	// accept unbounded property bags.
	if len(values) > maxPropertyCount {
		return ValidationError{Field: field, Reason: "has too many entries"}
	}

	// Validate each key and scalar value independently so future property
	// dictionaries can trust the collect contract instead of re-sanitizing raw
	// SDK input.
	for key, value := range values {
		if err := validatePropertyKey(field, key); err != nil {
			return err
		}
		if err := validatePropertyValue(propertyFieldName(field, key), value); err != nil {
			return err
		}
	}
	return nil
}

func validatePropertyKey(field string, key string) error {
	if key == "" {
		return ValidationError{Field: field, Reason: "contains empty property key"}
	}
	if len(key) > maxPropertyKeyLen {
		return ValidationError{Field: propertyFieldName(field, key), Reason: "key is too long"}
	}
	if !propertyKeyPattern.MatchString(key) {
		return ValidationError{Field: propertyFieldName(field, key), Reason: "key contains unsupported characters"}
	}
	return nil
}

func validatePropertyValue(field string, value any) error {
	// P1 accepts scalar values that can be indexed or flattened later without a
	// schema guess. Nested objects and arrays stay out until the property model
	// defines whether they are raw JSON, typed rows, or ignored metadata.
	switch typed := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return nil
	case string:
		if len(typed) > maxPropertyStrLen {
			return ValidationError{Field: field, Reason: "string value is too long"}
		}
		return nil
	case float32:
		return validateFloatProperty(field, float64(typed))
	case float64:
		return validateFloatProperty(field, typed)
	case json.Number:
		if _, err := typed.Float64(); err != nil {
			return ValidationError{Field: field, Reason: "number value is invalid"}
		}
		return nil
	default:
		return ValidationError{Field: field, Reason: "has unsupported value type"}
	}
}

func validateFloatProperty(field string, value float64) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ValidationError{Field: field, Reason: "number value is invalid"}
	}
	return nil
}

func propertyFieldName(field string, key string) string {
	if key == "" {
		return field
	}
	return field + "." + key
}

func cloneMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
