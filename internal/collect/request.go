package collect

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

const (
	maxClockSkew       = 5 * time.Minute
	maxEventNameLength = 128
	maxIDLength        = 128
	maxSourceTypeLen   = 32
)

var (
	eventNamePattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]*$`)
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]*$`)
	sourceTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
)

// Request is the normalized collect input before validation.
type Request struct {
	ID         string         // ID is the stable event id used for idempotent ingestion
	TenantID   string         // TenantID is the tenant boundary key
	ProjectID  string         // ProjectID is the project or website boundary key
	SourceID   string         // SourceID is the source boundary key inside the project
	SourceType string         // SourceType is the source category such as web, server, or mobile
	EventName  string         // EventName is the analytics event name
	DistinctID string         // DistinctID is the visitor or user identity key
	SessionID  string         // SessionID is the optional session key
	EventTime  time.Time      // EventTime is the timestamp produced by the source
	Properties map[string]any // Properties are event-scoped properties
	UserProps  map[string]any // UserProps are user-scoped properties
	Source     string         // Source is an optional diagnostic source label
}

// ValidationError describes a rejected collect field.
type ValidationError struct {
	Field  string // Field is the rejected field name
	Reason string // Reason is the validation failure summary
}

// Error returns a stable human-readable validation error.
func (e ValidationError) Error() string {
	if e.Field == "" {
		return e.Reason
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Reason)
}

// Normalize validates request and returns a stable event envelope.
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
