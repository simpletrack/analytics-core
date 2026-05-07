package contracts

import "time"

// EventEnvelope is the stable event message passed across collect, queue, and ingestion.
type EventEnvelope struct {
	ID         string         `json:"id"`                        // ID is the stable event id used for idempotent ingestion
	TenantID   string         `json:"tenant_id"`                 // TenantID is the tenant boundary key
	ProjectID  string         `json:"project_id"`                // ProjectID is the project or website boundary key
	SourceID   string         `json:"source_id"`                 // SourceID is the source boundary key inside the project
	SourceType string         `json:"source_type"`               // SourceType is the source category such as web, server, or mobile
	EventName  string         `json:"event_name"`                // EventName is the analytics event name
	DistinctID string         `json:"distinct_id"`               // DistinctID is the visitor or user identity key
	SessionID  string         `json:"session_id,omitempty"`      // SessionID is the optional session key
	VisitID    string         `json:"visit_id,omitempty"`        // VisitID is the canonical analytics visit key
	EventTime  time.Time      `json:"event_time"`                // EventTime is the timestamp produced by the source
	ReceivedAt time.Time      `json:"received_at"`               // ReceivedAt is the timestamp accepted by collect
	Properties map[string]any `json:"properties,omitempty"`      // Properties are event-scoped properties
	UserProps  map[string]any `json:"user_properties,omitempty"` // UserProps are user-scoped properties
	Source     string         `json:"source,omitempty"`          // Source is the optional source label for diagnostics
}
