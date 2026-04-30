package contracts

import "time"

// EventEnvelope is the stable event message passed across collect, queue, and ingestion.
type EventEnvelope struct {
	ID         string         `json:"id"`                        // stable event id used for idempotent ingestion
	TenantID   string         `json:"tenant_id"`                 // tenant boundary key
	ProjectID  string         `json:"project_id"`                // project or website boundary key
	SourceID   string         `json:"source_id"`                 // source boundary key inside the project
	SourceType string         `json:"source_type"`               // source category such as web, server, or mobile
	EventName  string         `json:"event_name"`                // analytics event name
	DistinctID string         `json:"distinct_id"`               // visitor or user identity key
	SessionID  string         `json:"session_id,omitempty"`      // optional session key
	EventTime  time.Time      `json:"event_time"`                // timestamp produced by the source
	ReceivedAt time.Time      `json:"received_at"`               // timestamp accepted by collect
	Properties map[string]any `json:"properties,omitempty"`      // event-scoped properties
	UserProps  map[string]any `json:"user_properties,omitempty"` // user-scoped properties
	Source     string         `json:"source,omitempty"`          // optional source label for diagnostics
}
