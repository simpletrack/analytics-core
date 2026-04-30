package contracts

import "time"

// EventEnvelope is the stable event message passed across collect, queue, and ingestion.
type EventEnvelope struct {
	ID         string         `json:"id"`
	TenantID   string         `json:"tenant_id"`
	ProjectID  string         `json:"project_id"`
	SourceID   string         `json:"source_id"`
	SourceType string         `json:"source_type"`
	EventName  string         `json:"event_name"`
	DistinctID string         `json:"distinct_id"`
	SessionID  string         `json:"session_id,omitempty"`
	EventTime  time.Time      `json:"event_time"`
	ReceivedAt time.Time      `json:"received_at"`
	Properties map[string]any `json:"properties,omitempty"`
	UserProps  map[string]any `json:"user_properties,omitempty"`
	Source     string         `json:"source,omitempty"`
}
