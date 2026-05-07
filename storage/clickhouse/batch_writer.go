package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/simpletrack/analytics-core/contracts"
	"github.com/simpletrack/analytics-core/storage"
)

var eventInsertColumns = eventColumnNames()

// BatchWriterOption customizes BatchWriter dependencies without exposing the
// ClickHouse driver or status-store implementation to ingestion.
type BatchWriterOption func(*BatchWriter)

// WithEventWriteGuard adds durable duplicate protection to BatchWriter.
//
// Production at-least-once consumers should provide this option so repeated
// delivery of the same event_id does not append a second analytics row.
func WithEventWriteGuard(guard storage.EventWriteGuard) BatchWriterOption {
	return func(writer *BatchWriter) {
		writer.guard = guard
	}
}

// BatchWriter writes validated events to ClickHouse using the native batch API.
//
// The writer is the only place where dynamic physical event tables and the
// clickhouse-go/v2 batch protocol meet. Collect, EventBus, ingestion, and
// analysis code should depend on storage.EventWriter instead of importing the
// ClickHouse driver directly.
type BatchWriter struct {
	router       *TableRouter            // router resolves tenant/project/source to a safe physical table
	prepareBatch prepareBatchFunc        // prepareBatch opens one native ClickHouse batch for an insert query
	guard        storage.EventWriteGuard // guard provides durable duplicate protection around at-least-once delivery
}

type nativeBatch interface {
	// Abort releases driver resources after append or send failure.
	Abort() error
	// Append adds one row to the native ClickHouse batch.
	Append(v ...any) error
	// Send flushes the accumulated rows to ClickHouse.
	Send() error
}

// prepareBatchFunc prepares a native ClickHouse batch for a generated insert statement.
type prepareBatchFunc func(context.Context, string) (nativeBatch, error)

// NewBatchWriter creates a ClickHouse batch EventWriter.
func NewBatchWriter(conn driver.Conn, router *TableRouter, opts ...BatchWriterOption) (*BatchWriter, error) {
	if conn == nil {
		return nil, errors.New("clickhouse connection is required")
	}

	// Adapt the concrete clickhouse-go/v2 connection to the narrow preparer
	// function used by tests and by the storage boundary.
	return newBatchWriterWithPreparer(router, func(ctx context.Context, query string) (nativeBatch, error) {
		return conn.PrepareBatch(ctx, query)
	}, opts...)
}

func newBatchWriterWithPreparer(router *TableRouter, prepare prepareBatchFunc, opts ...BatchWriterOption) (*BatchWriter, error) {
	if router == nil {
		return nil, errors.New("table router is required")
	}
	if prepare == nil {
		return nil, errors.New("clickhouse batch preparer is required")
	}

	// Keep optional dependencies behind functional options so ingestion never
	// needs to know which status store or ClickHouse connection is in use.
	writer := &BatchWriter{
		router:       router,
		prepareBatch: prepare,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(writer)
		}
	}
	return writer, nil
}

// WriteEvent writes one event envelope through a ClickHouse native batch.
//
// The method keeps queue ack semantics simple: it returns Inserted=false only
// when the idempotency guard proves the event was already inserted. Any prepare,
// append, send, commit, or rollback failure returns an error so the EventBus can
// leave the message unacknowledged for retry or dead-letter handling.
func (w *BatchWriter) WriteEvent(ctx context.Context, envelope contracts.EventEnvelope) (storage.WriteResult, error) {
	if w == nil {
		return storage.WriteResult{}, errors.New("clickhouse batch writer is required")
	}
	if envelope.ID == "" {
		return storage.WriteResult{}, errors.New("event_id is required")
	}

	// Claim the event before touching ClickHouse so at-least-once queue replay
	// can become an explicit duplicate no-op instead of a second event row.
	claim, err := w.startEventWrite(ctx, envelope)
	if err != nil {
		return storage.WriteResult{}, err
	}
	if claim != nil && claim.AlreadyInserted() {
		return storage.WriteResult{Inserted: false}, nil
	}

	// Append the event only after the idempotency claim succeeds; append
	// failures must roll the claim back so the queue can retry cleanly.
	if err := w.writeClaimedEvent(ctx, envelope); err != nil {
		return storage.WriteResult{}, w.rollbackEventWrite(ctx, claim, err)
	}
	if claim != nil {
		// Commit happens after ClickHouse Send because the status row is the
		// durable signal used by later duplicate deliveries.
		if err := claim.Commit(ctx); err != nil {
			return storage.WriteResult{}, err
		}
	}
	return storage.WriteResult{Inserted: true}, nil
}

func (w *BatchWriter) startEventWrite(ctx context.Context, envelope contracts.EventEnvelope) (storage.EventWriteClaim, error) {
	if w.guard == nil {
		// Tests and low-risk demos may run without durable duplicate tracking,
		// but production consumers should always configure a guard.
		return nil, nil
	}

	claim, err := w.guard.StartEventWrite(ctx, envelope)
	if err != nil {
		return nil, err
	}
	if claim == nil {
		return nil, errors.New("event write guard returned nil claim")
	}
	return claim, nil
}

func (w *BatchWriter) writeClaimedEvent(ctx context.Context, envelope contracts.EventEnvelope) error {
	// Table routing is kept inside the ClickHouse adapter so dynamic physical
	// table names never leak into collect, ingestion, or analysis code.
	table, err := w.router.Route(envelope)
	if err != nil {
		return err
	}

	// PrepareBatch uses the native ClickHouse protocol and is the hot-path
	// alternative to ORM CreateInBatches for event details.
	query := buildEventInsertStatement(table.Physical)
	batch, err := w.prepareBatch(ctx, query)
	if err != nil {
		return err
	}

	// Marshal event maps before Append so malformed property values abort the
	// batch without sending a partial row.
	values, err := eventInsertValues(envelope)
	if err != nil {
		_ = batch.Abort()
		return err
	}
	// Append and Send are separated because clickhouse-go can surface schema or
	// type issues before network flush; both failures must abort driver state.
	if err := batch.Append(values...); err != nil {
		_ = batch.Abort()
		return err
	}
	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		return err
	}
	return nil
}

func (w *BatchWriter) rollbackEventWrite(ctx context.Context, claim storage.EventWriteClaim, cause error) error {
	if claim == nil {
		return cause
	}
	// Join rollback errors with the original append error so the queue retry
	// path keeps the root cause while still exposing status-store failure.
	if err := claim.Rollback(ctx, cause); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func eventInsertValues(envelope contracts.EventEnvelope) ([]any, error) {
	// Store open-ended properties as JSON strings for the first write path;
	// typed property indexing can be added later behind metadata migrations.
	properties, err := marshalMapString(envelope.Properties)
	if err != nil {
		return nil, fmt.Errorf("marshal properties: %w", err)
	}
	userProperties, err := marshalMapString(envelope.UserProps)
	if err != nil {
		return nil, fmt.Errorf("marshal user_properties: %w", err)
	}

	return []any{
		envelope.ID,
		envelope.TenantID,
		envelope.ProjectID,
		envelope.SourceID,
		envelope.SourceType,
		envelope.EventName,
		envelope.DistinctID,
		envelope.SessionID,
		envelope.VisitID,
		envelope.EventTime.UTC(),
		envelope.ReceivedAt.UTC(),
		properties,
		userProperties,
		envelope.Source,
	}, nil
}

func marshalMapString(value map[string]any) (string, error) {
	if value == nil {
		return "{}", nil
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func buildEventInsertStatement(tableName string) string {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(tableName)
	builder.WriteString(" (")
	// Only adapter-generated table names and a fixed column allowlist reach this
	// SQL string; callers cannot inject arbitrary identifiers.
	for idx, column := range eventInsertColumns {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('`')
		builder.WriteString(column)
		builder.WriteByte('`')
	}
	builder.WriteByte(')')
	return builder.String()
}
