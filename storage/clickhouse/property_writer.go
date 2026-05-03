package clickhouse

import (
	"context"
	"errors"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/simpletrack/analytics-core/storage"
)

const propertyTableSuffix = "_properties"

var propertyInsertColumns = propertyColumnNames()

// PropertyBatchWriter writes typed event and user properties to ClickHouse.
//
// The writer stays separate from BatchWriter so the event-row commit path keeps
// its existing idempotency semantics. Production ingestion can compose both
// writers through storage.PropertyIndexingEventWriter without teaching queue
// workers about ClickHouse property tables.
type PropertyBatchWriter struct {
	router       *TableRouter     // router resolves tenant/project/source to the matching event table family
	prepareBatch prepareBatchFunc // prepareBatch opens one native ClickHouse batch for one property table
}

// NewPropertyBatchWriter creates a ClickHouse property row writer.
func NewPropertyBatchWriter(conn driver.Conn, router *TableRouter) (*PropertyBatchWriter, error) {
	if conn == nil {
		return nil, errors.New("clickhouse connection is required")
	}

	// Adapt the concrete clickhouse-go/v2 connection to the same narrow batch
	// preparer used by event writes, keeping tests free of a live connection.
	return newPropertyBatchWriterWithPreparer(router, func(ctx context.Context, query string) (nativeBatch, error) {
		return conn.PrepareBatch(ctx, query)
	})
}

func newPropertyBatchWriterWithPreparer(router *TableRouter, prepare prepareBatchFunc) (*PropertyBatchWriter, error) {
	if router == nil {
		return nil, errors.New("table router is required")
	}
	if prepare == nil {
		return nil, errors.New("clickhouse batch preparer is required")
	}
	return &PropertyBatchWriter{router: router, prepareBatch: prepare}, nil
}

// WriteEventProperties writes typed property records through ClickHouse native batches.
func (w *PropertyBatchWriter) WriteEventProperties(ctx context.Context, records []storage.EventPropertyRecord) (storage.PropertyWriteResult, error) {
	if w == nil {
		return storage.PropertyWriteResult{}, errors.New("clickhouse property batch writer is required")
	}
	if len(records) == 0 {
		return storage.PropertyWriteResult{}, nil
	}

	// Group records by the same tenant/project/source routing strategy used by
	// event writes. This lets one call accept a small mixed batch without
	// exposing physical property table names to ingestion.
	groups := make(map[string][]storage.EventPropertyRecord)
	tables := make(map[string]Table)
	for _, record := range records {
		if err := validatePropertyRecord(record); err != nil {
			return storage.PropertyWriteResult{}, err
		}
		table, err := w.routePropertyRecord(record)
		if err != nil {
			return storage.PropertyWriteResult{}, err
		}
		groups[table.Physical] = append(groups[table.Physical], record)
		tables[table.Physical] = table
	}

	// Send one native batch per routed property table. ClickHouse does not make
	// this cross-table transactional; callers should compose it with event
	// writes only after deciding duplicate and retry semantics.
	written := 0
	for tableName, routedRecords := range groups {
		if err := w.writePropertyGroup(ctx, tables[tableName], routedRecords); err != nil {
			return storage.PropertyWriteResult{}, err
		}
		written += len(routedRecords)
	}
	return storage.PropertyWriteResult{Rows: written}, nil
}

func validatePropertyRecord(record storage.EventPropertyRecord) error {
	if record.EventID == "" {
		return errors.New("event_id is required")
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
	return nil
}

func (w *PropertyBatchWriter) routePropertyRecord(record storage.EventPropertyRecord) (Table, error) {
	table, err := w.router.RouteKey(RoutingKey{
		TenantID:  record.TenantID,
		ProjectID: record.ProjectID,
		SourceID:  record.SourceID,
	})
	if err != nil {
		return Table{}, err
	}
	return PropertyTableFor(table)
}

func (w *PropertyBatchWriter) writePropertyGroup(ctx context.Context, table Table, records []storage.EventPropertyRecord) error {
	query := buildPropertyInsertStatement(table.Physical)
	batch, err := w.prepareBatch(ctx, query)
	if err != nil {
		return err
	}

	// Append all records before Send so schema/type failures leave the native
	// batch abortable and prevent partial caller-visible success.
	for _, record := range records {
		if err := batch.Append(propertyInsertValues(record)...); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		return err
	}
	return nil
}

func propertyInsertValues(record storage.EventPropertyRecord) []any {
	return []any{
		record.EventID,
		record.TenantID,
		record.ProjectID,
		record.SourceID,
		record.SourceType,
		record.EventName,
		record.DistinctID,
		record.SessionID,
		record.EventTime.UTC(),
		record.ReceivedAt.UTC(),
		record.Source,
		string(record.Scope),
		record.Name,
		string(record.ValueType),
		record.StringValue,
		record.NumberValue,
		record.BoolValue,
	}
}

func buildPropertyInsertStatement(tableName string) string {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(tableName)
	builder.WriteString(" (")
	// The table name is adapter-generated and columns are fixed, so callers
	// cannot inject ClickHouse identifiers through property metadata.
	for idx, column := range propertyInsertColumns {
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
