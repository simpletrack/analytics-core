package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

const (
	eventTableOrderBy    = "tenant_id, project_id, source_id, event_time, event_id"
	propertyTableOrderBy = "tenant_id, project_id, source_id, property_scope, property_name, event_time, event_id"
)

var eventTableColumns = []schemaColumn{
	{name: "event_id", typeName: "String"},
	{name: "tenant_id", typeName: "String"},
	{name: "project_id", typeName: "String"},
	{name: "source_id", typeName: "String"},
	{name: "source_type", typeName: "String"},
	{name: "event_name", typeName: "String"},
	{name: "distinct_id", typeName: "String"},
	{name: "session_id", typeName: "String"},
	{name: "event_time", typeName: "DateTime64(3, 'UTC')"},
	{name: "received_at", typeName: "DateTime64(3, 'UTC')"},
	{name: "properties", typeName: "String"},
	{name: "user_properties", typeName: "String"},
	{name: "source", typeName: "String"},
}

var propertyTableColumns = []schemaColumn{
	{name: "event_id", typeName: "String"},
	{name: "tenant_id", typeName: "String"},
	{name: "project_id", typeName: "String"},
	{name: "source_id", typeName: "String"},
	{name: "source_type", typeName: "String"},
	{name: "event_name", typeName: "String"},
	{name: "distinct_id", typeName: "String"},
	{name: "session_id", typeName: "String"},
	{name: "event_time", typeName: "DateTime64(3, 'UTC')"},
	{name: "received_at", typeName: "DateTime64(3, 'UTC')"},
	{name: "source", typeName: "String"},
	{name: "property_scope", typeName: "String"},
	{name: "property_name", typeName: "String"},
	{name: "property_type", typeName: "String"},
	{name: "string_value", typeName: "String"},
	{name: "number_value", typeName: "Float64"},
	{name: "bool_value", typeName: "Bool"},
}

type schemaColumn struct {
	name     string // name is the ClickHouse column identifier used by writer insert allowlists
	typeName string // typeName is the ClickHouse type expected by runtime DDL helpers
}

// CreateEventTableStatement returns the routed event-table DDL used by writers.
func CreateEventTableStatement(table Table) (string, error) {
	tableName, err := validatedPhysicalTableName(table)
	if err != nil {
		return "", err
	}
	// Generate DDL from the same column contract that feeds insert statements
	// so local auto-migration cannot drift from the write adapter silently.
	return buildCreateTableStatement(tableName, eventTableColumns, eventTableOrderBy), nil
}

// CreatePropertyTableStatement returns the routed typed-property table DDL.
func CreatePropertyTableStatement(table Table) (string, error) {
	propertyTable, err := PropertyTableFor(table)
	if err != nil {
		return "", err
	}
	// Property tables are derived from the event table family, preserving the
	// tenant/project/source routing boundary used by property writes.
	return buildCreateTableStatement(propertyTable.Physical, propertyTableColumns, propertyTableOrderBy), nil
}

// PropertyTableFor returns the typed-property table paired with table.
func PropertyTableFor(table Table) (Table, error) {
	tableName, err := validatedPhysicalTableName(table)
	if err != nil {
		return Table{}, err
	}
	return Table{
		Logical:  defaultLogicalTable + propertyTableSuffix,
		Physical: tableName + propertyTableSuffix,
	}, nil
}

func eventColumnNames() []string {
	return columnNames(eventTableColumns)
}

func propertyColumnNames() []string {
	return columnNames(propertyTableColumns)
}

func columnNames(columns []schemaColumn) []string {
	names := make([]string, len(columns))
	for idx, column := range columns {
		names[idx] = column.name
	}
	return names
}

func validatedPhysicalTableName(table Table) (string, error) {
	if strings.TrimSpace(table.Physical) == "" {
		return "", errors.New("physical table name is required")
	}
	if !tablePrefixPattern.MatchString(table.Physical) {
		return "", fmt.Errorf("physical table name %q is not a safe ClickHouse identifier", table.Physical)
	}
	return table.Physical, nil
}

func buildCreateTableStatement(tableName string, columns []schemaColumn, orderBy string) string {
	// Build the DDL from a validated table name and fixed schema columns. This
	// keeps all dynamic identifier handling inside the ClickHouse adapter.
	var builder strings.Builder
	builder.WriteString("CREATE TABLE IF NOT EXISTS ")
	builder.WriteString(quoteClickHouseIdentifier(tableName))
	builder.WriteString(" (\n")
	// Emit columns in writer order so tests can compare the DDL contract with
	// the insert allowlist without maintaining a second column list.
	for idx, column := range columns {
		if idx > 0 {
			builder.WriteString(",\n")
		}
		builder.WriteByte('\t')
		builder.WriteString(quoteClickHouseIdentifier(column.name))
		builder.WriteByte(' ')
		builder.WriteString(column.typeName)
	}
	// Use the same order keys as the e2e ClickHouse path: tenant/project/source
	// first for isolation, then time and id for recent-event scans.
	builder.WriteString("\n) ENGINE = MergeTree\nORDER BY (")
	builder.WriteString(orderBy)
	builder.WriteByte(')')
	return builder.String()
}

func quoteClickHouseIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}
