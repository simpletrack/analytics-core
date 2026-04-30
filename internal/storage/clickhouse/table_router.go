package clickhouse

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

const defaultLogicalTable = "events"

var tablePrefixPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// Table identifies the logical and physical ClickHouse event table.
type Table struct {
	Logical  string // Logical is the logical table name exposed to query builders
	Physical string // Physical is the physical table name used only inside storage adapters
}

// RoutingKey identifies the tenant/project/source table routing boundary.
type RoutingKey struct {
	TenantID  string // TenantID is the tenant boundary key
	ProjectID string // ProjectID is the project or website boundary key
	SourceID  string // SourceID is the source boundary key inside the project
}

// TableRouter maps an event envelope to a physical ClickHouse table.
type TableRouter struct {
	prefix string // prefix is the safe physical table prefix for routed event tables
}

// NewTableRouter creates a table router with a safe physical table prefix.
func NewTableRouter(prefix string) (*TableRouter, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = defaultLogicalTable
	}
	if !tablePrefixPattern.MatchString(prefix) {
		return nil, fmt.Errorf("table prefix %q is not a safe ClickHouse identifier", prefix)
	}
	return &TableRouter{prefix: prefix}, nil
}

// Route returns the logical and physical table for envelope.
func (r *TableRouter) Route(envelope contracts.EventEnvelope) (Table, error) {
	// Convert the write envelope to the same routing key used by read queries
	// so ingestion and analysis cannot drift into different table strategies.
	return r.RouteKey(RoutingKey{
		TenantID:  envelope.TenantID,
		ProjectID: envelope.ProjectID,
		SourceID:  envelope.SourceID,
	})
}

// RouteKey returns the logical and physical table for a routing key.
func (r *TableRouter) RouteKey(key RoutingKey) (Table, error) {
	if r == nil {
		return Table{}, errors.New("table router is required")
	}
	if key.TenantID == "" {
		return Table{}, errors.New("tenant_id is required")
	}
	if key.ProjectID == "" {
		return Table{}, errors.New("project_id is required")
	}
	if key.SourceID == "" {
		return Table{}, errors.New("source_id is required")
	}

	// Only hashed routing parts reach the physical table name so raw tenant,
	// project, and source identifiers never leak into ClickHouse identifiers.
	return Table{
		Logical:  defaultLogicalTable,
		Physical: fmt.Sprintf("%s_%s_%s_%s", r.prefix, shortHash(key.TenantID), shortHash(key.ProjectID), shortHash(key.SourceID)),
	}, nil
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:12]
}
