package clickhouse

import (
	"strings"
	"testing"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

func TestTableRouterRoutesByTenantProjectAndSource(t *testing.T) {
	router, err := NewTableRouter("")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	table, err := router.Route(contracts.EventEnvelope{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
	})
	if err != nil {
		t.Fatalf("route failed: %v", err)
	}

	if table.Logical != "events" {
		t.Fatalf("expected logical events table, got %q", table.Logical)
	}
	if !strings.HasPrefix(table.Physical, "events_") {
		t.Fatalf("expected physical events prefix, got %q", table.Physical)
	}
	if strings.Contains(table.Physical, "tenant_1") || strings.Contains(table.Physical, "project_1") || strings.Contains(table.Physical, "source_1") {
		t.Fatalf("physical table should not expose raw tenant/project/source ids: %q", table.Physical)
	}
}

func TestTableRouterReturnsStablePhysicalName(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	envelope := contracts.EventEnvelope{
		TenantID:  "tenant_1",
		ProjectID: "project_1",
		SourceID:  "source_1",
	}
	first, err := router.Route(envelope)
	if err != nil {
		t.Fatalf("first route failed: %v", err)
	}
	second, err := router.Route(envelope)
	if err != nil {
		t.Fatalf("second route failed: %v", err)
	}
	if first.Physical != second.Physical {
		t.Fatalf("expected stable physical table, got %q and %q", first.Physical, second.Physical)
	}
}

func TestTableRouterRejectsUnsafePrefix(t *testing.T) {
	if _, err := NewTableRouter("events;drop"); err == nil {
		t.Fatal("expected unsafe prefix error")
	}
}

func TestTableRouterRequiresRoutingKeys(t *testing.T) {
	router, err := NewTableRouter("events")
	if err != nil {
		t.Fatalf("new table router failed: %v", err)
	}

	if _, err := router.Route(contracts.EventEnvelope{TenantID: "tenant_1", ProjectID: "project_1"}); err == nil {
		t.Fatal("expected missing source error")
	}
}
