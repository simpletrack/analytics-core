package clickhouse_test

import (
	"fmt"
	"strings"

	"github.com/simpletrack/analytics-core/storage/clickhouse"
)

func ExampleCreateEventTableStatement() {
	ddl, err := clickhouse.CreateEventTableStatement(clickhouse.Table{
		Logical:  "events",
		Physical: "events_demo",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS `events_demo`"))
	// Output: true
}

func ExamplePropertyTableFor() {
	table, err := clickhouse.PropertyTableFor(clickhouse.Table{
		Logical:  "events",
		Physical: "events_demo",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(table.Physical)
	// Output: events_demo_properties
}
