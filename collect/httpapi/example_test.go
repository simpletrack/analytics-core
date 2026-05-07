package httpapi

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/simpletrack/analytics-core/collect"
)

func ExampleNewCollectApp() {
	bus := &recordingBus{}
	handler, _ := collect.NewHandler(bus, func() time.Time {
		return time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	})

	app, _ := NewCollectApp("/collect", handler)

	request, _ := http.NewRequest(http.MethodPost, "/collect", bytes.NewBufferString(`{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`))
	request.Header.Set("Content-Type", contentTypeJSON)

	response, _ := app.Test(request)
	fmt.Println(response.StatusCode)

	// Output:
	// 202
}
