package httpapi

import (
	"fmt"
	"time"

	"github.com/simpletrack/analytics-core/internal/collect"
	"github.com/valyala/fasthttp"
)

func ExampleNewCollectRoute() {
	bus := &recordingBus{}
	handler, _ := collect.NewHandler(bus, func() time.Time {
		return time.Date(2026, 4, 30, 10, 0, 0, 0, time.UTC)
	})

	route, _ := NewCollectRoute("/collect", handler)

	var request fasthttp.Request
	request.Header.SetMethod(fasthttp.MethodPost)
	request.SetRequestURI("/collect")
	request.Header.SetContentType(contentTypeJSON)
	request.SetBodyString(`{
		"id":"evt_1",
		"tenant_id":"tenant_1",
		"project_id":"project_1",
		"source_id":"source_1",
		"source_type":"web",
		"event_name":"page.view",
		"distinct_id":"visitor_1"
	}`)

	var ctx fasthttp.RequestCtx
	ctx.Init(&request, nil, nil)
	route(&ctx)

	fmt.Println(ctx.Response.StatusCode())

	// Output:
	// 202
}
