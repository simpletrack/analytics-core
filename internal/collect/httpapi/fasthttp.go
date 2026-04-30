package httpapi

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/simpletrack/analytics-core/internal/collect"
	"github.com/valyala/fasthttp"
)

const contentTypeJSON = "application/json"

// AcceptedResponse is returned when collect accepts an event for ingestion.
type AcceptedResponse struct {
	ID         string `json:"id"`          // ID is the accepted event id.
	ReceivedAt string `json:"received_at"` // ReceivedAt is the server acceptance timestamp in RFC3339Nano.
}

// ErrorResponse is returned when collect rejects or fails to publish an event.
type ErrorResponse struct {
	Error string `json:"error"` // Error is the stable error message.
}

// NewCollectHandler creates a fasthttp handler for the POST /collect API.
//
// The handler is intentionally thin: it translates HTTP into collect.Request
// and delegates validation plus EventBus publishing to collect.Handler. Keeping
// fasthttp.RequestCtx at this boundary prevents HTTP concerns from leaking into
// the analytics data-plane core.
func NewCollectHandler(handler *collect.Handler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if handler == nil {
			writeJSON(ctx, fasthttp.StatusInternalServerError, ErrorResponse{Error: "collect handler is required"})
			return
		}
		// POST is the only event reporting method in P1, which keeps retries and
		// client SDK behavior predictable while query APIs are designed separately.
		if !ctx.IsPost() {
			writeJSON(ctx, fasthttp.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
			return
		}

		var request collect.Request
		if err := json.Unmarshal(ctx.PostBody(), &request); err != nil {
			writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: "invalid collect payload"})
			return
		}

		envelope, err := handler.Handle(ctx, request)
		if err != nil {
			// Validation failures are client errors; publish failures are server
			// errors because the event was syntactically valid but not accepted.
			var validationErr collect.ValidationError
			if errors.As(err, &validationErr) {
				writeJSON(ctx, fasthttp.StatusBadRequest, ErrorResponse{Error: validationErr.Error()})
				return
			}
			writeJSON(ctx, fasthttp.StatusInternalServerError, ErrorResponse{Error: err.Error()})
			return
		}

		writeJSON(ctx, fasthttp.StatusAccepted, AcceptedResponse{
			ID:         envelope.ID,
			ReceivedAt: envelope.ReceivedAt.Format(time.RFC3339Nano),
		})
	}
}

// NewCollectRoute creates a minimal fasthttp route guard for a collect path.
//
// P1 has a single event reporting hot path, so a path guard avoids bringing a
// low-activity router dependency into analytics-core before there is a real
// routing surface to justify it.
func NewCollectRoute(path string, handler *collect.Handler) (fasthttp.RequestHandler, error) {
	if path == "" {
		return nil, errors.New("collect path is required")
	}

	collectHandler := NewCollectHandler(handler)
	return func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Path()) != path {
			writeJSON(ctx, fasthttp.StatusNotFound, ErrorResponse{Error: "not found"})
			return
		}
		collectHandler(ctx)
	}, nil
}

// writeJSON writes the stable JSON response shape for collect endpoints.
func writeJSON(ctx *fasthttp.RequestCtx, statusCode int, response any) {
	ctx.SetStatusCode(statusCode)
	ctx.Response.Header.SetContentType(contentTypeJSON)

	payload, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString(`{"error":"failed to encode response"}`)
		return
	}
	ctx.SetBody(payload)
}
