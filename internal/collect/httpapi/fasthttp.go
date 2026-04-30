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
	ID         string `json:"id"`          // ID is the accepted event id
	ReceivedAt string `json:"received_at"` // ReceivedAt is the server acceptance timestamp in RFC3339Nano
}

// ErrorResponse is returned when collect rejects or fails to publish an event.
type ErrorResponse struct {
	Error string `json:"error"` // Error is the stable error message
}

// NewCollectHandler creates a fasthttp handler for the collect API.
func NewCollectHandler(handler *collect.Handler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if handler == nil {
			writeJSON(ctx, fasthttp.StatusInternalServerError, ErrorResponse{Error: "collect handler is required"})
			return
		}
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
