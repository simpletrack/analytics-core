package httpapi

import (
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"time"

	"github.com/simpletrack/analytics-core/collect"
	"github.com/valyala/fasthttp"
)

const contentTypeJSON = "application/json"

// CollectHandlerOption configures the fasthttp collect boundary.
type CollectHandlerOption func(*collectHandlerConfig)

// AcceptedResponse is returned when collect accepts an event for ingestion.
type AcceptedResponse struct {
	ID         string `json:"id"`                 // ID is the accepted event id.
	ReceivedAt string `json:"received_at"`        // ReceivedAt is the server acceptance timestamp in RFC3339Nano.
	Filtered   bool   `json:"filtered,omitempty"` // Filtered reports valid traffic dropped before queue publish.
}

// ErrorResponse is returned when collect rejects or fails to publish an event.
type ErrorResponse struct {
	Error string `json:"error"` // Error is the stable error message.
}

// WithTrustedProxyHeaders allows forwarded client IP headers to drive collect stages.
//
// Enable this only behind a trusted proxy that overwrites incoming
// X-Forwarded-For and X-Real-IP headers. Direct internet-facing deployments
// should keep the default RemoteIP-only behavior so clients cannot spoof
// internal traffic filters or salted IP hashes.
func WithTrustedProxyHeaders() CollectHandlerOption {
	return func(config *collectHandlerConfig) {
		config.trustForwardedHeaders = true
	}
}

// NewCollectHandler creates a fasthttp handler for the POST /collect API.
//
// The handler is intentionally thin: it translates HTTP into collect.Request
// and delegates validation plus EventBus publishing to collect.Handler. Keeping
// fasthttp.RequestCtx at this boundary prevents HTTP concerns from leaking into
// the analytics data-plane core.
func NewCollectHandler(handler *collect.Handler, opts ...CollectHandlerOption) fasthttp.RequestHandler {
	config := newCollectHandlerConfig(opts...)
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
		request.Client = clientInfoFromRequest(ctx, config)

		envelope, err := handler.Handle(ctx, request)
		if err != nil {
			// Validation failures are client errors; publish failures are server
			// errors because the event was syntactically valid but not accepted.
			var filteredErr collect.FilteredError
			if errors.As(err, &filteredErr) {
				if envelope.ID == "" {
					envelope = filteredErr.Envelope
				}
				writeJSON(ctx, fasthttp.StatusAccepted, AcceptedResponse{
					ID:         envelope.ID,
					ReceivedAt: envelope.ReceivedAt.Format(time.RFC3339Nano),
					Filtered:   true,
				})
				return
			}
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
func NewCollectRoute(path string, handler *collect.Handler, opts ...CollectHandlerOption) (fasthttp.RequestHandler, error) {
	if path == "" {
		return nil, errors.New("collect path is required")
	}

	collectHandler := NewCollectHandler(handler, opts...)
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

type collectHandlerConfig struct {
	trustForwardedHeaders bool // trustForwardedHeaders enables proxy-provided client address headers
}

func newCollectHandlerConfig(opts ...CollectHandlerOption) collectHandlerConfig {
	config := collectHandlerConfig{}
	for _, opt := range opts {
		// Nil route options are ignored so callers can compose optional proxy
		// configuration without branching at the HTTP adapter boundary.
		if opt != nil {
			opt(&config)
		}
	}
	return config
}

func clientInfoFromRequest(ctx *fasthttp.RequestCtx, config collectHandlerConfig) collect.ClientInfo {
	if ctx == nil {
		return collect.ClientInfo{}
	}

	// Extract only transient client metadata for collect stages. The HTTP
	// adapter decides which address source is trustworthy, while collect decides
	// whether the metadata should become a derived property or filter input.
	return collect.ClientInfo{
		UserAgent: string(ctx.UserAgent()),
		IP:        clientIPFromRequest(ctx, config),
		Referrer:  string(ctx.Request.Header.Peek("Referer")),
	}
}

func clientIPFromRequest(ctx *fasthttp.RequestCtx, config collectHandlerConfig) string {
	// Forwarded address headers are only trustworthy when an upstream proxy is
	// known to strip caller-supplied values. The default direct mode uses the
	// transport remote address to keep filters and hashes resistant to spoofing.
	if config.trustForwardedHeaders {
		if forwarded := firstHeaderValue(ctx.Request.Header.Peek("X-Forwarded-For")); forwarded != "" {
			if addr := canonicalClientIP(forwarded); addr != "" {
				return addr
			}
		}
		if realIP := strings.TrimSpace(string(ctx.Request.Header.Peek("X-Real-IP"))); realIP != "" {
			if addr := canonicalClientIP(realIP); addr != "" {
				return addr
			}
		}
	}
	if remoteIP := ctx.RemoteIP(); remoteIP != nil {
		return canonicalClientIP(remoteIP.String())
	}
	return ""
}

func firstHeaderValue(value []byte) string {
	text := strings.TrimSpace(string(value))
	if text == "" {
		return ""
	}
	if comma := strings.IndexByte(text, ','); comma >= 0 {
		return strings.TrimSpace(text[:comma])
	}
	return text
}

func canonicalClientIP(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	// Accept either bare addresses or address:port values from fasthttp. Invalid
	// values are dropped instead of being passed into filtering or hash inputs.
	if addr, err := netip.ParseAddr(value); err == nil {
		return addr.String()
	}
	addrPort, err := netip.ParseAddrPort(value)
	if err != nil {
		return ""
	}
	return addrPort.Addr().String()
}
