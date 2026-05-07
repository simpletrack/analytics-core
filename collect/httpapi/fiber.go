package httpapi

import (
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/simpletrack/analytics-core/collect"
)

const contentTypeJSON = "application/json"

// CollectHandlerOption configures the Fiber collect boundary.
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
// should keep the default transport address behavior so clients cannot spoof
// internal traffic filters or salted IP hashes.
func WithTrustedProxyHeaders() CollectHandlerOption {
	return func(config *collectHandlerConfig) {
		config.trustForwardedHeaders = true
	}
}

// NewCollectHandler creates a Fiber handler for the POST /collect API.
//
// The handler is intentionally thin: it translates HTTP into collect.Request
// and delegates validation plus EventBus publishing to collect.Handler. Keeping
// Fiber Ctx at this boundary prevents HTTP concerns from leaking into the
// analytics data-plane core.
func NewCollectHandler(handler *collect.Handler, opts ...CollectHandlerOption) fiber.Handler {
	config := newCollectHandlerConfig(opts...)
	return func(ctx fiber.Ctx) error {
		if handler == nil {
			return writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: "collect handler is required"})
		}

		// POST is the only event reporting method in P1, which keeps retries and
		// client SDK behavior predictable while query APIs are designed separately.
		if ctx.Method() != fiber.MethodPost {
			return writeJSON(ctx, fiber.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		}

		var request collect.Request
		if err := json.Unmarshal(ctx.Body(), &request); err != nil {
			return writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: "invalid collect payload"})
		}
		request.Client = clientInfoFromRequest(ctx, config)

		envelope, err := handler.Handle(ctx.Context(), request)
		if err != nil {
			// Validation failures are client errors; publish failures are server
			// errors because the event was syntactically valid but not accepted.
			var filteredErr collect.FilteredError
			if errors.As(err, &filteredErr) {
				if envelope.ID == "" {
					envelope = filteredErr.Envelope
				}
				return writeJSON(ctx, fiber.StatusAccepted, AcceptedResponse{
					ID:         envelope.ID,
					ReceivedAt: envelope.ReceivedAt.Format(time.RFC3339Nano),
					Filtered:   true,
				})
			}
			var validationErr collect.ValidationError
			if errors.As(err, &validationErr) {
				return writeJSON(ctx, fiber.StatusBadRequest, ErrorResponse{Error: validationErr.Error()})
			}
			return writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		}

		return writeJSON(ctx, fiber.StatusAccepted, AcceptedResponse{
			ID:         envelope.ID,
			ReceivedAt: envelope.ReceivedAt.Format(time.RFC3339Nano),
		})
	}
}

// RegisterCollectRoute registers the collect route on an existing Fiber app.
func RegisterCollectRoute(app *fiber.App, path string, handler *collect.Handler, opts ...CollectHandlerOption) error {
	if app == nil {
		return errors.New("fiber app is required")
	}
	if path == "" {
		return errors.New("collect path is required")
	}

	app.All(path, NewCollectHandler(handler, opts...))
	return nil
}

// NewCollectApp creates a minimal Fiber app for one collect route.
func NewCollectApp(path string, handler *collect.Handler, opts ...CollectHandlerOption) (*fiber.App, error) {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(ctx fiber.Ctx, err error) error {
			return writeJSON(ctx, fiber.StatusInternalServerError, ErrorResponse{Error: err.Error()})
		},
	})
	if err := RegisterCollectRoute(app, path, handler, opts...); err != nil {
		return nil, err
	}
	app.Use(func(ctx fiber.Ctx) error {
		return writeJSON(ctx, fiber.StatusNotFound, ErrorResponse{Error: "not found"})
	})
	return app, nil
}

// writeJSON writes the stable JSON response shape for collect endpoints.
func writeJSON(ctx fiber.Ctx, statusCode int, response any) error {
	return ctx.Status(statusCode).JSON(response, contentTypeJSON)
}

type collectHandlerConfig struct {
	trustForwardedHeaders bool // trustForwardedHeaders enables proxy-provided client address headers.
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

func clientInfoFromRequest(ctx fiber.Ctx, config collectHandlerConfig) collect.ClientInfo {
	if ctx == nil {
		return collect.ClientInfo{}
	}

	// Extract only transient client metadata for collect stages. The HTTP
	// adapter decides which address source is trustworthy, while collect decides
	// whether the metadata should become a derived property or filter input.
	return collect.ClientInfo{
		UserAgent: ctx.Get("User-Agent"),
		IP:        clientIPFromRequest(ctx, config),
		Referrer:  ctx.Get("Referer"),
	}
}

func clientIPFromRequest(ctx fiber.Ctx, config collectHandlerConfig) string {
	// Forwarded address headers are only trustworthy when an upstream proxy is
	// known to strip caller-supplied values. The default direct mode uses the
	// transport remote address to keep filters and hashes resistant to spoofing.
	if config.trustForwardedHeaders {
		if forwarded := firstHeaderValue(ctx.Get("X-Forwarded-For")); forwarded != "" {
			if addr := canonicalClientIP(forwarded); addr != "" {
				return addr
			}
		}
		if realIP := strings.TrimSpace(ctx.Get("X-Real-IP")); realIP != "" {
			if addr := canonicalClientIP(realIP); addr != "" {
				return addr
			}
		}
	}
	return canonicalClientIP(ctx.IP())
}

func firstHeaderValue(value string) string {
	text := strings.TrimSpace(value)
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

	// Accept either bare addresses or address:port values from Fiber. Invalid
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
