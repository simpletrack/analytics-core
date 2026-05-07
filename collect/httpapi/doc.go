// Package httpapi adapts event reporting HTTP requests to collect handlers.
//
// The package owns the Fiber boundary for the P1 POST /collect hot path:
// method checks, path checks, JSON decoding, status codes, and JSON responses.
// It intentionally keeps fiber.Ctx out of collect.Handler so the
// collect core can be reused by workers, tests, SDK adapters, and future
// non-HTTP entrypoints without depending on a specific HTTP library.
package httpapi
