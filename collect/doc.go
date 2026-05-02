// Package collect owns event decoding, validation, normalization, and
// pre-queue enrichment.
//
// The package is the framework-neutral collect boundary. HTTP adapters may pass
// temporary client metadata through Request.Client, but collect must publish
// only normalized EventEnvelope values and must not leak raw transport objects
// or raw IP addresses into storage.
package collect
