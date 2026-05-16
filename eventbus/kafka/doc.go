// Package kafka implements the production EventBus provider with IBM Sarama.
//
// Sarama's consumer-group session and callback types stay inside this package.
// Callers use eventbus.EventBus, return handler errors, and let the provider
// own retry, dead-letter, and ordered offset completion.
package kafka
