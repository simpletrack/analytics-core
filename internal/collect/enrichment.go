package collect

import (
	"context"
	"errors"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

const (
	clientUserAgentProperty = "client.user_agent"
	clientIPHashProperty    = "client.ip_hash"
	clientReferrerProperty  = "client.referrer"
)

// ClientEnrichmentConfig configures derived client properties.
type ClientEnrichmentConfig struct {
	HashSalt         string // HashSalt namespaces derived hashes when IncludeIPHash is enabled
	IncludeUserAgent bool   // IncludeUserAgent copies a bounded user agent string into event properties
	IncludeIPHash    bool   // IncludeIPHash stores only a salted IP hash, never the raw IP address
	IncludeReferrer  bool   // IncludeReferrer copies a bounded referrer string into event properties
}

// NewClientEnrichmentStage creates a Stage that adds bounded client properties.
//
// The stage writes only derived event properties and deliberately avoids raw IP
// persistence. If the original event already uses one of the reserved property
// keys, the client-provided value wins and enrichment skips that key.
func NewClientEnrichmentStage(config ClientEnrichmentConfig) (Stage, error) {
	if config.IncludeIPHash && config.HashSalt == "" {
		return nil, errors.New("client enrichment hash salt is required when IP hashing is enabled")
	}
	return clientEnrichmentStage{config: config}, nil
}

type clientEnrichmentStage struct {
	config ClientEnrichmentConfig // config selects which transient client fields become derived properties
}

func (s clientEnrichmentStage) Apply(_ context.Context, input StageInput, envelope contracts.EventEnvelope) (contracts.EventEnvelope, error) {
	client := normalizeClientInfo(input.Request.Client)
	properties := cloneMap(envelope.Properties)

	// Add optional metadata one key at a time. Enrichment must never reject an
	// otherwise valid event just because the user's property bag is already full.
	if s.config.IncludeUserAgent && client.UserAgent != "" {
		properties = putGeneratedProperty(properties, clientUserAgentProperty, truncatePropertyString(client.UserAgent))
	}
	if s.config.IncludeReferrer && client.Referrer != "" {
		properties = putGeneratedProperty(properties, clientReferrerProperty, truncatePropertyString(client.Referrer))
	}
	if s.config.IncludeIPHash && client.IP != "" {
		properties = putGeneratedProperty(properties, clientIPHashProperty, "ip_"+saltedDigest("client-ip", s.config.HashSalt, client.IP)[:32])
	}

	envelope.Properties = properties
	return envelope, nil
}

func putGeneratedProperty(properties map[string]any, key string, value string) map[string]any {
	if value == "" {
		return properties
	}
	if properties == nil {
		properties = make(map[string]any, 1)
	}
	if _, exists := properties[key]; exists {
		return properties
	}
	if len(properties) >= maxPropertyCount {
		return properties
	}
	properties[key] = value
	return properties
}
