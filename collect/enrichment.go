package collect

import (
	"context"
	"errors"
	"strings"

	"github.com/simpletrack/analytics-core/contracts"
)

const (
	clientUserAgentProperty = "client.user_agent"
	clientIPHashProperty    = "client.ip_hash"
	clientReferrerProperty  = "client.referrer"
	clientBrowserProperty   = "client.browser"
	clientOSProperty        = "client.os"
	clientDeviceProperty    = "client.device"
	geoCountryProperty      = "geo.country"
	geoRegionProperty       = "geo.region"
	geoCityProperty         = "geo.city"
)

// ClientEnrichmentConfig configures derived client properties.
type ClientEnrichmentConfig struct {
	HashSalt           string          // HashSalt namespaces derived hashes when IncludeIPHash is enabled
	IncludeUserAgent   bool            // IncludeUserAgent copies a bounded user agent string into event properties
	IncludeIPHash      bool            // IncludeIPHash stores only a salted IP hash, never the raw IP address
	IncludeReferrer    bool            // IncludeReferrer copies a bounded referrer string into event properties
	IncludeBrowserInfo bool            // IncludeBrowserInfo stores bounded browser, OS, and device families
	IncludeGeoInfo     bool            // IncludeGeoInfo stores bounded country, region, and city values
	UserAgentParser    UserAgentParser // UserAgentParser derives browser, OS, and device families from a user agent
	GeoResolver        GeoResolver     // GeoResolver resolves transient client IPs into coarse geographic properties
}

// UserAgentInfo describes coarse user-agent dimensions for analytics breakdowns.
type UserAgentInfo struct {
	Browser string // Browser is the browser family, for example Chrome or Safari
	OS      string // OS is the operating system family, for example Windows or iOS
	Device  string // Device is the coarse device family, for example desktop, mobile, or tablet
}

// UserAgentParser derives analytics-safe dimensions from a transient user-agent string.
type UserAgentParser interface {
	// Parse returns coarse browser, OS, and device families for userAgent.
	Parse(userAgent string) UserAgentInfo
}

// GeoLocation describes coarse geography derived from a transient client IP.
type GeoLocation struct {
	Country string // Country is the country name or ISO code supplied by the resolver
	Region  string // Region is the state, province, or region supplied by the resolver
	City    string // City is the city supplied by the resolver
}

// GeoResolver resolves transient client IPs into analytics-safe geography.
type GeoResolver interface {
	// Resolve returns coarse geography and false when the IP has no match.
	Resolve(ip string) (GeoLocation, bool)
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
	if config.IncludeBrowserInfo && config.UserAgentParser == nil {
		config.UserAgentParser = defaultUserAgentParser{}
	}
	if config.IncludeGeoInfo && config.GeoResolver == nil {
		return nil, errors.New("client enrichment geo resolver is required when geo info is enabled")
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
	if s.config.IncludeBrowserInfo && client.UserAgent != "" {
		properties = putUserAgentProperties(properties, s.config.UserAgentParser.Parse(client.UserAgent))
	}
	if s.config.IncludeGeoInfo && client.IP != "" {
		properties = putGeoProperties(properties, s.config.GeoResolver, client.IP)
	}

	envelope.Properties = properties
	return envelope, nil
}

func putUserAgentProperties(properties map[string]any, info UserAgentInfo) map[string]any {
	properties = putGeneratedProperty(properties, clientBrowserProperty, truncatePropertyString(info.Browser))
	properties = putGeneratedProperty(properties, clientOSProperty, truncatePropertyString(info.OS))
	properties = putGeneratedProperty(properties, clientDeviceProperty, truncatePropertyString(info.Device))
	return properties
}

func putGeoProperties(properties map[string]any, resolver GeoResolver, ip string) map[string]any {
	location, ok := resolver.Resolve(ip)
	if !ok {
		return properties
	}
	properties = putGeneratedProperty(properties, geoCountryProperty, truncatePropertyString(location.Country))
	properties = putGeneratedProperty(properties, geoRegionProperty, truncatePropertyString(location.Region))
	properties = putGeneratedProperty(properties, geoCityProperty, truncatePropertyString(location.City))
	return properties
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

type defaultUserAgentParser struct{}

func (defaultUserAgentParser) Parse(userAgent string) UserAgentInfo {
	userAgent = strings.TrimSpace(userAgent)
	if userAgent == "" {
		return UserAgentInfo{}
	}

	// Keep the built-in parser intentionally coarse. Production deployments can
	// inject a richer parser without changing collect, storage, or query code.
	lower := strings.ToLower(userAgent)
	return UserAgentInfo{
		Browser: detectBrowserFamily(lower),
		OS:      detectOSFamily(lower),
		Device:  detectDeviceFamily(lower),
	}
}

func detectBrowserFamily(userAgent string) string {
	switch {
	case strings.Contains(userAgent, "edgios") || strings.Contains(userAgent, "edga/") || strings.Contains(userAgent, "edg/") || strings.Contains(userAgent, "edge/"):
		return "Edge"
	case strings.Contains(userAgent, "opios") || strings.Contains(userAgent, "opr/") || strings.Contains(userAgent, "opera"):
		return "Opera"
	case strings.Contains(userAgent, "fxios") || strings.Contains(userAgent, "firefox/"):
		return "Firefox"
	case strings.Contains(userAgent, "crios") || strings.Contains(userAgent, "chrome/") || strings.Contains(userAgent, "chromium/"):
		return "Chrome"
	case strings.Contains(userAgent, "safari/"):
		return "Safari"
	default:
		return ""
	}
}

func detectOSFamily(userAgent string) string {
	switch {
	case strings.Contains(userAgent, "iphone") || strings.Contains(userAgent, "ipad"):
		return "iOS"
	case strings.Contains(userAgent, "android"):
		return "Android"
	case strings.Contains(userAgent, "windows nt"):
		return "Windows"
	case strings.Contains(userAgent, "mac os x") || strings.Contains(userAgent, "macintosh"):
		return "macOS"
	case strings.Contains(userAgent, "cros"):
		return "ChromeOS"
	case strings.Contains(userAgent, "linux"):
		return "Linux"
	default:
		return ""
	}
}

func detectDeviceFamily(userAgent string) string {
	switch {
	case strings.Contains(userAgent, "ipad") || strings.Contains(userAgent, "tablet"):
		return "tablet"
	case strings.Contains(userAgent, "mobi") || strings.Contains(userAgent, "iphone") || strings.Contains(userAgent, "android"):
		return "mobile"
	case strings.Contains(userAgent, "mozilla/"):
		return "desktop"
	default:
		return ""
	}
}
