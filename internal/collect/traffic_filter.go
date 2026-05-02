package collect

import (
	"context"
	"fmt"
	"net/netip"
	"strings"

	"github.com/simpletrack/analytics-core/pkg/contracts"
)

var defaultBotUserAgentTokens = []string{
	"bot",
	"crawler",
	"headlesschrome",
	"spider",
}

// TrafficFilterConfig configures collect-time traffic rejection.
type TrafficFilterConfig struct {
	BotUserAgents []string // BotUserAgents are case-insensitive user agent substrings to drop before publishing
	InternalCIDRs []string // InternalCIDRs are network ranges to drop before publishing
	InternalIPs   []string // InternalIPs are exact client addresses to drop before publishing
}

// FilteredError reports a valid event that collect intentionally did not publish.
type FilteredError struct {
	Reason   string                  // Reason is the stable filter category or explanation
	Envelope contracts.EventEnvelope // Envelope is the normalized event that was rejected before queue publish
}

// Error returns a stable filter message.
func (e FilteredError) Error() string {
	if e.Reason == "" {
		return "collect traffic filtered"
	}
	return "collect traffic filtered: " + e.Reason
}

// NewTrafficFilterStage creates a Stage that drops bot or internal traffic.
//
// Filtering runs after validation and before EventBus publishing, so invalid
// protocol input still fails loudly while intentional noise is kept out of the
// queue and storage layers.
func NewTrafficFilterStage(config TrafficFilterConfig) (Stage, error) {
	stage := trafficFilterStage{
		botTokens: normalizeBotTokens(config.BotUserAgents),
	}
	if len(stage.botTokens) == 0 {
		stage.botTokens = append(stage.botTokens, defaultBotUserAgentTokens...)
	}

	for _, cidr := range config.InternalCIDRs {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(cidr))
		if err != nil {
			return nil, fmt.Errorf("internal cidr %q is invalid: %w", cidr, err)
		}
		stage.internalCIDRs = append(stage.internalCIDRs, prefix)
	}
	for _, ip := range config.InternalIPs {
		addr, ok := parseClientAddr(ip)
		if !ok {
			return nil, fmt.Errorf("internal ip %q is invalid", ip)
		}
		stage.internalIPs = append(stage.internalIPs, addr)
	}
	return stage, nil
}

type trafficFilterStage struct {
	botTokens     []string       // botTokens are lower-case user agent substrings
	internalCIDRs []netip.Prefix // internalCIDRs are trusted network ranges that should not enter analytics
	internalIPs   []netip.Addr   // internalIPs are exact trusted addresses that should not enter analytics
}

func (s trafficFilterStage) Apply(_ context.Context, input StageInput, envelope contracts.EventEnvelope) (contracts.EventEnvelope, error) {
	client := normalizeClientInfo(input.Request.Client)

	// Bot filtering is intentionally simple and deterministic for P1; stronger
	// classifiers can be swapped into this stage without touching storage.
	if reason := s.matchBot(client.UserAgent); reason != "" {
		return envelope, FilteredError{Reason: reason, Envelope: envelope}
	}

	// Internal traffic checks use transient client IPs only. Raw addresses are
	// discarded after the stage and never written to the envelope.
	if s.matchInternalIP(client.IP) {
		return envelope, FilteredError{Reason: "internal ip", Envelope: envelope}
	}
	return envelope, nil
}

func (s trafficFilterStage) matchBot(userAgent string) string {
	userAgent = strings.ToLower(strings.TrimSpace(userAgent))
	if userAgent == "" {
		return ""
	}
	for _, token := range s.botTokens {
		if token != "" && strings.Contains(userAgent, token) {
			return "bot user agent"
		}
	}
	return ""
}

func (s trafficFilterStage) matchInternalIP(value string) bool {
	addr, ok := parseClientAddr(value)
	if !ok {
		return false
	}
	for _, blocked := range s.internalIPs {
		if addr == blocked {
			return true
		}
	}
	for _, prefix := range s.internalCIDRs {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

func normalizeBotTokens(tokens []string) []string {
	normalized := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.ToLower(strings.TrimSpace(token))
		if token != "" {
			normalized = append(normalized, token)
		}
	}
	return normalized
}

func parseClientAddr(value string) (netip.Addr, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return netip.Addr{}, false
	}
	if comma := strings.IndexByte(value, ','); comma >= 0 {
		value = strings.TrimSpace(value[:comma])
	}
	if addr, err := netip.ParseAddr(value); err == nil {
		return addr, true
	}
	addrPort, err := netip.ParseAddrPort(value)
	if err != nil {
		return netip.Addr{}, false
	}
	return addrPort.Addr(), true
}
