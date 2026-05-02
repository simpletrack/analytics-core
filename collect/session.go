package collect

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/simpletrack/analytics-core/contracts"
)

const defaultSessionWindow = 30 * time.Minute

// SessionResolverConfig configures privacy-friendly session id derivation.
type SessionResolverConfig struct {
	Salt                     string        // Salt namespaces derived ids and prevents cross-installation joins
	Window                   time.Duration // Window is the deterministic activity bucket used when no session id is supplied
	IncludeClientFingerprint bool          // IncludeClientFingerprint adds transient user agent and IP context to the hash input
}

// NewSessionResolverStage creates a Stage that fills missing session ids.
//
// The resolver never stores raw IP or user agent values. When client
// fingerprinting is enabled, those values are included only in the salted hash
// material used to derive the session id.
func NewSessionResolverStage(config SessionResolverConfig) (Stage, error) {
	if config.Salt == "" {
		return nil, errors.New("session resolver salt is required")
	}
	if config.Window == 0 {
		config.Window = defaultSessionWindow
	}
	if config.Window < time.Minute {
		return nil, errors.New("session resolver window must be at least one minute")
	}
	return sessionResolverStage{config: config}, nil
}

type sessionResolverStage struct {
	config SessionResolverConfig // config keeps derivation deterministic and privacy bounded
}

func (s sessionResolverStage) Apply(_ context.Context, input StageInput, envelope contracts.EventEnvelope) (contracts.EventEnvelope, error) {
	if envelope.SessionID != "" {
		return envelope, nil
	}

	// Build the stable bucket from event time rather than wall time so retries of
	// the same source event keep the same derived session id.
	bucket := envelope.EventTime.UTC().Truncate(s.config.Window).Unix()
	parts := []string{
		envelope.TenantID,
		envelope.ProjectID,
		envelope.SourceID,
		envelope.DistinctID,
		strconv.FormatInt(bucket, 10),
	}

	// Optional client fingerprinting improves browser-session separation without
	// writing raw IP or user agent into the envelope.
	client := normalizeClientInfo(input.Request.Client)
	if s.config.IncludeClientFingerprint {
		parts = append(parts, client.UserAgent, client.IP)
	}

	envelope.SessionID = fmt.Sprintf("ses_%s", saltedDigest("session", s.config.Salt, parts...)[:32])
	if err := validateIdentifier("session_id", envelope.SessionID); err != nil {
		return contracts.EventEnvelope{}, err
	}
	return envelope, nil
}
