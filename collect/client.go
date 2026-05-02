package collect

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// ClientInfo carries transient transport metadata for collect stages.
//
// ClientInfo is intentionally excluded from JSON request decoding. HTTP
// adapters populate it from headers and remote connection metadata after body
// decoding so raw network details do not become part of the public event
// protocol or storage contract.
type ClientInfo struct {
	UserAgent string `json:"-"` // UserAgent is the trimmed request user agent used for filtering or derived properties
	IP        string `json:"-"` // IP is the transient client address used for filtering or salted hashing only
	Referrer  string `json:"-"` // Referrer is the trimmed browser referrer used for derived event properties
}

func normalizeClientInfo(info ClientInfo) ClientInfo {
	info.UserAgent = strings.TrimSpace(info.UserAgent)
	info.IP = strings.TrimSpace(info.IP)
	info.Referrer = strings.TrimSpace(info.Referrer)
	return info
}

func saltedDigest(prefix string, salt string, parts ...string) string {
	hash := sha256.New()
	hash.Write([]byte(prefix))
	hash.Write([]byte{0})
	hash.Write([]byte(salt))
	for _, part := range parts {
		hash.Write([]byte{0})
		hash.Write([]byte(part))
	}
	sum := hash.Sum(nil)
	return hex.EncodeToString(sum)
}

func truncatePropertyString(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= maxPropertyStrLen {
		return value
	}
	return value[:maxPropertyStrLen]
}
