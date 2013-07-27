// Provides helpers for authentication & authorization via HTTP.
// Outlet credentials are encrypted and signed in the authorization payload.
package auth

import (
	"encoding/base64"
	"errors"
	"github.com/kr/fernet"
	"github.com/ryandotsmith/l2met/conf"
	"net/http"
	"strings"
	"time"
)

var (
	ttl  = time.Hour * 24 * 365 * 100
	keys []*fernet.Key
)

func init() {
	if len(conf.Secrets) > 0 {
		keys = fernet.MustDecodeKeys(conf.Secrets...)
	}
}

// Use the first valid key to sign b.
// Returns error if no key is able to sign b.
func EncryptAndSign(b []byte) ([]byte, error) {
	for i := range keys {
		if res, err := fernet.EncryptAndSign(b, keys[i]); err == nil {
			return res, err
		}
	}
	return []byte(""), errors.New("Unable to sign payload.")
}

func parseAuthHeader(r *http.Request) (string, error) {
	header, ok := r.Header["Authorization"]
	if !ok {
		return "", errors.New("Unable to parse Authorization header.")
	}
	return header[0], nil
}

// Extract the username and password from the authorization
// line of an HTTP header. This function will handle the
// parsing and decoding of the line.
func ParseRaw(authLine string) (string, string, error) {
	parts := strings.SplitN(authLine, " ", 2)
	if len(parts) != 2 {
		return "", "", errors.New("Authorization header malformed.")
	}

	method := parts[0]
	if method != "Basic" {
		return "", "", errors.New("Authorization must be basic.")
	}

	payload := parts[1]
	decodedPayload, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", "", err
	}

	userPass := strings.SplitN(string(decodedPayload), ":", 2)
	switch len(userPass) {
	case 1:
		return userPass[0], "", nil
	case 2:
		return userPass[0], userPass[1], nil
	}

	return "", "", errors.New("Unable to parse username or password.")
}

// ParseAndDecrypt returns the outlet's API credentials which
// are expected to be signed & encrypted and stuffed into the
// authorization header of the HTTP request.
func ParseAndDecrypt(r *http.Request) (string, string, error) {
	header, err := parseAuthHeader(r)
	if err != nil {
		return "", "", err
	}

	user, _, err := ParseRaw(header)
	if err != nil {
		return "", "", err
	}

	// If we have gotten here, we have a signed, authentication request.
	// If we can verify & decrypt, then we will pass the decrypted credenti
	// to the caller. Most of the time, the username and password will be
	// credentials to Librato metric API. We don't care about the
	// validity of those credentials here. If they are wrong
	// the metrics will be dropped in the outlet. Keep an eye on HTTP
	// authentication errors from the log output of the outlets.
	if len(keys) > 0 {
		s := fernet.VerifyAndDecrypt([]byte(user), ttl, keys)
		if s != nil {
			parts := strings.Split(string(s), ":")
			return parts[0], parts[1], nil
		}
	}
	return "", "", errors.New("End of Authentication chain reached.")
}
