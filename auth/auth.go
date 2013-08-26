// Provides helpers for authentication & authorization via HTTP.
// Outlet credentials are encrypted and signed in the authorization payload.
package auth

import (
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/kr/fernet"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	ttl  = time.Hour * 24 * 365 * 100
	keys []*fernet.Key
)

func init() {
	s := os.Getenv("SECRETS")
	if len(s) > 0 {
		keys = fernet.MustDecodeKeys(strings.Split(s, ":")...)
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

// Extract the username and password from the authorization
// line of an HTTP header. This function will handle the
// parsing and decoding of the line.
func Parse(authLine string) (string, error) {
	parts := strings.SplitN(authLine, " ", 2)
	if len(parts) != 2 {
		return "", errors.New("Authorization header malformed.")
	}
	method := parts[0]
	if method != "Basic" {
		return "", errors.New("Authorization must be basic.")
	}
	payload := parts[1]
	decodedPayload, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(decodedPayload), ":"), nil
}

func Decrypt(s string) (string, error) {
	msg := fernet.VerifyAndDecrypt([]byte(s), ttl, keys)
	if msg == nil {
		return "", errors.New("Unable to decrypt.")
	}
	return string(msg), nil
}

func ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method must be POST.", 400)
		return
	}
	l := r.Header.Get("Authorization")
	user, err := Parse(l)
	if err != nil {
		http.Error(w, "Unable to parse headers.", 400)
		return
	}
	matched := false
	for i := range keys {
		if user == keys[i].Encode() {
			matched = true
		}
	}
	if !matched {
		http.Error(w, "Authentication failed.", 401)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		http.Error(w, "Unable to read body.", 400)
		return
	}
	signed, err := EncryptAndSign(b)
	if err != nil {
		http.Error(w, "Unable to sign body.", 500)
		return
	}
	fmt.Fprint(w, string(signed))
}
