package auth

import (
	"bytes"
	"encoding/base64"
	"github.com/kr/fernet"
	"net/http"
	"testing"
)

func TestParseNoPassword(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}
	_, _, err = ParseAndDecrypt(r)
	if err == nil {
		t.Errorf("Expected Parse to return error when with no creds.")
	}
}

func TestParseEncryptedAuth(t *testing.T) {
	if len(keys) == 0 {
		t.Fatalf("Must set $SECRETS\n")
	}

	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}

	u := "ryan@heroku.com"
	p := "abc123"
	tok, err := fernet.EncryptAndSign([]byte(u+":"+p), keys[0])
	if err != nil {
		t.Error(err)
	}
	r.Header.Set("Authorization",
		"Basic "+base64.StdEncoding.EncodeToString(tok))

	expectedUser, expectedPass, err := ParseAndDecrypt(r)
	if err != nil {
		t.Error(err)
	}

	if expectedUser != u {
		t.Errorf("expected=%q actual=%q\n", u, expectedUser)
	}

	if expectedPass != p {
		t.Errorf("expected=%q actual=%q\n", p, expectedPass)
	}
}
