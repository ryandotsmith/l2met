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
	_, _, err = Parse(r)
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

	libratoUser := "ryan@heroku.com"
	libratoPass := "abc123"
	tok, err := fernet.EncryptAndSign([]byte(libratoUser+":"+libratoPass), keys[0])
	if err != nil {
		t.Error(err)
	}
	r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString(tok))
	expectedUser, expectedPass, err := Parse(r)

	if err != nil {
		t.Error(err)
	}

	if expectedUser != "ryan@heroku.com" {
		t.Errorf("expected=%q actual=%q\n", "l2met", expectedUser)
	}

	if expectedPass != "abc123" {
		t.Errorf("expected=%q actual=%q\n", "token", expectedPass)
	}
}
