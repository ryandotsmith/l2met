package utils

import (
	"encoding/base64"
	"bytes"
	"net/http"
	"testing"
)

func TestParseAuthDBToken(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}
	r.SetBasicAuth("l2met", "token")
	expectedUser, expectedPass, err := ParseAuth(r)

	if err != nil {
		t.Error(err)
	}

	if expectedUser != "l2met" {
		t.Errorf("expected=%q actual=%q\n", "l2met", expectedUser)
	}

	if expectedPass != "token" {
		t.Errorf("expected=%q actual=%q\n", "token", expectedPass)
	}
}

func TestParseAuthLibratoCreds(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}

	libratoUser := "ryan@heroku.com"
	libratoPass := "abc123"
	tok, err := authSecret1.EncryptAndSign([]byte(libratoUser + ":" + libratoPass))
	if err != nil {
		t.Error(err)
	}
	r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString(tok))
	expectedUser, expectedPass, err := ParseAuth(r)

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

func TestParseAuthNoPassword(t *testing.T) {
	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Error(err)
	}
	_, _, err = ParseAuth(r)
	if err == nil {
		t.Errorf("Expected ParseAuth to return error when with no creds.")
	}
}
