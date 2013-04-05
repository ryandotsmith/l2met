package utils

import (
  "testing"
  "net/http"
  "encoding/base64"
)

func TestParseAuthDBToken(t *testing.T) {
  r := new(http.Request)
  r.SetBasicAuth("l2met", "token")
  expectedUser, expectedPass, err := utils.ParseAuth(r)

  if expectedUser != "l2met" {
		fmt.Printf("expected=%q actual=%q\n", "l2met", expectedUser)
		t.FailNow()
  }

  if expectedPass != "token" {
		fmt.Printf("expected=%q actual=%q\n", "token", expectedPass)
		t.FailNow()
  }
}

func TestParseAuthLibratoCreds(t *testing.T) {}

func TestParseAuthNoPassword(t *testing.T) {
  
}
