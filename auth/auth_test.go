package auth

import (
	"bytes"
	"encoding/base64"
	"github.com/kr/fernet"
	"net/http"
	"testing"
)

type authTest struct {
	input  string
	output string
}

var authTests = []authTest{
	{
		"user:password",
		"user:password",
	},
}

func TestAuth(t *testing.T) {
	for _, ts := range authTests {
		testEncryptDecrypt(t, ts)
	}
}

func testEncryptDecrypt(t *testing.T, ts authTest) {
	if len(keys) == 0 {
		t.Fatalf("Must set $SECRETS\n")
	}

	var b bytes.Buffer
	r, err := http.NewRequest("GET", "http://does-not-matter.com", &b)
	if err != nil {
		t.Fatalf("error=%s\n", err)
	}

	tok, err := fernet.EncryptAndSign([]byte(ts.input), keys[0])
	if err != nil {
		t.Fatalf("error=%s\n", err)
	}
	r.Header.Set("Authorization",
		"Basic "+base64.StdEncoding.EncodeToString(tok))

	parseRes, err := Parse(r.Header["Authorization"][0])
	if err != nil {
		t.Fatalf("error=%s\n", err)
	}

	actualOutput, err := Decrypt(parseRes)
	if err != nil {
		t.Fatalf("error=%s\n", err)
	}

	if actualOutput != ts.output {
		t.Fatalf("actual=%q expected=%q\n", actualOutput, ts.output)
	}
}

var parseTests = []struct{
	input string
	output string
}{
	{
		"Basic QmVjc3RzWVNrSlkzM1VzOTFrZ2w2cVB1Ykhvd1dYY3FhQnhxaHU3TnU2Xz06",
		"BecstsYSkJY33Us91kgl6qPubHowWXcqaBxqhu7Nu6_=",
	},
}

func TestParse(t *testing.T) {
	for _, ts := range parseTests {
		res, err := Parse(ts.input)
		if err != nil {
			t.Fatal(err)
		}
		if res != ts.output {
			t.Fatalf("acutal=%s expected=%s\n", res, ts.output)
		}
	}
}
