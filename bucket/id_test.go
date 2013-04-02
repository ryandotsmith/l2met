package bucket

import (
	"fmt"
	"testing"
)

func TestParseId(t *testing.T) {
	expected, err := ParseId("1364858753→1000000000→123→hello→machine")
	if err != nil {
		t.FailNow()
	}
	if expected.Source != "machine" {
		fmt.Printf("expected=%d actual=%d\n", "machine", expected.Source)
		t.FailNow()
	}
	if expected.Name != "hello" {
		fmt.Printf("expected=%d actual=%d\n", "hello", expected.Name)
		t.FailNow()
	}
	if expected.Token != "123" {
		fmt.Printf("expected=%d actual=%d\n", "123", expected.Token)
		t.FailNow()
	}
	if expected.Resolution != 1000000000 {
		fmt.Printf("expected=%d actual=%d\n", 1000, expected.Resolution)
		t.FailNow()
	}
}
