package bucket

import (
	"fmt"
	"testing"
	"time"
)

func TestParseId(t *testing.T) {
	id := Id{time.Now(), time.Minute, "token", "name", "source"}
	expected, err := ParseId(id.String())
	if err != nil {
		t.FailNow()
	}
	if expected.Source != "source" {
		fmt.Printf("expected=%d actual=%d\n", "source", expected.Source)
		t.FailNow()
	}
	if expected.Name != "name" {
		fmt.Printf("expected=%d actual=%d\n", "name", expected.Name)
		t.FailNow()
	}
	if expected.Token != "token" {
		fmt.Printf("expected=%d actual=%d\n", "token", expected.Token)
		t.FailNow()
	}
	if expected.Resolution != time.Minute {
		fmt.Printf("expected=%d actual=%d\n", time.Minute, expected.Resolution)
		t.FailNow()
	}
}

func TestParseIdWithoutSource(t *testing.T) {
	id := Id{time.Now(), time.Minute, "token", "name", ""}
	expected, err := ParseId(id.String())
	if err != nil {
		t.FailNow()
	}
	if expected.Name != "name" {
		fmt.Printf("expected=%d actual=%d\n", "name", expected.Name)
		t.FailNow()
	}
	if expected.Token != "token" {
		fmt.Printf("expected=%d actual=%d\n", "token", expected.Token)
		t.FailNow()
	}
	if expected.Resolution != time.Minute {
		fmt.Printf("expected=%d actual=%d\n", time.Minute, expected.Resolution)
		t.FailNow()
	}
}
