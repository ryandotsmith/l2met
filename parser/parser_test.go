package parser

import (
	"bufio"
	"bytes"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/metchan"
	"testing"
)

type testCase struct {
	tname string
	in    string
	opts  options
	names []string
	metrics []string
}

var parseTest = []testCase{
	{
		"simple",
		`88 <174>1 2013-07-22T00:06:26-00:00 somehost name test - measure#hello=1 measure#world=1ms\n`,
		options{"auth": []string{"abc123"}},
		[]string{"hello", "world"},
		[]string{},
	},
	{
		"logplex L10",
		`162 <174>1 2013-04-17T19:04:46+00:00 d.1234-drain-identifier-567 heroku logplex - - Error L10 (output buffer overflow): 500 messages dropped since 2013-04-17T19:04:46+00:00.`,
		options{"auth": []string{"abc123"}},
		[]string{},
		[]string{"name=.logplex.l10 source= vals=[500]"},
	},
	{
		"legacy",
		`70 <174>1 2013-07-22T00:06:26-00:00 somehost name test - measure.hello=1\n`,
		options{"auth": []string{"abc123"}},
		[]string{"hello"},
		[]string{},
	},
}

func TestBuildBuckets(t *testing.T) {
	for _, tc := range parseTest {
		mchan := new(metchan.Channel)
		mchan.Enabled = true
		mchan.Buffer = make(map[string]*bucket.Bucket)
		body := bufio.NewReader(bytes.NewBufferString(tc.in))
		buckets := make([]*bucket.Bucket, 0)
		for b := range BuildBuckets(body, tc.opts, mchan) {
			buckets = append(buckets, b)
		}
		if len(tc.names) > 0 {
			testNames(t, buckets, tc)
		}
		if len(tc.metrics) > 0 {
			testMetrics(t, mchan, tc)
		}
	}
}

func testMetrics(t *testing.T, mc *metchan.Channel, tc testCase) {
	for _, met := range tc.metrics {
		found := false
		for _, b := range mc.Buffer {
			if b.String() == met {
				found = true
			}
		}
		if !found {
			t.Fatalf("actual-metrics=%v expected-metrics=%v\n",
				mc.Buffer, tc.metrics)
		}
	}
}

func testNames(t *testing.T, b []*bucket.Bucket, tc testCase) {
	if len(b) != len(tc.names) {
		t.Fatalf("test=%s actual-len=%d expected-len=%d\n",
			tc.tname, len(b), len(tc.names))
	}
	for i := range tc.names {
		if b[i].Id.Name != tc.names[i] {
			t.Fatalf("test=%s actual-name=%s expected-name=%s\n",
				tc.tname, tc.names[i], b[i].Id.Name)
		}
	}
}
