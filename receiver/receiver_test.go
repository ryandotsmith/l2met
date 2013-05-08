package receiver

import (
	"fmt"
	"l2met/bucket"
	"l2met/store"
	"testing"
	"time"
)

type testOps map[string][]string

func TestReceiver(t *testing.T) {
	currentTime := time.Now()
	opts := testOps{"resolution": []string{"1"}, "user": []string{"u"}, "password": []string{"p"}}
	cases := []struct {
		Opts    testOps
		LogLine []byte
		Buckets []*bucket.Bucket
	}{
		{
			opts,
			fmtLog(currentTime, "router", "host=l2met.net connect=1ms service=4ms bytes=10"),
			[]*bucket.Bucket{
				testBucket("router.connect", "l2met.net", "u", "p", time.Minute, []float64{1}),
				testBucket("router.service", "l2met.net", "u", "p", time.Minute, []float64{4}),
				testBucket("router.bytes", "l2met.net", "u", "p", time.Minute, []float64{10}),
			},
		},
		{
			opts,
			fmtLog(currentTime, "app", "measure.a"),
			[]*bucket.Bucket{testBucket("a", "", "u", "p", time.Minute, []float64{1})},
		},
		{
			opts,
			fmtLog(currentTime, "app", "measure.a"),
			[]*bucket.Bucket{testBucket("a", "", "u", "p", time.Second, []float64{1})},
		},
		{
			opts,
			fmtLog(currentTime, "app", "measure.a=1"),
			[]*bucket.Bucket{testBucket("a", "", "u", "p", time.Minute, []float64{1})},
		},
		{
			opts,
			fmtLog(currentTime, "app", "measure.a=1 measure.b=2"),
			[]*bucket.Bucket{
				testBucket("a", "", "u", "p", time.Minute, []float64{1}),
				testBucket("b", "", "u", "p", time.Minute, []float64{2}),
			},
		},
	}

	for i := range cases {
		match := 0
		actual, err := receiveInput(cases[i].Opts, cases[i].LogLine)
		if err != nil {
			t.Errorf("error=%s\n", err)
		}
		expected := cases[i].Buckets
		for j := range expected {
			for k := range actual {
				if bucketsEqual(actual[k], expected[j], t) {
					match++
				}
			}
		}
		if match != len(expected) {
			t.Fatalf("Expected buckets to match.")
		}
	}
}

func testBucket(name, source, user, pass string, res time.Duration, vals []float64) *bucket.Bucket {
	id := new(bucket.Id)
	id.Name = name
	id.Source = source
	id.User = user
	id.Pass = pass
	id.Resolution = res
	return &bucket.Bucket{Id: id, Vals: vals}
}

func fmtLog(t time.Time, appname, msg string) []byte {
	prival := 190 //local7/info
	version := 1
	timestamp := t.Format("2006-01-02T15:04:05+00:00")
	hostname := "hostname"
	msgid := "-"
	procid := "-"
	layout := "<%d>%d %s %s %s %s %s %s"
	packet := fmt.Sprintf(layout,
		prival, version, timestamp, hostname, appname, procid, msgid, msg)
	result := fmt.Sprintf("%d %s", len(packet), packet)
	return []byte(result)
}

func receiveInput(opts testOps, msg []byte) ([]*bucket.Bucket, error) {
	st := store.NewMemStore()
	recv := NewReceiver(100, 1, time.Millisecond*5, st)
	recv.Start()
	defer recv.Stop()

	recv.Receive(msg, opts)
	time.Sleep(time.Second)

	ch, err := st.Scan(time.Now())
	if err != nil {
		return nil, err
	}
	buckets := make([]*bucket.Bucket, 0)
	for b := range ch {
		buckets = append(buckets, b)
	}
	return buckets, nil
}

func bucketsEqual(actual, expected *bucket.Bucket, t *testing.T) bool {
	if actual.Id.Name != expected.Id.Name {
		t.Logf("actual-name=%s expected-name=%s\n", actual.Id.Name, expected.Id.Name)
		return false
	}
	if actual.Id.Source != expected.Id.Source {
		t.Logf("actual-source=%s expected-source=%s\n", actual.Id.Source, expected.Id.Source)
		return false
	}
	if actual.Sum() != expected.Sum() {
		t.Logf("actual-sum=%f expected-sum=%f\n", actual.Sum(), expected.Sum())
		return false
	}
	return true
}
