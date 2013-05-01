package receiver

import (
	"l2met/bucket"
	"l2met/store"
	"testing"
	"strings"
	"time"
)

func makeReceiver() (store.Store, *Receiver) {
	st := store.NewMemStore()
	recv := NewReceiver(100, 1, time.Millisecond*5, st)
	return st, recv
}

func TestReceive(t *testing.T) {
	st, recv := makeReceiver()
	recv.Start()
	defer recv.Stop()

	opts := make(map[string][]string)
	opts["user"] = []string{"u"}
	opts["password"] = []string{"p"}
	msg := []byte("94 <190>1 2013-03-27T20:02:24+00:00 hostname token shuttle - - measure.hello=99 measure.world=100")
	recv.Receive(msg, opts)
	time.Sleep(recv.FlushInterval * 2)

	var helloBucket, worldBucket *bucket.Bucket
	ch, err := st.Scan()
	if err != nil {
		t.Error(err)
	}
	for b := range ch {
		if strings.Contains(b.Id.Name, "hello") {
			helloBucket = b
		}
		if strings.Contains(b.Id.Name, "world") {
			worldBucket = b
		}
	}

	if helloBucket.Id.Name != "hello" {
		t.Errorf("actual-name=%s expected-name=%s\n", helloBucket.Id.Name, "hello")
	}
	if worldBucket.Id.Name != "world" {
		t.Errorf("actual-name=%s expected-name=%s\n", worldBucket.Id.Name, "world")
	}

	if helloBucket.Sum() != 99 {
		t.Errorf("actual-sum=%s expected-sum=%s\n", helloBucket.Sum(), 99)
	}
	if worldBucket.Sum() != 100 {
		t.Errorf("actual-sum=%s expected-sum=%s\n", worldBucket.Sum(), 100)
	}
}

func TestReceiveOpts(t *testing.T) {
	st, recv := makeReceiver()
	recv.FlushInterval = time.Millisecond
	recv.Start()
	defer recv.Stop()

	opts := map[string][]string{"resolution": []string{"1"}}
	msg := []byte("81 <190>1 2013-03-27T00:00:01+00:00 hostname token shuttle - - measure=hello val=99\n")
	recv.Receive("user", "pass", msg, opts)
	time.Sleep(recv.FlushInterval * 2)

	var buckets []*bucket.Bucket
	ch, err := st.Scan()
	if err != nil {
		t.Error(err)
	}
	for b := range ch {
		buckets = append(buckets, b)
	}

	if len(buckets) != 1 {
		t.FailNow()
	}

	testBucket := buckets[0]

	expectedSum := float64(99)
	actualSum := testBucket.Sum()
	if actualSum != expectedSum {
		t.Errorf("actual=%d expected=%d\n", actualSum, expectedSum)
	}

	expectedSecond := 1
	actualSecond := testBucket.Id.Time.Second()
	if actualSecond != expectedSecond {
		t.Errorf("actual=%d expected=%d\n", actualSecond, expectedSecond)
	}
}

/*
func TestReceiveMultiMetrics(t *testing.T) {
	st, recv := makeReceiver()
	recv.FlushInterval = time.Millisecond
	recv.Start()
	defer recv.Stop()

	opts := map[string][]string{"resolution": []string{"1000"}}
	msg := []byte("95 <190>1 2013-03-27T00:00:01+00:00 hostname token shuttle - - measure=hello val=10 measure.db=10\n")
	recv.Receive("user", "pass", msg, opts)
	time.Sleep(recv.FlushInterval * 2)

	var buckets []*bucket.Bucket
	ch, err := st.Scan()
	if err != nil {
		t.Error(err)
	}
	for b := range ch {
		buckets = append(buckets, b)
	}

	expectedLength := 2
	actualLength := len(buckets)
	if actualLength != expectedLength {
		t.Errorf("expected=%d actual=%d\n", expectedLength, actualLength)
	}

	//the log line above has two measurements with values of 10.
	actualSum := float64(0)
	for i := range buckets {
		actualSum += buckets[i].Sum()
	}
	expectedSum := float64(20)
	if actualSum != expectedSum {
		t.Errorf("expected=%d actual=%d\n", expectedSum, actualSum)
	}
}

func TestReceiveRouter(t *testing.T) {
	st, recv := makeReceiver()
	recv.FlushInterval = time.Millisecond
	recv.Start()
	defer recv.Stop()

	opts := map[string][]string{"resolution": []string{"1"}}
	msg := []byte("113 <190>1 2013-03-27T00:00:01+00:00 shuttle heroku router - - host=test.l2met.net service=10ms connect=10ms bytes=45")
	recv.Receive("user", "pass", msg, opts)
	time.Sleep(recv.FlushInterval * 2)

	var buckets []*bucket.Bucket
	ch, err := st.Scan()
	if err != nil {
		t.Error(err)
	}
	for b := range ch {
		buckets = append(buckets, b)
	}

	expectedLength := 3
	actualLength := len(buckets)
	if actualLength != expectedLength {
		t.Errorf("expected=%d actual=%d\n", expectedLength, actualLength)
	}

	//the log line above has two measurements with values of 10.
	actualSum := float64(0)
	for i := range buckets {
		actualSum += buckets[i].Sum()
	}
	expectedSum := float64(65)
	if actualSum != expectedSum {
		t.Errorf("expected=%d actual=%d\n", expectedSum, actualSum)
	}

	//The name of the metric will be router.service, router.connect.
	//The source will include the host.
	expectedSrc := "test.l2met.net"
	for i := range buckets {
		actualSrc := buckets[i].Id.Source
		if actualSrc != expectedSrc {
			t.Errorf("expected=%s actual=%s\n", expectedSrc, actualSrc)
		}
	}
}
*/
