package receiver

import (
	"testing"
	"l2met/bucket"
	"sync"
	"time"
	"errors"
)

type InMemStore struct {
	sync.Mutex
	m map[bucket.Id]*bucket.Bucket
}

func NewInMemStore() *InMemStore {
	return &InMemStore{m: make(map[bucket.Id]*bucket.Bucket)}
}

func (m *InMemStore) Scan(partition string) chan *bucket.Bucket {
	m.Lock()
	buckets := make(chan *bucket.Bucket, 1000)
	go func(out chan *bucket.Bucket) {
		defer close(out)
		for _, bucket := range m.m {
			out <- bucket
		}
	}(buckets)
	return buckets
}

func (m *InMemStore) Get(b *bucket.Bucket) error {
	m.Lock()
	bucket, present := m.m[*b.Id]
	if !present {
		return errors.New("Bucket not in InMemStore.")
	}
	b = bucket
	m.Unlock()
	return nil
}

func (m *InMemStore) Put(b *bucket.Bucket) error {
	m.Lock()
	m.m[*b.Id] = b
	m.Unlock()
	return nil
}

func TestReceive(t *testing.T) {
	store := NewInMemStore()

	recv := NewReceiver()
	recv.MaxOutbox = 100
	recv.MaxInbox = 100
	recv.FlushInterval = 1
	recv.NumOutlets = 2
	recv.NumAcceptors = 2
	recv.Store = store
	recv.Start()

	msg := []byte("81 <190>1 2013-03-27T20:02:24+00:00 hostname token shuttle - - measure=hello val=99\n")
	recv.Receive("123", msg)

	time.Sleep(time.Second * 2)

	var buckets []*bucket.Bucket
	for bucket := range store.Scan("not important") {
		buckets = append(buckets, bucket)
	}

	if len(buckets) != 1 {
		t.FailNow()
	}

	testBucket := buckets[0]
	if testBucket.Id.Name != "hello" {
		t.FailNow()
	}

	if testBucket.Sum() != 99 {
		t.FailNow()
	}
}
