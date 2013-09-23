package store

import (
	"errors"
	"github.com/ryandotsmith/l2met/bucket"
	"net/http"
	"sync"
	"time"
)

type MemStore struct {
	sync.Mutex
	m map[bucket.Id]*bucket.Bucket
}

func NewMemStore() *MemStore {
	return &MemStore{m: make(map[bucket.Id]*bucket.Bucket)}
}

func (s *MemStore) Health() bool {
	return true
}

func (m *MemStore) MaxPartitions() uint64 {
	return uint64(1)
}

func (m *MemStore) Now() time.Time {
	return time.Now()
}

func (m *MemStore) Scan(schedule time.Time, out chan<- *bucket.Bucket) error {
	m.Lock()
	defer m.Unlock()
	for k, v := range m.m {
		ready := v.Id.Time.Add(v.Id.Resolution).Add(time.Second)
		if !ready.After(schedule) {
			delete(m.m, k)
			out <- v
		}
	}
	return  nil
}

func (m *MemStore) Get(b *bucket.Bucket) error {
	m.Lock()
	defer m.Unlock()
	bucket, present := m.m[*b.Id]
	if !present {
		return errors.New("Bucket not in MemStore.")
	}
	b = bucket
	return nil
}

func (m *MemStore) Put(b *bucket.Bucket) error {
	m.Lock()
	defer m.Unlock()
	if _, present := m.m[*b.Id]; !present {
		m.m[*b.Id] = b
	} else {
		m.m[*b.Id].Merge(b)
	}
	return nil
}

func (m *MemStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	return
}
