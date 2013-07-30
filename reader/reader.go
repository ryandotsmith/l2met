// The reader pkg is responsible for reading data from
// the store, building buckets from the data, and placing
// the buckets into a user-supplied channel.
package reader

import (
	"fmt"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/store"
	"time"
)

type Reader struct {
	Store       store.Store
	Interval    time.Duration
	Partition   string
	Ttl         uint64
	NumOutlets  int
	NumScanners int
	Inbox       chan *bucket.Bucket
	Outbox      chan *bucket.Bucket
	Mchan       *metchan.Channel
}

func New(sz, c int, i time.Duration, st store.Store) *Reader {
	rdr := new(Reader)
	rdr.Partition = "bucket-reader"
	rdr.Inbox = make(chan *bucket.Bucket, sz)
	rdr.NumScanners = c
	rdr.NumOutlets = c
	rdr.Interval = i
	rdr.Store = st
	return rdr
}

func (r *Reader) Start(out chan *bucket.Bucket) {
	r.Outbox = out
	go r.scan()
	for i := 0; i < r.NumOutlets; i++ {
		go r.outlet()
	}
}

func (r *Reader) scan() {
	for t := range time.Tick(r.Interval) {
		startScan := time.Now()
		buckets, err := r.Store.Scan(t)
		if err != nil {
			fmt.Printf("at=bucket.scan error=%s\n", err)
			continue
		}
		for b := range buckets {
			r.Inbox <- b
		}
		r.Mchan.Measure("reader.scan", startScan)
	}
}

func (r *Reader) outlet() {
	for b := range r.Inbox {
		startGet := time.Now()
		r.Store.Get(b)
		r.Outbox <- b
		r.Mchan.Measure("reader.get", startGet)
	}
}
