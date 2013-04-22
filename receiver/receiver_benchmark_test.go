package receiver

import (
	"l2met/store"
	"testing"
	"time"
)

func BenchmarkReceive(b *testing.B) {
	b.StopTimer()

	store := store.NewMemStore()
	maxOutbox := 100
	maxInbox := 100
	recv := NewReceiver(maxInbox, maxOutbox)
	recv.NumOutlets = 10
	recv.NumAcceptors = 10
	recv.Store = store
	recv.FlushInterval = time.Millisecond
	recv.Start()
	defer recv.Stop()

	opts := map[string][]string{}
	msg := []byte("81 <190>1 2013-03-27T20:02:24+00:00 hostname token shuttle - - measure=hello val=99\n")

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		recv.Receive("user", "pass", msg, opts)
		b.StopTimer()
		b.SetBytes(int64(len(msg)))
	}
}
