package receiver

import (
	"fmt"
	"github.com/ryandotsmith/l2met/conf"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/store"
	"net/url"
	"testing"
	"time"
)

func BenchmarkReceive(b *testing.B) {
	b.StopTimer()
	st := store.NewMemStore()
	cfg := &conf.D{
		Concurrency:   1,
		BufferSize:    1,
		FlushInterval: time.Second,
	}
	recv := NewReceiver(cfg, st)
	u, _ := url.Parse("https://u:p@test.com/")
	recv.Mchan = metchan.New(false, u)
	recv.Start()
	defer recv.Stop()

	opts := make(map[string][]string)
	opts["user"] = []string{"u"}
	opts["password"] = []string{"p"}

	tf := "2013-03-27T20:02:24+00:00"
	bmsg := "94 <190>1 %s hostname token shuttle - - measure.hello=1"

	for i := 0; i < b.N; i++ {
		msg := fmt.Sprintf(bmsg, time.Now().Format(tf))
		b.StartTimer()
		recv.Receive([]byte(msg), opts)
		b.StopTimer()
		b.SetBytes(int64(len(msg)))
	}
}
