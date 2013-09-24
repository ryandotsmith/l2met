package main

import (
	"fmt"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/receiver"
	"github.com/ryandotsmith/l2met/store"
	"testing"
	"time"
)

func BenchmarkReceive(b *testing.B) {
	b.StopTimer()

	mchan := metchan.New(cfg)
	mchan.Start()

	st := store.NewRedisStore(cfg)
	st.Mchan = mchan

	recv := receiver.NewReceiver(cfg, st)
	recv.Mchan = mchan
	recv.Start()

	opts := make(map[string][]string)
	opts["user"] = []string{"u"}
	opts["password"] = []string{"p"}

	tf := "2013-03-27T20:02:24+00:00"
	bmsg := "94 <190>1 %s hostname token shuttle - - measure#hello=1"

	for i := 0; i < b.N; i++ {
		msg := fmt.Sprintf(bmsg, time.Now().Format(tf))
		b.StartTimer()
		recv.Receive([]byte(msg), opts)
		b.StopTimer()
		b.SetBytes(int64(len(msg)))
	}
	recv.Wait()
}
