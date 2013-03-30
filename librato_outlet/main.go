package main

import (
	"l2met/outlet"
	"l2met/store"
	"l2met/utils"
	"log"
	"net"
	"net/http"
	"runtime"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// Set timeouts on all http connections.
// We don't want to wait forever on librato.
func init() {
	http.DefaultTransport = &http.Transport{
		DisableKeepAlives: true,
		Dial: func(n, a string) (net.Conn, error) {
			c, err := net.DialTimeout(n, a, time.Second*5)
			if err != nil {
				return c, err
			}
			return c, c.SetDeadline(time.Now().Add(time.Second * 7))
		},
	}
}

func main() {
	server, pass, err := utils.ParseRedisUrl()
	if err != nil {
		log.Fatal(err)
	}
	rs := store.NewRedisStore(server, pass, 1, 1024)

	rdr := outlet.NewBucketReader()
	rdr.NumScanners = 300
	rdr.NumOutlets = 300
	rdr.Store = rs
	rdr.Interval = time.Millisecond * 500

	l := outlet.NewLibratoOutlet()
	l.Reader = rdr
	l.NumOutlets = 300
	l.NumConverters = 300
	l.Retries = 2
	l.User = utils.EnvString("LIBRATO_USER", "")
	l.Pass = utils.EnvString("LIBRATO_TOKEN", "")
	l.Start()
	select {}
}
