package main

import (
	"fmt"
	"io/ioutil"
	"l2met/outlet"
	"l2met/receiver"
	"l2met/store"
	"l2met/utils"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var store *store.Store
	if usingRedis {
		// The number of partitions that our backends support.
		numPartitions := utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
		// The bucket.Store struct will initialize a redis pool for us.
		maxRedisConn := utils.EnvInt("OUTLET_C", 2) + 100
		// We use the store to Put buckets into redis.
		server, pass, err := utils.ParseRedisUrl()
		if err != nil {
			log.Fatal(err)
		}
		store = store.NewRedisStore(server, pass, numPartitions, maxRedisConn)
	} else {
		store := store.NewMemStore()
	}

	if startLibratoOutlet {
		concurrency := utils.EnvInt("LOCAL_WORKERS", 2)
		numPartitions := utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)

		rdr := outlet.NewBucketReader(1000)
		rdr.NumScanners = concurrency
		rdr.NumOutlets = concurrency
		rdr.Interval = time.Millisecond * 500
		rdr.Store = store

		l := outlet.NewLibratoOutlet(1000, 1000, 1000)
		l.Reader = rdr
		l.NumOutlets = concurrency
		l.NumConverters = concurrency
		l.Retries = 2
		l.User = utils.EnvString("LIBRATO_USER", "")
		l.Pass = utils.EnvString("LIBRATO_TOKEN", "")
		l.Start()
	}

	if startGraphiteOutlet {
		// The number of partitions that our backends support.
		numPartitions := utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
		// The bucket.Store struct will initialize a redis pool for us.
		maxRedisConn := utils.EnvInt("OUTLET_C", 2) + 100
		interval := time.Millisecond * 200
		rdr := &outlet.BucketReader{Store: store, Interval: interval}
		g := outlet.NewGraphiteOutlet(1000, 1000)
		g.Reader = rdr
		g.ApiToken = utils.EnvString("GRAPHITE_API_TOKEN", "")
		g.Start()
	}

	if startHttpOutlet {
		httpOutlet := new(outlet.HttpOutlet)
		httpOutlet.Store = rs
		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			httpOutlet.ServeReadBucket(w, r)
		})
	}

	if startReceiver {
		reqBuf := utils.EnvInt("REQUEST_BUFFER", 1000)
		recv := receiver.NewReceiver(reqBuf, reqBuf)
		recv.FlushInterval = time.Millisecond * 200
		recv.NumOutlets = utils.EnvInt("OUTLET_C", 100)
		recv.NumAcceptors = utils.EnvInt("ACCEPT_C", 100)
		recv.Store = rs
		recv.Start()
		if verbose {
			go recv.Report()
		}
		http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
			recvLogs(w, r, recv)
		})
	}

	//The only thing that constitutes a healthy l2met
	//is the health of the store. In some cases, this might mean
	//a redis health check.
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		healthCheck(w, r, rs)
		ok := store.Health()
		if !ok {
			msg := "Redis is unavailable."
			fmt.Printf("error=%q\n", msg)
			http.Error(w, msg, 500)
		}
	})


	//Start the HTTP server.
	port := utils.EnvString("PORT", "8000")
	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Printf("error=%s msg=%q\n", err, "Unable to start http server.")
		os.Exit(1)
	}
	fmt.Printf("at=l2met-initialized port=%s\n", port)
}

// Pull data from the http request, stick it in a channel and close the request.
// We don't do any validation on the data. Always respond with 200.
func recvLogs(w http.ResponseWriter, r *http.Request, recv *receiver.Receiver) {
	defer utils.MeasureT("http-receiver", time.Now())
	if r.Method != "POST" {
		http.Error(w, "Invalid Request", 400)
		return
	}
	user, pass, err := utils.ParseAuth(r)
	if err != nil {
		fmt.Printf("measure.failed-auth erro=%s user=%s pass=%s user-agent=%s token=%s client=%s\n",
			err, user, pass, r.Header.Get("User-Agent"), r.Header.Get("Logplex-Drain-Token"), r.Header.Get("X-Forwarded-For"))
		http.Error(w, "Invalid Request", 400)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		http.Error(w, "Invalid Request", 400)
		return
	}
	recv.Receive(user, pass, b, r.URL.Query())
}
