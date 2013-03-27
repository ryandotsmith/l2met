package main

import (
	"fmt"
	"io/ioutil"
	"l2met/bucket"
	"l2met/receiver"
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
	// The number of partitions that our backends support.
	numPartitions := utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
	// The bucket.Store struct will initialize a redis pool for us.
	maxRedisConn := utils.EnvInt("OUTLET_C", 2) + 100
	// We use the store to Put buckets into redis.
	server, pass, err := utils.ParseRedisUrl()
	if err != nil {
		log.Fatal(err)
	}
	store := bucket.NewStore(server, pass, numPartitions, maxRedisConn)

	// Initialize our receiver.
	recv := receiver.NewReceiver()
	recv.MaxOutbox = utils.EnvInt("REQUEST_BUFFER", 1000)
	recv.MaxInbox = utils.EnvInt("REQUEST_BUFFER", 1000)
	recv.FlushInterval = utils.EnvInt("FLUSH_INTERVAL", 1)
	recv.NumOutlets = utils.EnvInt("OUTLET_C", 2)
	recv.NumAcceptors = utils.EnvInt("ACCEPT_C", 2)
	recv.Store = store
	recv.Start()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		healthCheck(w, r, store)
	})

	http.HandleFunc("/logs", func(w http.ResponseWriter, r *http.Request) {
		recvLogs(w, r, recv)
	})

	port := utils.EnvString("PORT", "8000")
	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Printf("at=error error=\"Unable to start http server.\"\n")
		os.Exit(1)
	}
	fmt.Printf("at=start-l2met port=%s\n", port)
}

func healthCheck(w http.ResponseWriter, r *http.Request, s *bucket.Store) {
	ok := s.RedisHealth()
	if !ok {
		msg := "Redis is unavailable."
		fmt.Printf("error=%q\n", msg)
		http.Error(w, msg, 500)
	}
}

// Pull data from the http request, stick it in a channel and close the request.
// We don't do any validation on the data. Always respond with 200.
func recvLogs(w http.ResponseWriter, r *http.Request, recv *receiver.Receiver) {
	defer utils.MeasureT("http-receiver", time.Now())
	if r.Method != "POST" {
		http.Error(w, "Invalid Request", 400)
		return
	}
	token, err := utils.ParseToken(r)
	if err != nil {
		http.Error(w, "Invalid Request", 400)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		http.Error(w, "Invalid Request", 400)
		return
	}
	recv.Receive(token, b)
}
