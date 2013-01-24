package main

import (
	"bufio"
	"flag"
	"fmt"
	"l2met/store"
	"l2met/utils"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	workers        = flag.Int("workers", 2, "Number of workers to process the storing of metrics.")
	port           = flag.String("port", "8080", "Port for HTTP server to bind to.")
	registerLocker sync.Mutex
)

func init() {
	flag.Parse()
}

func main() {
	fmt.Printf("at=start-l2met port=%s\n", *port)
	register := make(map[string]*store.Bucket)
	iChan := make(chan *store.Bucket, 1000)
	oChan := make(chan *store.Bucket, 1000)
	for i := 0; i < *workers; i++ {
		go accept(iChan, &register)
		go transfer(&register, oChan)
		go outlet(oChan)
	}
	reciever := func(w http.ResponseWriter, r *http.Request) { recieveLogs(w, r, iChan) }
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {})
	http.HandleFunc("/logs", reciever)
	http.HandleFunc("/buckets", getBuckets)
	http.HandleFunc("/metrics", getMetrics)
	err := http.ListenAndServe(":"+*port, nil)
	if err != nil {
		fmt.Printf("at=error error=\"Unable to start http server.\"\n")
		os.Exit(1)
	}
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
	defer utils.MeasureT(time.Now(), "get-metrics")

	// Support CORS.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

	token, err := utils.ParseToken(r)
	if err != nil {
		fmt.Printf("at=error error=%q\n", err)
		errmsg := map[string]string{"error": "Missing authorization."}
		utils.WriteJson(w, 401, errmsg)
		return
	}

	q := r.URL.Query()
	limit, err := strconv.ParseInt(q.Get("limit"), 10, 32)
	if err != nil {
		errmsg := map[string]string{"error": "Missing limit parameter."}
		utils.WriteJson(w, 400, errmsg)
		return
	}

	resolution, err := strconv.ParseInt(q.Get("resolution"), 10, 32)
	if err != nil {
		errmsg := map[string]string{"error": "Missing resolution parameter."}
		utils.WriteJson(w, 400, errmsg)
		return
	}

	max := utils.RoundTime(time.Now(), (time.Minute * time.Duration(resolution)))
	min := max.Add(-1 * time.Minute * time.Duration(limit * resolution))

	metrics, err := store.GetMetrics(token, resolution, min, max)
	if err != nil {
		errmsg := map[string]string{"error": "Unable to find metrics."}
		utils.WriteJson(w, 500, errmsg)
		return
	}
	utils.WriteJson(w, 200, metrics)
}

func getBuckets(w http.ResponseWriter, r *http.Request) {
	defer utils.MeasureT(time.Now(), "get-buckets")

	if r.Method != "GET" {
		http.Error(w, "Invalid Request", 400)
		return
	}

	token, err := utils.ParseToken(r)
	if err != nil {
		errmsg := map[string]string{"error": "Missing authorization."}
		utils.WriteJson(w, 401, errmsg)
		return
	}

	q := r.URL.Query()
	limit, err := strconv.ParseInt(q.Get("limit"), 10, 32)
	if err != nil {
		errmsg := map[string]string{"error": "Missing limit parameter."}
		utils.WriteJson(w, 400, errmsg)
		return
	}

	max := utils.RoundTime(time.Now(), time.Minute)
	min := max.Add(-1 * time.Minute * time.Duration(limit))
	buckets, err := store.GetBuckets(token, min, max)
	if err != nil {
		errmsg := map[string]string{"error": "Unable to find buckets"}
		utils.WriteJson(w, 500, errmsg)
		return
	}
	utils.WriteJson(w, 200, buckets)
}

func recieveLogs(w http.ResponseWriter, r *http.Request, ch chan<- *store.Bucket) {
	defer utils.MeasureT(time.Now(), "http-receiver")
	if r.Method != "POST" {
		http.Error(w, "Invalid Request", 400)
		return
	}
	defer r.Body.Close()
	token, err := utils.ParseToken(r)
	if err != nil {
		utils.MeasureE("http-auth", err)
		http.Error(w, "Invalid Request", 400)
		return
	}
	defer utils.MeasureT(time.Now(), token+"-http-receive")

	buckets, err := store.NewBucket(token, bufio.NewReader(r.Body))
	if err != nil {
		http.Error(w, "Invalid Request", 400)
		return
	}

	for i := range buckets {
		ch <- buckets[i]
	}
}

func accept(ch <-chan *store.Bucket, register *map[string]*store.Bucket) {
	for m := range ch {
		k := m.String()
		var bucket *store.Bucket
		registerLocker.Lock()
		bucket, ok := (*register)[k]
		if !ok {
			(*register)[k] = m
		} else {
			bucket.Add(m)
		}
		registerLocker.Unlock()
	}
}

func transfer(register *map[string]*store.Bucket, ch chan<- *store.Bucket) {
	for _ = range time.Tick(time.Second) {
		for k := range *register {
			registerLocker.Lock()
			if m, ok := (*register)[k]; ok {
				ch <- m
				delete(*register, k)
			}
			registerLocker.Unlock()
		}
	}
}

func outlet(ch <-chan *store.Bucket) {
	for m := range ch {
		m.Put()
		fmt.Printf("at=put-bucket\n")
	}
}
