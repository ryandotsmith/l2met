package main

import (
	"bufio"
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
	port           string
	registerLocker sync.Mutex
)

func init() {
	port = os.Getenv("PORT")
	if len(port) == 0 {
		fmt.Printf("at=error error=\"$PORT not set.\"\n")
		os.Exit(1)
	}
}

func main() {
	fmt.Printf("at=start-l2met port=%s\n", port)
	register := make(map[string]*store.Bucket)
	iChan := make(chan *store.Bucket, 1000)
	oChan := make(chan *store.Bucket, 1000)
	for i := 0; i < 5; i++ {
		go accept(iChan, &register)
		go transfer(&register, oChan)
		go outlet(oChan)
	}
	reciever := func(w http.ResponseWriter, r *http.Request) { recieveLogs(w, r, iChan) }
	http.HandleFunc("/logs", reciever)
	http.HandleFunc("/query", findMetrics)
	http.HandleFunc("/buckets", getBucket)
	http.ListenAndServe(":"+port, nil)
}

func findMetrics(w http.ResponseWriter, r *http.Request) {
	defer utils.MeasureT(time.Now(), "query-bucket")
	q := r.URL.Query()
	limit, err := strconv.ParseInt(q.Get("limit"), 10, 32)
	if err != nil {
		errmsg := map[string]string{"error": "Missing limit parameter."}
		utils.WriteJson(w, 400, errmsg)
		return
	}
	max := utils.RoundTime(time.Now(), time.Minute)
	min := max.Add(-1 * time.Minute * time.Duration(limit))
	buckets, err := store.FindMetrics(min, max)
	if err != nil {
		errmsg := map[string]string{"error": "Unable to find buckets"}
		utils.WriteJson(w, 500, errmsg)
		return
	}
	utils.WriteJson(w, 200, buckets)
}

func getBucket(w http.ResponseWriter, r *http.Request) {
	defer utils.MeasureT(time.Now(), "get-bucket")
	q := r.URL.Query()
	id, err := strconv.ParseInt(q.Get("id"), 10, 64)
	if err != nil {
		errmsg := map[string]string{"error": "Missing bucket id."}
		utils.WriteJson(w, 400, errmsg)
		return
	}
	bucket := &store.Bucket{Id: id}
	b, ok := store.CacheGet(bucket)
	if ok {
		utils.WriteJsonBytes(w, 200, b)
		return
	}
	bucket.Get()
	store.CacheSet(bucket)
	utils.WriteJson(w, 200, bucket)
}

func recieveLogs(w http.ResponseWriter, r *http.Request, ch chan<- *store.Bucket) {
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
