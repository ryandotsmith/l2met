package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"l2met/store"
	"l2met/utils"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"
)

var (
	metricsPat     = regexp.MustCompile(`\/metrics\/(.*)\??`)
	workers        = flag.Int("workers", 2, "Number of workers to process the storing of metrics.")
	port           = flag.String("port", "8080", "Port for HTTP server to bind to.")
	registerLocker sync.Mutex
)

func init() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
}

type LogRequest struct {
	Token string
	Body  *bytes.Reader
}

func main() {
	fmt.Printf("at=start-l2met port=%s\n", *port)
	register := make(map[store.BKey]*store.Bucket)
	inbox := make(chan *LogRequest, 10000)
	outbox := make(chan *store.Bucket, 10000)

	go report(inbox, outbox)
	for i := 0; i < *workers; i++ {
		go accept(inbox, register)
	}
	go transfer(register, outbox)
	for i := 0; i < *workers; i++ {
		go outlet(outbox)
	}

	receiver := func(w http.ResponseWriter, r *http.Request) { receiveLogs(w, r, inbox) }
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {})
	http.HandleFunc("/logs", receiver)
	http.HandleFunc("/buckets", getBuckets)
	http.HandleFunc("/metrics/", getMetrics)
	err := http.ListenAndServe(":"+*port, nil)
	if err != nil {
		fmt.Printf("at=error error=\"Unable to start http server.\"\n")
		os.Exit(1)
	}
}

func report(inbox chan *LogRequest, outbox chan *store.Bucket) {
	for _ = range time.Tick(time.Second * 5) {
		utils.MeasureI("web.inbox", int64(len(inbox)))
		utils.MeasureI("web.outbox", int64(len(outbox)))
	}
}

func getMetrics(w http.ResponseWriter, r *http.Request) {
	defer utils.MeasureT(time.Now(), "get-metrics")

	// Support CORS.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")

	names := metricsPat.FindStringSubmatch(r.URL.Path)
	if len(names) < 2 {
		fmt.Printf("at=error error=%q\n", "Name parameter not provided.")
		errmsg := map[string]string{"error": "Name parameter not provided."}
		utils.WriteJson(w, 401, errmsg)
		return
	}
	name := names[1]

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
	min := max.Add(-1 * time.Minute * time.Duration(limit*resolution))

	metrics, err := store.GetMetrics(token, name, resolution, min, max)
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

func receiveLogs(w http.ResponseWriter, r *http.Request, inbox chan<- *LogRequest) {
	defer utils.MeasureT(time.Now(), "http-receiver")
	if r.Method != "POST" {
		http.Error(w, "Invalid Request", 400)
		return
	}
	token, err := utils.ParseToken(r)
	if err != nil {
		utils.MeasureE("http-auth", err)
		http.Error(w, "Invalid Request", 400)
		return
	}
	defer utils.MeasureT(time.Now(), token+"-http-receive")
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid Request", 400)
		return
	}
	inbox <- &LogRequest{token, bytes.NewReader(b)}
}

func accept(inbox <-chan *LogRequest, register map[store.BKey]*store.Bucket) {
	for lreq := range inbox {
		rdr := bufio.NewReader(lreq.Body)
		for bucket := range store.NewBucket(lreq.Token, rdr) {
			registerLocker.Lock()
			k := store.BKey{bucket.Time, bucket.Name, bucket.Source}
			_, present := register[k]
			if !present {
				fmt.Printf("at=%q minute=%d name=%s\n",
					"add-to-register", bucket.Time.Minute(), bucket.Name)
				register[k] = bucket
			} else {
				register[k].Add(bucket)
			}
			registerLocker.Unlock()
		}
	}
}

func transfer(register map[store.BKey]*store.Bucket, outbox chan<- *store.Bucket) {
	for _ = range time.Tick(time.Second) {
		for k := range register {
			registerLocker.Lock()
			if m, ok := register[k]; ok {
				outbox <- m
				delete(register, k)
			}
			registerLocker.Unlock()
		}
	}
}

func outlet(outbox <-chan *store.Bucket) {
	for b := range outbox {
		err := b.Put()
		if err != nil {
			fmt.Printf("error=%s\n", err)
		}
	}
}
