package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"l2met/store"
	"l2met/utils"
	"log"
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
	workers        int
	port           string
	registerLocker sync.Mutex
	numPartitions  uint64
	reqBuffer      int
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var tmp string
	var err error

	tmp = os.Getenv("NUM_OUTLET_PARTITIONS")
	numPartitions, err = strconv.ParseUint(tmp, 10, 64)
	if err != nil {
		log.Fatal("Unable to read NUM_OUTLET_PARTITIONS")
	}

	port = os.Getenv("PORT")
	if len(port) == 0 {
		port = "8000"
	}

	workers = 2
	tmp = os.Getenv("LOCAL_WORKERS")
	if len(tmp) != 0 {
		n, err := strconv.Atoi(tmp)
		if err == nil {
			workers = n
		}
	}

	reqBuffer = 1000
	tmp = os.Getenv("REQUEST_BUFFER")
	if len(tmp) != 0 {
		n, err := strconv.Atoi(tmp)
		if err == nil {
			reqBuffer = n
		}
	}
}

type LogRequest struct {
	Token string
	Body  []byte
}

func main() {
	fmt.Printf("at=start-l2met port=%s\n", port)
	register := make(map[store.BKey]*store.Bucket)
	inbox := make(chan *LogRequest, 1000)
	outbox := make(chan *store.Bucket, 1000)

	go report(inbox, outbox, register)
	for i := 0; i < workers; i++ {
		go accept(inbox, register)
	}
	go transfer(register, outbox)
	for i := 0; i < workers; i++ {
		go outlet(outbox)
	}

	receiver := func(w http.ResponseWriter, r *http.Request) { receiveLogs(w, r, inbox) }
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {})
	http.HandleFunc("/logs", receiver)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Printf("at=error error=\"Unable to start http server.\"\n")
		os.Exit(1)
	}
}

func report(inbox chan *LogRequest, outbox chan *store.Bucket, register map[store.BKey]*store.Bucket) {
	for _ = range time.Tick(time.Second * 2) {
		utils.MeasureI("web.inbox", int64(len(inbox)))
		utils.MeasureI("web.register", int64(len(register)))
		utils.MeasureI("web.outbox", int64(len(outbox)))
	}
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

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Invalid Request", 400)
		r.Body.Close()
		return
	}
	r.Body.Close()
	inbox <- &LogRequest{token, b}
}

func accept(inbox <-chan *LogRequest, register map[store.BKey]*store.Bucket) {
	for lreq := range inbox {
		rdr := bufio.NewReader(bytes.NewReader(lreq.Body))
		for bucket := range store.NewBucket(lreq.Token, rdr) {
			registerLocker.Lock()
			k := bucket.Key
			_, present := register[k]
			if !present {
				fmt.Printf("at=%q minute=%d name=%s\n",
					"add-to-register", bucket.Key.Time.Minute(), bucket.Key.Name)
				register[k] = bucket
			} else {
				register[k].Add(bucket)
			}
			registerLocker.Unlock()
		}
	}
}

func transfer(register map[store.BKey]*store.Bucket, outbox chan<- *store.Bucket) {
	for _ = range time.Tick(time.Second * 2) {
		for k := range register {
			registerLocker.Lock()
			if m, ok := register[k]; ok {
				delete(register, k)
				registerLocker.Unlock()
				outbox <- m
			} else {
				registerLocker.Unlock()
			}
		}
	}
}

func outlet(outbox <-chan *store.Bucket) {
	for b := range outbox {
		err := b.Put(numPartitions)
		if err != nil {
			fmt.Printf("error=%s\n", err)
		}
	}
}
