package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"l2met/store"
	"l2met/utils"
	"net/http"
	"strconv"
	"time"
)

var (
	workers         = flag.Int("workers", 4, "Number of routines that will post data to librato")
	processInterval = flag.Int("proc-int", 5, "Number of seconds to wait in between bucket processing.")
)

func init() {
	flag.Parse()
}

type LM struct {
	Name   string `json:"name"`
	Time   int64  `json:"measure_time"`
	Val    string `json:"value"`
	Source string `json:"source,omitempty"`
	Token  string
}

type LP struct {
	Gauges *[]*LM `json:"gauges"`
}

var (
	libratoUrl = "https://metrics-api.librato.com/v1/metrics"
)

func main() {
	// The inbox is used to hold empty buckets that are
	// waiting to be processed. We buffer the chanel so
	// as not to slow down the fetch routine. We can
	inbox := make(chan *store.Bucket, 1000)
	// The converter will take items from the inbox,
	// fill in the bucket with the vals, then convert the
	// bucket into a librato metric.
	lms := make(chan *LM, 1000)
	// The converter will place the librato metrics into
	// the outbox for HTTP submission. We rely on the batch
	// routine to make sure that the collections of librato metrics
	// in the outbox are homogeneous with respect to their token.
	// This ensures that we route the metrics to the correct librato account.
	outbox := make(chan *[]*LM, 1000)

	// Print chan visibility.
	go report(inbox, lms, outbox)

	// Routine that reads ints from the database
	// and sends them to the inbox.
	go fetch(inbox)

	// We take the empty buckets from the inbox,
	// get the values from the database, then make librato metrics out of them.
	for i := 0; i < *workers; i++ {
		go convert(inbox, lms)
	}

	// Shouldn't need to be concurrent since it's responsibility
	// it to serialize a collection of librato metrics.
	go batch(lms, outbox)

	// These routines involve reading data from the database
	// and making HTTP requests. We will want to take advantage of
	// parallel processing.
	for i := 0; i < *workers; i++ {
		go post(outbox)
	}

	// Live forever.
	select {}
}

func report(i chan *store.Bucket, l chan *LM, o chan *[]*LM) {
	for _ = range time.Tick(time.Second * 5) {
		utils.MeasureI("librato.inbox", int64(len(i)))
		utils.MeasureI("librato.lms", int64(len(l)))
		utils.MeasureI("librato.outbox", int64(len(o)))
	}
}

func allBucketIds(min, max time.Time) ([]int64, error) {
	var buckets []int64
	startQuery := time.Now()
	r, err := pg.Query("select id from metrics where bucket >= $1 and bucket < $2 order by bucket desc",
		min, max)
	if err != nil {
		return nil, err
	}
	utils.MeasureT(startQuery, "metrics.query")
	startParse := time.Now()
	defer r.Close()
	for r.Next() {
		var id int64
		r.Scan(&id)
		buckets = append(buckets, id)
	}
	utils.MeasureT(startParse, "metrics.vals.parse")
	return buckets, nil
}

// Fetch should kick off the librato outlet process.
// Its responsibility is to get the ids of buckets for the current time,
// make empty Buckets, then place the buckets in an inbox to be filled
// (load the vals into the bucket) and processed.
func fetch(inbox chan<- *store.Bucket) {
	for t := range time.Tick(time.Duration(*processInterval) * time.Second) {
		fmt.Printf("at=start_fetch minute=%d\n", t.Minute())
		startPoll := time.Now()
		max := utils.RoundTime(t, time.Minute)
		min := max.Add(-time.Minute)
		ids, err := allBucketIds(min, max)
		if err != nil {
			utils.MeasureE("find-failed", err)
			return
		}
		for i := range ids {
			b := store.Bucket{Id: ids[i]}
			inbox <- &b
		}
		utils.MeasureT(startPoll, "librato.fetch")
	}
}

func convert(inbox <-chan *store.Bucket, lms chan<- *LM) {
	for b := range inbox {
		err := b.Get()
		if err != nil {
			fmt.Printf("error=%s\n", err)
			continue
		}
		if len(b.Vals) == 0 {
			fmt.Printf("at=bucket-no-vals name=%s\n", b.Name)
			continue
		}
		fmt.Printf("at=librato.process.bucket minute=%d name=%q\n",
			b.Time.Minute(), b.Name)
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".last", Val: ff(b.Last())}
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".min", Val: ff(b.Min())}
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".max", Val: ff(b.Max())}
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".mean", Val: ff(b.Mean())}
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".median", Val: ff(b.Median())}
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".perc95", Val: ff(b.P95())}
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".perc99", Val: ff(b.P99())}
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".count", Val: fi(b.Count())}
		lms <- &LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".sum", Val: ff(b.Sum())}
		utils.MeasureI("librato.convert", 1)
	}
}

func ff(x float64) string {
	return strconv.FormatFloat(x, 'f', 5, 64)
}

func fi(x int) string {
	return strconv.FormatInt(int64(x), 10)
}

func batch(lms <-chan *LM, outbox chan<- *[]*LM) {
	ticker := time.Tick(time.Second)
	batchMap := make(map[string]*[]*LM)
	for {
		select {
		case <-ticker:
			for k, v := range batchMap {
				if len(*v) > 0 {
					outbox <- v
					delete(batchMap, k)
				}
			}
		case lm := <-lms:
			k := lm.Token
			v, ok := batchMap[k]
			if !ok {
				tmp := make([]*LM, 0, 50)
				v = &tmp
				batchMap[k] = v
			}
			*v = append(*v, lm)
			if len(*v) == cap(*v) {
				outbox <- v
				delete(batchMap, k)
			}
		}
	}
}

func post(outbox <-chan *[]*LM) {
	for metrics := range outbox {
		if len(*metrics) < 1 {
			fmt.Printf("at=%q\n", "post.empty.metrics")
			continue
		}
		m := *metrics
		minute := time.Unix(m[0].Time, -1)
		fmt.Printf("at=start_post minute=%d\n", minute.Minute())
		token := store.Token{Id: m[0].Token}
		token.Get()
		payload := LP{metrics}
		j, err := json.Marshal(payload)
		postBody := bytes.NewBuffer(j)
		if err != nil {
			utils.MeasureE("librato.json", err)
			continue
		}
		req, err := http.NewRequest("POST", libratoUrl, postBody)
		if err != nil {
			continue
		}
		req.Header.Add("Content-Type", "application/json")
		req.SetBasicAuth(token.User, token.Pass)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			utils.MeasureE("librato-post", err)
			continue
		}
		if resp.StatusCode/100 != 2 {
			b, _ := ioutil.ReadAll(resp.Body)
			fmt.Printf("status=%d post-body=%s resp-body=%s\n",
				resp.StatusCode, postBody, b)
		}
		utils.MeasureI("librato.post", 1)
		resp.Body.Close()
	}
}
