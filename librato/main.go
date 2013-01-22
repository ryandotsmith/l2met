package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"l2met/db"
	"l2met/store"
	"l2met/utils"
	"net/http"
	"strconv"
	"time"
)

var (
	workers = flag.Int("workers", 4, "Number of routines that will post data to librato")
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
	Gauges []LM `json:"gauges"`
}

var (
	libratoUrl = "https://metrics-api.librato.com/v1/metrics"
)

func main() {
	lms := make(chan LM)
	inbox := make(chan *store.Bucket)
	outbox := make(chan []LM)

	go fetch(inbox)
	go batch(lms, outbox)
	for i := 0; i < *workers; i++ {
		go convert(inbox, lms)
		go post(outbox)
	}
	select {}
}

func allBucketIds(min, max time.Time) ([]int64, error) {
	var buckets []int64
	startQuery := time.Now()
	r, err := db.PGR.Query("select id from metrics where bucket >= $1 and bucket < $2 order by bucket desc",
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

func fetch(out chan<- *store.Bucket) {
	for _ = range time.Tick(time.Second * 10) {
		startPoll := time.Now()
		max := utils.RoundTime(time.Now(), time.Minute)
		min := max.Add(-time.Minute)
		ids, err := allBucketIds(min, max)
		if err != nil {
			utils.MeasureE("find-failed", err)
			continue
		}
		for i := range ids {
			b := store.Bucket{Id: ids[i]}
			out <- &b
		}
		utils.MeasureT(startPoll, "librato.fetch")
	}
}

func convert(in <-chan *store.Bucket, out chan<- LM) {
	for b := range in {
		b.Get()
		out <- LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".last", Val: ff(b.Last())}
		out <- LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".min", Val: ff(b.Min())}
		out <- LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".max", Val: ff(b.Max())}
		out <- LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".mean", Val: ff(b.Mean())}
		out <- LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".median", Val: ff(b.Median())}
		out <- LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".perc95", Val: ff(b.P95())}
		out <- LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".perc99", Val: ff(b.P99())}
		out <- LM{Token: b.Token, Time: b.Time.Unix(), Source: b.Source, Name: b.Name + ".count", Val: fi(b.Count())}
	}
}

func ff(x float64) string {
	return strconv.FormatFloat(x, 'f', 5, 64)
}

func fi(x int) string {
	return strconv.FormatInt(int64(x), 10)
}

func batch(in <-chan LM, out chan<- []LM) {
	ticker := time.Tick(time.Second)
	batchMap := make(map[string]*[]LM)
	for {
		select {
		case <-ticker:
			for k, v := range batchMap {
				if len(*v) > 0 {
					out <- *v
					delete(batchMap, k)
				}
			}
		case lm := <-in:
			k := lm.Token
			v, ok := batchMap[k]
			if !ok {
				tmp := make([]LM, 0, 50)
				v = &tmp
				batchMap[k] = v
			}
			*v = append(*v, lm)
			if len(*v) == cap(*v) {
				out <- *v
				delete(batchMap, k)
			}
		}
	}
}

func post(in <-chan []LM) {
	for metrics := range in {
		if len(metrics) < 1 {
			fmt.Printf("at=%q\n", "post.empty.metrics")
			continue
		}
		token := store.Token{Id: metrics[0].Token}
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
		resp.Body.Close()
	}
}
