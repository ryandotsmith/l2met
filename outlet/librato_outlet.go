// The outlet pkg is responsible for taking
// buckets from the reader, formatting them in the Librato format
// and delivering the formatted librato metrics to Librato's API.
package outlet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ryandotsmith/l2met/bucket"
	"github.com/ryandotsmith/l2met/metchan"
	"github.com/ryandotsmith/l2met/reader"
	"github.com/ryandotsmith/l2met/utils"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"time"
)

var libratoUrl = "https://metrics-api.librato.com/v1/metrics"

var httpClient *http.Client

func init() {
	tr := &http.Transport{
		DisableKeepAlives: true,
		Dial: func(n, a string) (net.Conn, error) {
			c, err := net.DialTimeout(n, a, time.Second*2)
			if err != nil {
				return c, err
			}
			return c, c.SetDeadline(time.Now().Add(time.Second * 2))
		},
	}
	httpClient = &http.Client{Transport: tr}
}

type libratoRequest struct {
	Gauges []*bucket.LibratoMetric `json:"gauges"`
}

type LibratoOutlet struct {
	// The inbox will be passed to the reader so
	// it (the reader) can deliver buckets to the outlet.
	// Buckets delivered in the outlet's inbox are complete.
	inbox chan *bucket.Bucket

	// The converter will take items from the inbox,
	// fill in the bucket with the vals, then convert the
	// bucket into a librato metric.
	conversions chan *bucket.LibratoMetric
	// The converter will place the librato metrics into
	// the outbox for HTTP submission. We rely on the batch
	// routine to make sure that the collections of librato metrics
	// in the outbox are homogeneous with respect to their token.
	// Ensures that we route the metrics to the correct librato account.
	outbox chan []*bucket.LibratoMetric
	// How many outlet routines should be running.
	numOutlets int
	// We use the Reader to read buckets from the store into our Inbox.
	rdr *reader.Reader
	// Number of times to retry HTTP requests to librato's api.
	numRetries int
	Mchan      *metchan.Channel
}

func NewLibratoOutlet(sz, conc, retries int, r *reader.Reader) *LibratoOutlet {
	l := new(LibratoOutlet)
	l.inbox = make(chan *bucket.Bucket, sz)
	l.conversions = make(chan *bucket.LibratoMetric, sz)
	l.outbox = make(chan []*bucket.LibratoMetric, sz)
	l.numOutlets = conc
	l.numRetries = retries
	l.rdr = r
	return l
}

func (l *LibratoOutlet) Start() {
	go l.rdr.Start(l.inbox)
	// Converting is CPU bound as it reads from memory
	// then computes statistical functions over an array.
	for i := 0; i < runtime.NumCPU(); i++ {
		go l.convert()
	}
	go l.groupByUser()
	for i := 0; i < l.numOutlets; i++ {
		go l.outlet()
	}
}

func (l *LibratoOutlet) Report() {
	for _ = range time.Tick(time.Second * 2) {
		utils.MeasureI("librato-outlet.inbox", len(l.inbox))
		utils.MeasureI("librato-outlet.conversions", len(l.conversions))
		utils.MeasureI("librato-outlet.outbox", len(l.outbox))
	}
}

func (l *LibratoOutlet) convert() {
	for bucket := range l.inbox {
		for _, m := range bucket.Metrics() {
			l.conversions <- m
		}
		fmt.Printf("measure.bucket.conversion.delay=%d\n",
			bucket.Id.Delay(time.Now()))
	}
}

func (l *LibratoOutlet) groupByUser() {
	ticker := time.Tick(time.Millisecond * 200)
	m := make(map[string][]*bucket.LibratoMetric)
	for {
		select {
		case <-ticker:
			for k, v := range m {
				if len(v) > 0 {
					l.outbox <- v
				}
				delete(m, k)
			}
		case payload := <-l.conversions:
			usr := payload.User + ":" + payload.Pass
			_, present := m[usr]
			if !present {
				m[usr] = make([]*bucket.LibratoMetric, 1, 300)
				m[usr][0] = payload
			} else {
				m[usr] = append(m[usr], payload)
			}
			if len(m[usr]) == cap(m[usr]) {
				l.outbox <- m[usr]
				delete(m, usr)
			}
		}
	}
}

func (l *LibratoOutlet) outlet() {
	for payloads := range l.outbox {
		if len(payloads) < 1 {
			fmt.Printf("at=%q\n", "empty-metrics-error")
			continue
		}
		//Since a playload contains all metrics for
		//a unique librato user/pass, we can extract the user/pass
		//from any one of the payloads.
		user := payloads[0].User
		pass := payloads[0].Pass
		libratoReq := &libratoRequest{payloads}
		j, err := json.Marshal(libratoReq)
		if err != nil {
			fmt.Printf("at=json error=%s user=%s\n", err, user)
			continue
		}
		if err := l.postWithRetry(user, pass, j); err != nil {
			fmt.Printf("measure.outlet.drop user=%s\n", user)
		}
	}
}

func (l *LibratoOutlet) postWithRetry(u, p string, body []byte) error {
	for i := 0; i <= l.numRetries; i++ {
		if err := l.post(u, p, body); err != nil {
			fmt.Printf("measure.librato.error user=%s msg=%s attempt=%d\n", u, err, i)
			if i == l.numRetries {
				return err
			}
			continue
		}
		return nil
	}
	//Should not be possible.
	return errors.New("Unable to post.")
}

func (l *LibratoOutlet) post(u, p string, body []byte) error {
	defer l.Mchan.Measure("outlet.post", time.Now())
	b := bytes.NewBuffer(body)
	req, err := http.NewRequest("POST", libratoUrl, b)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "l2met/0")
	req.SetBasicAuth(u, p)
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var m string
		s, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			m = fmt.Sprintf("error=failed-request code=%d", resp.StatusCode)
		} else {
			m = fmt.Sprintf("error=failed-request code=%d resp=body=%s req-body=%s",
				resp.StatusCode, s, body)
		}
		return errors.New(m)
	}
	return nil
}
