// An internal metrics channel.
// l2met internal components can publish their metrics
// here and they will be outletted to Librato.
package metchan

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ryandotsmith/l2met/bucket"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

var libratoUrl = "https://metrics-api.librato.com/v1/metrics"

// Convert l2met data into Librato's API format.
type libratoMetric struct {
	Name   string  `json:"name"`
	Time   int64   `json:"measure_time"`
	Source string  `json:"source"`
	Count  int     `json:"count"`
	Sum    float64 `json:"sum"`
	Max    float64 `json:"max"`
	Min    float64 `json:"min"`
}

// The channel for which internal metrics are organized.
type Channel struct {
	sync.Mutex
	verbose bool
	enabled bool
	buffer  map[bucket.Id]*bucket.Bucket
	outbox  chan *bucket.Bucket
}

func New(enabled, verbose bool) *Channel {
	c := new(Channel)
	c.enabled = enabled
	c.verbose = verbose
	c.buffer = make(map[bucket.Id]*bucket.Bucket)
	c.outbox = make(chan *bucket.Bucket, 10)
	go c.scheduleFlush()
	go c.outlet()
	return c
}

func (c *Channel) Measure(name string, t time.Time) {
	elapsed := time.Since(t) / time.Millisecond
	if c.verbose {
		fmt.Printf("measure.%s=%f\n", name, float64(elapsed))
	}
	if !c.enabled {
		return
	}
	b := &bucket.Bucket{
		Id: &bucket.Id{
			Time:       time.Now().Truncate(time.Minute),
			Resolution: time.Minute,
			Name:       name,
			Units:      "ms",
			// TODO(ryandotsmith):
			// Maybe we use the system's hostname?
			Source:     "metchan",
		},
		Vals: []float64{float64(elapsed)},
	}
	c.add(b)
}

func (c *Channel) add(b *bucket.Bucket) {
	c.Lock()
	defer c.Unlock()
	existing, present := c.buffer[*b.Id]
	if !present {
		c.buffer[*b.Id] = b
		return
	}
	existing.Add(b)
}

func (c *Channel) scheduleFlush() {
	for _ = range time.Tick(time.Second * 5) {
		c.flush()
	}
}

func (c *Channel) flush() {
	c.Lock()
	defer c.Unlock()
	for id, b := range c.buffer {
		c.outbox <- b
		delete(c.buffer, id)
	}
}

func (c *Channel) outlet() {
	for b := range c.outbox {
		met := &libratoMetric{
			Name:   b.Id.Name,
			Time:   b.Id.Time.Unix(),
			Source: b.Id.Source,
			Count:  b.Count(),
			Sum:    b.Sum(),
			Max:    b.Max(),
			Min:    b.Min(),
		}
		if err := post(met); err != nil {
			fmt.Printf("at=metchan-post error=%s\n", err)
		} else {
			fmt.Printf("at=metchan-post status=success\n")
		}
	}
}

func post(m *libratoMetric) error {
	payload := struct {
		Gauges []*libratoMetric `json:"gauges"`
	}{
		[]*libratoMetric{m},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", libratoUrl, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "l2met-metchan/0")
	req.SetBasicAuth("s@32k.io", "484c793b46dabbf72c24489f88793b65e05664b83e4b0ddce55b051822a008d0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var m string
		s, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			m = fmt.Sprintf("code=%d", resp.StatusCode)
		} else {
			m = fmt.Sprintf("code=%d resp=body=%s req-body=%s",
				resp.StatusCode, s, body)
		}
		return errors.New(m)
	}
	return nil
}
