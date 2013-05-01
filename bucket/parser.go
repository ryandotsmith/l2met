package bucket

import (
	"fmt"
	"bufio"
	"bytes"
	"errors"
	"github.com/ryandotsmith/lpx"
	"github.com/kr/logfmt"
	"time"
	"strings"
	"strconv"
)

type Options map[string][]string

type measurement struct {
	Key    string
	Val    float64
	Source string
	Unit   string
}

type measurements []*measurement

func (mm *measurements) HandleLogfmt(key, val []byte) error {
	i := bytes.LastIndexFunc(val, isDigit)
	v, err := strconv.ParseFloat(string(val[:i+1]), 10)
	if err != nil {
		return err
	}
	m := &measurement{
		Key: string(key),
		Val:  v,
		Unit: string(val[i+1:]),
	}
	*mm = append(*mm, m)
	return nil
}

// return true if r is an ASCII digit only, as opposed to unicode.IsDigit.
func isDigit(r rune) bool {
	return '0' <= r && r <= '9'
}


var parseError = errors.New("Unable to parse message.")

var (
	routerPrefix  = "router"
	measurePrefix = "measure."
)

func NewBuckets(body *bufio.Reader, opts Options) <-chan *Bucket {
	rdr := lpx.NewReader(body)
	out := make(chan *Bucket)
	go func(o chan *Bucket) {
		defer close(o)
		for rdr.Next() {
			header := rdr.Header()
			msg := rdr.Bytes()
			parseHkRouter(o, opts, header, msg)
			parseMeasurements(o, opts, header, msg)
		}
	}(out)
	return out
}

func parseMeasurements(out chan *Bucket, opts Options, header *lpx.Header, msg []byte) error {
	mm := make(measurements, 0)
	if err := logfmt.Unmarshal(msg, &mm); err != nil {
		fmt.Printf("error=%s msg=%s\n", err, string(msg))
		return err
	}
	for i := range mm {
		if !strings.HasPrefix(mm[i].Key, measurePrefix) {
			continue
		}
		id := new(Id)
		id.Resolution = parseResolution(opts)
		id.Time = parseTime(id.Resolution, header.Time)
		id.User = opts["user"][0]
		id.Pass = opts["password"][0]
		id.Name = buildPrefix(opts, mm[i].Key)
		id.Units = mm[i].Unit
		id.Source = mm[i].Source
		out <- &Bucket{Id: id, Vals: []float64{mm[i].Val}}
	}
	return nil
}

func parseHkRouter(out chan *Bucket, opts Options, header *lpx.Header, msg []byte) error {
	if string(header.Procid) != routerPrefix {
		return nil
	}
	mm := make(measurements, 0)
	if err := logfmt.Unmarshal(msg, &mm); err != nil {
		return parseError
	}
	for i := range mm {
		id := new(Id)
		id.Resolution = parseResolution(opts)
		id.Time = parseTime(id.Resolution, header.Time)
		id.User = opts["user"][0]
		id.Pass = opts["password"][0]
		id.Source = mm[i].Source
		id.Units = mm[i].Unit

		switch mm[i].Key {
		case "bytes":
			id.Name = buildPrefix(opts, "router.bytes")
		case "connect":
			id.Name = buildPrefix(opts, "router.connect")
		case "service":
			id.Name = buildPrefix(opts, "router.service")
		}
		out <- &Bucket{Id: id, Vals: []float64{mm[i].Val}}
	}
	return nil
}

func buildPrefix(opts Options, suffix string) string {
	//Remove measure. from the name if present.
	if strings.HasPrefix(suffix, measurePrefix) {
		suffix = suffix[len(measurePrefix):]
	}
	pre, present := opts["prefix"]
	if !present {
		return suffix
	}
	return pre[0] + "." + suffix
}

func parseTime(d time.Duration, ts []byte) time.Time {
	t, err := time.Parse(time.RFC3339, string(ts))
	if err != nil {
		t = time.Now()
	}
	return time.Unix(0, int64((time.Duration(t.UnixNano())/d)*d))
}

func parseResolution(opts Options) time.Duration {
	resTmp, present := opts["resolution"]
	if !present {
		resTmp = []string{"60"}
	}

	res, err := strconv.Atoi(resTmp[0])
	if err != nil {
		return time.Minute
	}

	return time.Second * time.Duration(res)
}
