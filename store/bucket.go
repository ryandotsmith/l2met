package store

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/bmizerany/logplex"
	"io"
	"l2met/encoding"
	"l2met/utils"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

type BKey struct {
	Time   time.Time
	Name   string
	Source string
}

type Bucket struct {
	sync.Mutex
	Id     int64     `json:"id"`
	Time   time.Time `json:"time"`
	Name   string    `json:"name"`
	Source string    `json:"source,omitempty"`
	Token  string
	Vals   []float64 `json:"vals,omitempty"`
}

// Cachable Interface
func (b *Bucket) Key() int64 {
	return b.Id
}

func GetBuckets(token string, min, max time.Time) ([]*Bucket, error) {
	var buckets []*Bucket
	startQuery := time.Now()
	rows, err := pgRead.Query("select measure, time, source, token, vals from buckets where token = $1 and time > $2 and time <= $3 order by time desc",
		token, min, max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	utils.MeasureT(startQuery, "buckets.get-all")

	startParse := time.Now()
	for rows.Next() {
		var tmp []byte
		b := new(Bucket)
		buckets = append(buckets, b)
		rows.Scan(&b.Name, &b.Time, &b.Source, &b.Token, &tmp)
		if len(tmp) == 0 {
			b.Vals = []float64{}
			continue
		}
		encoding.DecodeArray(tmp, &b.Vals)
	}
	utils.MeasureT(startParse, "buckets.vals.decode")
	return buckets, nil
}

func NewBucket(token string, rdr *bufio.Reader) <-chan *Bucket {
	buckets := make(chan *Bucket, 10)
	go func(c chan<- *Bucket) {
		defer close(c)
		lp := logplex.NewReader(rdr)
		for {
			packet, err := lp.ReadMsg()
			if err != nil {
				if err == io.EOF {
					break
				}
				fmt.Printf("at=logplex-error err=%s\n", err)
				return
			}
			utils.Measure("received-log-line")
			utils.Measure(token + "-received-log-line")
			d, err := encoding.ParseMsgData(packet.Msg)
			if err != nil {
				continue
			}

			name, ok := d["measure"]
			if !ok {
				continue
			}

			source, ok := d["source"]
			if !ok {
				source = ""
			}

			var val float64
			tmpVal, ok := d["val"]
			if ok {
				val, err = strconv.ParseFloat(tmpVal, 64)
				if err != nil {
					fmt.Printf("at=error error=\"unable to parse val.\"\n")
					continue
				}
			} else {
				val = float64(1)
			}

			t, err := packet.Time()
			if err != nil {
				fmt.Printf("at=time-error error=%s\n", err)
				continue
			}

			m := &Bucket{}
			m.Token = token
			m.Time = utils.RoundTime(t, time.Minute)
			m.Name = name
			m.Source = source
			m.Vals = append(m.Vals, val)
			c <- m
		}
	}(buckets)
	return buckets
}

func (b *Bucket) Add(otherM *Bucket) {
	b.Lock()
	defer b.Unlock()
	for _, v := range otherM.Vals {
		b.Vals = append(b.Vals, v)
	}
}

// time:consumer:name:source
func (b *Bucket) String() (res string) {
	b.Lock()
	defer b.Unlock()
	res += strconv.FormatInt(b.Time.Unix(), 10) + ":"
	res += b.Name
	if len(b.Source) > 0 {
		res += ":" + b.Source
	}
	return
}

func (b *Bucket) Get() error {
	defer utils.MeasureT(time.Now(), "bucket.get")
	rows, err := pgRead.Query("select measure, time, source, token, vals from buckets where id = $1",
		b.Id)
	if err != nil {
		return err
	}
	defer rows.Close()
	rows.Next()
	var tmp []byte
	rows.Scan(&b.Name, &b.Time, &b.Source, &b.Token, &tmp)
	err = encoding.DecodeArray(tmp, &b.Vals)
	if err != nil {
		return err
	}
	return nil
}

func (b *Bucket) Put() error {
	defer utils.MeasureT(time.Now(), "bucket.put")
	b.Lock()
	defer b.Unlock()

	txn, err := pg.Begin()
	if err != nil {
		return err
	}

	found := false
	s := "select id from buckets where measure = $1 and source = $2 and time = $3"
	rows, err := txn.Query(s, b.Name, b.Source, b.Time)
	if err != nil {
		txn.Rollback()
		return err
	}
	for rows.Next() {
		tmp := new(sql.NullInt64)
		err = rows.Scan(tmp)
		if tmp.Valid {
			found = true
		}
	}
	rows.Close()

	if !found {
		fmt.Printf("at=%q minute=%d name=%s\n",
			"insert-bucket", b.Time.Minute(), b.Name)
		_, err = txn.Exec("insert into buckets (measure, time, source, token) values($1,$2,$3,$4)",
			b.Name, b.Time, b.Source, b.Token)
		if err != nil {
			txn.Rollback()
			return err
		}
	}
	err = txn.Commit()
	if err != nil {
		return err
	}

	_, err = pg.Exec("update buckets set vals = vals || $1::float8[] where measure = $2 and source = $4 and time = $3",
		string(encoding.EncodeArray(b.Vals)), b.Name, b.Time, b.Source)
	if err != nil {
		return err
	}

	return nil
}

func (b *Bucket) Count() int {
	return len(b.Vals)
}

func (b *Bucket) Sum() float64 {
	s := float64(0)
	for i := range b.Vals {
		s += b.Vals[i]
	}
	return s
}

func (b *Bucket) Mean() float64 {
	return b.Sum() / float64(b.Count())
}

func (b *Bucket) Sort() {
	if !sort.Float64sAreSorted(b.Vals) {
		sort.Float64s(b.Vals)
	}
}

func (b *Bucket) Min() float64 {
	b.Sort()
	return b.Vals[0]
}

func (b *Bucket) Median() float64 {
	b.Sort()
	pos := int(math.Ceil(float64(b.Count() / 2)))
	return b.Vals[pos]
}

func (b *Bucket) P95() float64 {
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.95))
	return b.Vals[pos]
}

func (b *Bucket) P99() float64 {
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.99))
	return b.Vals[pos]
}

func (b *Bucket) Max() float64 {
	b.Sort()
	pos := b.Count() - 1
	return b.Vals[pos]
}

func (b *Bucket) Last() float64 {
	pos := b.Count() - 1
	return b.Vals[pos]
}
