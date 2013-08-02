// A collection of measurements.
package bucket

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

type Bucket struct {
	// A bucket can be locked to ensure safe memory access.
	sync.Mutex
	// The identity of a bucket is used in registers and as keys in redis.
	Id *Id
	// A slice of all the measurements for a bucket.
	Vals []float64 `json:"vals,omitempty"`
}

// Adding bucket a to bucket b copies the vals of bucket b and
// appends them to the vals of bucket a. Nothing is done with the
// Ids of the buckets. You should ensure that buckets have the same Id.
func (b *Bucket) Add(otherM *Bucket) {
	b.Lock()
	defer b.Unlock()
	for _, v := range otherM.Vals {
		b.Vals = append(b.Vals, v)
	}
}

func (b *Bucket) Metrics() []*LibratoMetric {
	metrics := make([]*LibratoMetric, 9)
	metrics[0] = b.Metric("min")
	metrics[1] = b.Metric("median")
	metrics[2] = b.Metric("p95")
	metrics[3] = b.Metric("p99")
	metrics[4] = b.Metric("max")
	metrics[6] = b.Metric("sum")
	metrics[7] = b.Metric("count")
	metrics[5] = b.Metric("mean")
	metrics[8] = b.Metric("last")
	return metrics
}

func (b *Bucket) Metric(name string) *LibratoMetric {
	return &LibratoMetric{
		Attr: &libratoAttrs{
			Min:   0,
			Units: b.Id.Units,
		},
		Name:   b.Id.Name + "." + name,
		Source: b.Id.Source,
		Time:   b.Id.Time.Unix(),
		User:   b.Id.User,
		Pass:   b.Id.Pass,
		Val:    b.queryVal(name),
	}
}

func (b *Bucket) String() string {
	return fmt.Sprintf("name=%s source=%s vals=%v",
		b.Id.Name, b.Id.Source, b.Vals)
}

func (b *Bucket) queryVal(name string) float64 {
	switch name {
	case "min":
		return b.Min()
	case "median":
		return b.Median()
	case "p95":
		return b.P95()
	case "p99":
		return b.P99()
	case "max":
		return b.Max()
	case "sum":
		return b.Sum()
	case "count":
		return float64(b.Count())
	case "mean":
		return b.Mean()
	case "last":
		return b.Last()
	default:
		panic("Value not defined.")
	}
}

func (b *Bucket) Count() int {
	return len(b.Vals)
}

func (b *Bucket) Sum() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	s := float64(0)
	for i := range b.Vals {
		s += b.Vals[i]
	}
	return s
}

func (b *Bucket) Mean() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	return b.Sum() / float64(b.Count())
}

func (b *Bucket) Sort() {
	if !sort.Float64sAreSorted(b.Vals) {
		sort.Float64s(b.Vals)
	}
}

func (b *Bucket) Min() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	return b.Vals[0]
}

func (b *Bucket) Median() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Ceil(float64(b.Count() / 2)))
	return b.Vals[pos]
}

func (b *Bucket) P95() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.95))
	return b.Vals[pos]
}

func (b *Bucket) P99() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.99))
	return b.Vals[pos]
}

func (b *Bucket) Max() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := b.Count() - 1
	return b.Vals[pos]
}

func (b *Bucket) Last() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	pos := b.Count() - 1
	return b.Vals[pos]
}
