// A collection of measurements.
package bucket

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

type Bucket struct {
	sync.Mutex
	Id *Id
	Vals []float64
	Emtr Emitter
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

// Relies on the Emitter to determine which type of
// metrics should be returned.
func (b *Bucket) Metrics() []*LibratoMetric {
	return b.Emtr(b)
}

func (b *Bucket) Metric(name string, val float64) *LibratoMetric {
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
		Val:    val,
	}
}

func (b *Bucket) String() string {
	return fmt.Sprintf("name=%s source=%s vals=%v",
		b.Id.Name, b.Id.Source, b.Vals)
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
