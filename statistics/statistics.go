// A collection of statisticle functions.
package statistics

import (
	"math"
)

type Bucket interface {
	Sort()
	Vals() []float64
}

func Count(b Bucket) int {
	return len(b.Vals())
}

func Sum(b Bucket) float64 {
	if Count(b) == 0 {
		return float64(0)
	}
	s := float64(0)
	for i := range b.Vals() {
		s += b.Vals()[i]
	}
	return s
}

func Mean(b Bucket) float64 {
	if Count(b) == 0 {
		return float64(0)
	}
	return Sum(b) / float64(Count(b))
}

func Min(b Bucket) float64 {
	if Count(b) == 0 {
		return float64(0)
	}
	b.Sort()
	return b.Vals()[0]
}

func Median(b Bucket) float64 {
	if Count(b) == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Ceil(float64(Count(b) / 2)))
	return b.Vals()[pos]
}

func P95(b Bucket) float64 {
	if Count(b) == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(Count(b)) * 0.95))
	return b.Vals()[pos]
}

func P99(b Bucket) float64 {
	if Count(b) == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(Count(b)) * 0.99))
	return b.Vals()[pos]
}

func Max(b Bucket) float64 {
	if Count(b) == 0 {
		return float64(0)
	}
	b.Sort()
	pos := Count(b) - 1
	return b.Vals()[pos]
}

func Last(b Bucket) float64 {
	if Count(b) == 0 {
		return float64(0)
	}
	pos := Count(b) - 1
	return b.Vals()[pos]
}
