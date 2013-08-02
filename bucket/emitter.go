package bucket

type libratoAttrs struct {
	Min   int    `json:"display_min"`
	Units string `json:"display_units_long"`
}

// When submitting data to Librato, we need to coerce
// our bucket representation into something their API
// can handle. Because there is not a 1-1 parity
// with the statistical functions that a bucket offers and
// the types of data the Librato API accepts (e.g. Librato does-
// not have support for p50, p95, p99) we need to expand
// our bucket into a set of LibratoMetric(s).
type LibratoMetric struct {
	Name   string        `json:"name"`
	Time   int64         `json:"measure_time"`
	Val    float64       `json:"value"`
	Source string        `json:"source,omitempty"`
	User   string        `json:"-"`
	Pass   string        `json:"-"`
	Attr   *libratoAttrs `json:"attributes,omitempty"`
}

// Emitter abstracts the bucket from what type of
// metrics are emitted from the bucket. This is useful
// when you want to selectively submit different metrics to
// the outlet API.
type Emitter func(*Bucket) []*LibratoMetric

// The standard emitter. All log data with `measure.foo` will
// be mapped to the MeasureEmitter.
func MeasureEmitter(b *Bucket) []*LibratoMetric {
	metrics := make([]*LibratoMetric, 9)
	metrics[0] = b.Metric("min", b.Min())
	metrics[1] = b.Metric("median", b.Median())
	metrics[2] = b.Metric("p95", b.P95())
	metrics[3] = b.Metric("p99", b.P99())
	metrics[4] = b.Metric("max", b.Max())
	metrics[5] = b.Metric("mean", b.Mean())
	metrics[6] = b.Metric("sum", b.Sum())
	metrics[7] = b.Metric("count", float64(b.Count()))
	metrics[8] = b.Metric("last", b.Last())
	return metrics
}

func CountEmitter(b *Bucket) []*LibratoMetric {
	metrics := make([]*LibratoMetric, 1)
	metrics[0] = b.Metric("sum", b.Sum())
	return metrics
}

func SampleEmitter(b *Bucket) []*LibratoMetric {
	metrics := make([]*LibratoMetric, 1)
	metrics[0] = b.Metric("last", b.Last())
	return metrics
}
