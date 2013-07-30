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
