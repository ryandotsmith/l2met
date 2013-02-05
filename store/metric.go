package store

import (
	"l2met/encoding"
	"l2met/utils"
	"time"
)

type Metric struct {
	Time   time.Time `json:"time"`
	Name   string    `json:"name"`
	Source string    `json:"source,omitempty"`
	Mean   float64   `json:"mean"`
}

func GetMetrics(token, name string, resolution int64, min, max time.Time) ([]*Metric, error) {
	startQuery := time.Now()
	rows, err := pg.Query("select * from get_buckets($1, $2, $3, $4, $5)",
		token, name, resolution, min, max)
	if err != nil {
		utils.MeasureE("get-metrics-error", err)
		return nil, err
	}
	utils.MeasureT(startQuery, "get-metrics.query")
	startParse := time.Now()
	defer rows.Close()
	var metrics []*Metric
	for rows.Next() {
		startLoop := time.Now()
		var tmp []byte
		b := new(Bucket)
		rows.Scan(&b.Name, &b.Source, &b.Time, &tmp)
		if len(tmp) == 0 {
			b.Vals = []float64{}
			continue
		}
		encoding.DecodeArray(tmp, &b.Vals)
		m := new(Metric)
		m.Time = b.Time
		m.Name = b.Name
		m.Source = b.Source
		m.Mean = b.Mean()
		metrics = append(metrics, m)
		utils.MeasureT(startLoop, "get-metrics.scan-struct-loop")
	}
	utils.MeasureT(startParse, "parse.get-metrics")
	return metrics, nil
}
