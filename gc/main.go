package main

import (
	"flag"
	"fmt"
	"l2met/utils"
	"time"
)

var (
	period = flag.String("period", "day", "Prune the last period. E.g. Delete day from last minute | hour | day")
)

func init() {
	flag.Parse()
}

func main() {
	for t := range time.Tick(time.Second) {
		if t.Second()%10 == 0 {
			count, err := pruneBuckets(*period)
			if err != nil {
				fmt.Printf("measure=%q\n", "prune-failure")
			}
			fmt.Printf("measure=%q val=%d\n", "prune-buckets", count)
		}
	}
}

func pruneBuckets(period string) (int64, error) {
	defer utils.MeasureT(time.Now(), "delete-buckets")
	s := "delete from buckets where time < now() - '1 "
	s += period + " '::interval"
	res, err := pg.Exec(s)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
