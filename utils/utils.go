package utils

import (
	"fmt"
	"time"
)

func MeasureI(name string, val int) {
	fmt.Printf("measure.%s=%d\n", name, val)
}

func MeasureT(name string, t time.Time) {
	fmt.Printf("measure.%s=%dms\n", name, time.Since(t)/time.Millisecond)
}

func RoundTime(t time.Time, d time.Duration) time.Time {
	return time.Unix(0, int64((time.Duration(t.UnixNano())/d)*d))
}
