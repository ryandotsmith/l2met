package utils

import (
	"encoding/json"
	"fmt"
	"l2met/conf"
	"net/http"
	"time"
)

func MeasureI(name string, val int) {
	m := conf.AppName + "." + name
	fmt.Printf("measure.%s=%d\n", m, val)
}

func MeasureT(name string, t time.Time) {
	m := conf.AppName + "." + name
	fmt.Printf("measure.%s=%dms\n", m, time.Since(t)/time.Millisecond)
}

func WriteJsonBytes(w http.ResponseWriter, status int, b []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	w.Write(b)
	w.Write([]byte("\n"))
}

// Convenience
func WriteJson(w http.ResponseWriter, status int, data interface{}) {
	b, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		WriteJson(w, 500, map[string]string{"error": "Internal Server Error"})
	}
	WriteJsonBytes(w, status, b)
}

func RoundTime(t time.Time, d time.Duration) time.Time {
	return time.Unix(0, int64((time.Duration(t.UnixNano())/d)*d))
}
