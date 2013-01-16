package utils

import (
	"errors"
	"encoding/base64"
	"encoding/json"
	"strings"
	"fmt"
	"net/http"
	"time"
)

func MeasureE(n string, e error) {
	fmt.Printf("measure=%q error=%s\n", n, e)
}

func MeasureT(t time.Time, n string) {
	fmt.Printf("measure=%q val=%d\n", n, time.Since(t)/time.Millisecond)
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

func ParseToken(r *http.Request) (string, error) {
	header, ok := r.Header["Authorization"]
	if !ok {
		return "", errors.New("Authorization header not set.")
	}

    auth := strings.SplitN(header[0], " ", 2)
	if len(auth) != 2 {
		return "", errors.New("Malformed header.")
	}

	userPass, err := base64.StdEncoding.DecodeString(auth[1])
	if err != nil {
		return "", errors.New("Malformed encoding.")
	}

	parts := strings.Split(string(userPass), ":")
	if len(parts) != 2 {
		return "", errors.New("Password not supplied.")
	}

	return parts[1], nil
}
