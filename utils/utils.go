package utils

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	appName string
)

func init() {
	appName = os.Getenv("APP_NAME")
	if len(appName) == 0 {
		fmt.Println("Must set APP_NAME.")
		os.Exit(1)
	}
}

func Measure(n string) {
	n = appName + "." + n
	fmt.Printf("measure=%q\n", n)
}

func MeasureI(n string, i int64) {
	n = appName + "." + n
	fmt.Printf("measure=%q val=%d\n", n, i)
}

func MeasureE(n string, e error) {
	n = appName + "." + n
	fmt.Printf("measure=%q error=%s\n", n, e)
}

func MeasureT(t time.Time, n string) {
	n = appName + "." + n
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

func LockPartition(pg *sql.DB, ns string, max int) (int, error) {
	tab := crc64.MakeTable(crc64.ISO)

	for {
		for p := 0; p < max; p++ {
			pId := fmt.Sprintf("%s.%d", ns, p)
			check := crc64.Checksum([]byte(pId), tab)

			rows, err := pg.Query("select pg_try_advisory_lock($1)", check)
			if err != nil {
				continue
			}
			for rows.Next() {
				var result sql.NullBool
				rows.Scan(&result)
				if result.Valid && result.Bool {
					fmt.Printf("at=%q partition=%d max=%d\n",
						"acquired-lock", p, max)
					rows.Close()
					return p, nil
				}
			}
			rows.Close()
		}
		time.Sleep(time.Second * 10)
	}
	return 0, errors.New("Unable to lock partition.")
}
