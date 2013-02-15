package utils

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"os"
	"strconv"
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

func EnvUint64(name string, defaultVal uint64) uint64 {
	tmp := os.Getenv(name)
	if len(tmp) != 0 {
		n, err := strconv.ParseUint(tmp, 10, 64)
		if err == nil {
			return n
		}
	}
	return defaultVal
}

func EnvString(name string, defaultVal string) string {
	tmp := os.Getenv(name)
	if len(tmp) != 0 {
		return tmp
	}
	return defaultVal
}

func EnvInt(name string, defaultVal int) int {
	tmp := os.Getenv(name)
	if len(tmp) != 0 {
		n, err := strconv.Atoi(tmp)
		if err == nil {
			return n
		}
	}
	return defaultVal
}

func MeasureI(n string, i int64) {
	n = appName + "." + n
	fmt.Printf("measure=%q val=%d\n", n, i)
}

func MeasureT(name string, t time.Time) {
	fmt.Printf("measure=%q val=%d\n", name, time.Since(t)/time.Millisecond)
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

func LockPartition(pg *sql.DB, ns string, max uint64) (uint64, error) {
	tab := crc32.MakeTable(crc32.IEEE)
	for {
		var p uint64
		for p = 0; p < max; p++ {
			pId := fmt.Sprintf("%s.%d", ns, p)
			check := crc32.Checksum([]byte(pId), tab)
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
		fmt.Printf("at=%q\n", "waiting-for-partition-lock")
		time.Sleep(time.Second * 10)
	}
	return 0, errors.New("Unable to lock partition.")
}
