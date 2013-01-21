package db

import (
	"database/sql"
	"fmt"
	"github.com/bmizerany/pq"
	"os"
	"sync"
)

var (
	PGLocker  sync.Mutex
	PGRLocker sync.Mutex
	PG        *sql.DB
	PGR       *sql.DB
)

func init() {
	url := os.Getenv("DATABASE_URL")
	if len(url) == 0 {
		fmt.Printf("at=error error=\"must set DATABASE_URL\"\n")
		os.Exit(1)
	}
	str, err := pq.ParseURL(url)
	if err != nil {
		fmt.Printf("at=error error=\"unable to parse DATABASE_URL\"\n")
		os.Exit(1)
	}
	PG, err = sql.Open("postgres", str)
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		os.Exit(1)
	}

	rurl := os.Getenv("DATABASE_READ_URL")
	if len(rurl) <= 0 {
		rstr, err := pq.ParseURL(rurl)
		if err != nil {
			fmt.Printf("at=error error=\"unable to parse DATABASE_READ_URL\"\n")
			os.Exit(1)
		}
		PGR, err = sql.Open("postgres", rstr)
		if err != nil {
			fmt.Printf("at=error error=%s\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Missing DATABASE_READ_URL. Using DATABASE_URL to service reads.\n")
	PGR, err = sql.Open("postgres", str)
	if err != nil {
		fmt.Printf("at=error error=%s\n", err)
		os.Exit(1)
	}
}
