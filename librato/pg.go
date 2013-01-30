package main

import (
	"database/sql"
	"fmt"
	"github.com/bmizerany/pq"
	"os"
)

var (
	pg *sql.DB
)

func init() {
	url := os.Getenv("DATABASE_URL")
	if len(url) == 0 {
		fmt.Printf("error=\"must set DATABASE_URL\"\n")
		os.Exit(1)
	}

	pgurl, err := pq.ParseURL(url)
	if err != nil {
		fmt.Printf("error=\"unable to parse DATABASE_URL\"\n")
		os.Exit(1)
	}

	pg, err = sql.Open("postgres", pgurl)
	if err != nil {
		fmt.Printf("error=%s\n", err)
		os.Exit(1)
	}

	pg.Exec("set application_name = 'l2met-next_librato'")
}
