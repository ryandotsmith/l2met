package main

import (
	"database/sql"
	"fmt"
	"github.com/bmizerany/pq"
	"os"
)

var (
	postgresEnabled = true
	pg              *sql.DB
)

func init() {
	url := os.Getenv("DATABASE_URL")
	if len(url) == 0 {
		postgresEnabled = false
		fmt.Println("Postgres has been disabled.")
		return
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
