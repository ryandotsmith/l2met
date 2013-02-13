package main

import (
	"database/sql"
	"fmt"
	"l2met/encoding"
	"l2met/store"
	"l2met/utils"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
)

var (
	partitionId     int
	maxPartitions   int
	workers         int
	processInterval int
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var strmp string
	var err error

	strmp = os.Getenv("LOCAL_WORKERS")
	if len(strmp) == 0 {
		workers = 2
	} else {
		n, err := strconv.Atoi(strmp)
		if err != nil {
			n = 2
		}
		workers = n
	}

	strmp = os.Getenv("POSTGRES_INTERVAL")
	if len(strmp) == 0 {
		processInterval = 5
	} else {
		n, err := strconv.Atoi(strmp)
		if err != nil {
			n = 5
		}
		processInterval = n
	}

	tmp := os.Getenv("MAX_POSTGRES_PROCS")
	maxPartitions, err = strconv.Atoi(tmp)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	var err error
	outbox := make(chan *store.Bucket, 1000)

	// acquire partition lock
	partitionId, err = utils.LockPartition(pg, "postgres_outlet", maxPartitions)
	if err != nil {
		log.Fatal("Unable to lock partition.")
	}

	// schedule redis reader
	go scheduleFetch(outbox)

	// read from the outbox
	// dump into postgres
	for i := 0; i < workers; i++ {
		go handleBuckets(outbox)
	}

	// Print chanel metrics & live forever.
	report(outbox)
}

func report(o chan *store.Bucket) {
	for _ = range time.Tick(time.Second * 5) {
		utils.MeasureI("postgres_outlet.outbox", int64(len(o)))
	}
}

func scheduleFetch(outbox chan<- *store.Bucket) {
	for t := range time.Tick(time.Second) {
		if t.Second()%processInterval == 0 {
			fetch(t, outbox)
		}
	}
}

func fetch(t time.Time, outbox chan<- *store.Bucket) {
	fmt.Printf("at=start_fetch minute=%d\n", t.Minute())
	defer utils.MeasureT(time.Now(), "postgres_outlet.fetch")

	mailbox := fmt.Sprintf("postgres_outlet.%d", partitionId)
	for bucket := range store.ScanBuckets(mailbox) {
		outbox <- bucket
	}
}

func handleBuckets(outbox <-chan *store.Bucket) {
	for bucket := range outbox {
		err := writeToPostgres(bucket)
		if err != nil {
			log.Printf("measure=%q\n", "pg-write-failure")
			continue
		}
	}
}

func writeToPostgres(bucket *store.Bucket) error {
	var err error

	tx, err := pg.Begin()
	if err != nil {
		return err
	}

	bucket.Get()
	vals := string(encoding.EncodeArray(bucket.Vals))
	row := tx.QueryRow(`
    SELECT id
    FROM buckets
    WHERE token = $1 AND measure = $2 AND source = $3 AND time = $4
  `, bucket.Key.Token, bucket.Key.Name, bucket.Key.Source, bucket.Key.Time)

	var id sql.NullInt64
	row.Scan(&id)

	if id.Valid {
		_, err = tx.Exec("UPDATE buckets SET vals = $1::FLOAT8[] WHERE id = $2", vals, id)

		if err != nil {
			tx.Rollback()
			return err
		}
	} else {
		_, err = tx.Exec(`
      INSERT INTO buckets(token, measure, source, time, vals)
      VALUES($1, $2, $3, $4, $5::FLOAT8[])
    `, bucket.Key.Token, bucket.Key.Name, bucket.Key.Source, bucket.Key.Time, vals)

		if err != nil {
			tx.Rollback()
			return err
		}
	}

	tx.Commit()
	return err
}
