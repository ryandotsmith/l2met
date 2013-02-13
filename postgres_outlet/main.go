package main

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
	partitionId, err = lockPartition()
	if err != nil {
		log.Fatal("Unable to lock partition.")
	}

	// schedule redis reader
	go scheduleFetch(outbox)

	// read from outbox
	// dump into postgres
}

// Lock a partition to work.
func lockPartition() (int, error) {
	tab := crc64.MakeTable(crc64.ISO)

	for {
		for p := 0; p < maxPartitions; p++ {
			pId := fmt.Sprintf("postgres_outlet.%d", p)
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
						"acquired-lock", p, maxPartitions)
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
