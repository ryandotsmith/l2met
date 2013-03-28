package main

import (
	"l2met/outlet"
	"l2met/store"
	"l2met/utils"
	"log"
)

func main() {
	// The number of partitions that our backends support.
	numPartitions := utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
	// The bucket.Store struct will initialize a redis pool for us.
	maxRedisConn := utils.EnvInt("OUTLET_C", 2) + 100
	// We use the store to Put buckets into redis.
	server, pass, err := utils.ParseRedisUrl()
	if err != nil {
		log.Fatal(err)
	}
	rs := store.NewRedisStore(server, pass, numPartitions, maxRedisConn)

	g := outlet.NewGraphiteOutlet()
	g.Store = rs
	g.ApiToken = ""
	g.MaxPartitions = 1
	g.Start()
	select {}
}
