package store

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"l2met/utils"
	"log"
)

// Since each worker will most likely
// be using a redis connection, we can optimize our
// connection establishment operations by having a pool
// that is equal in size to the number of workers.
// We add 10 to the workers because: why not?
var maxConn int
func init() {
	maxConn = utils.EnvInt("LOCAL_WORKERS", 2) + 10
}

var redisPool *redis.Pool
func init() {
	var err error
	host, password, err := utils.ParseRedisUrl()
	if err != nil {
		log.Fatal(err)
	}
	redisPool = &redis.Pool{
		MaxIdle:     maxConn,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", host, time.Second, time.Second, time.Second)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", password)
			return c, err
		},
	}
}

func PingRedis() error {
	rc := redisPool.Get()
	defer rc.Close()
	_, err := rc.Do("PING")
	return err
}
