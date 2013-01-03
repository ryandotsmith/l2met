package store

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"l2met/utils"
	"net/url"
	"os"
	"time"
)

var rp *redis.Pool

func init() {
	u, err := url.Parse(os.Getenv("REDISGREEN_URL"))
	if err != nil {
		fmt.Printf("error=%q\n", "Missing REDISGREEN_URL.")
		os.Exit(1)
	}
	server := u.Host
	password, set := u.User.Password()
	if !set {
		fmt.Printf("at=error error=%q\n", "password not set")
		os.Exit(1)
	}
	rp = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 1 * time.Hour,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", password)
			return c, err
		},
	}
}

type Cachable interface {
	Key() int64
}

func CacheSet(c Cachable) error {
	defer utils.MeasureT(time.Now(), "cache-set")
	rc := rp.Get()
	defer rc.Close()
	bs, err := json.Marshal(c)
	if err != nil {
		utils.MeasureE("cache-json-encode", err)
		return err
	}
	_, err = rc.Do("SET", c.Key(), bs)
	if err != nil {
		utils.MeasureE("cache-set", err)
		return err
	}
	return nil
}

func CacheGet(c Cachable) ([]byte, bool) {
	defer utils.MeasureT(time.Now(), "cache-get")
	rc := rp.Get()
	defer rc.Close()
	bs, err := redis.Bytes(rc.Do("GET", c.Key()))
	if err != nil {
		utils.MeasureE("redis-get", err)
		return nil, false
	}
	return bs, true
}
