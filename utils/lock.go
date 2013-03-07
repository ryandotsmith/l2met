package utils

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

func LockPartition(ns string, max, ttl uint64) (uint64, error) {
	for {
		for p := uint64(0); p < max; p++ {
			name := fmt.Sprintf("lock.%s.%d", ns, p)
			locked, err := writeLock(name, ttl)
			if err != nil {
				return 0, err
			}
			if locked {
				return p, nil
			}
		}
		time.Sleep(time.Second * 5)
	}
	return 0, errors.New("LockPartition impossible broke the loop.")
}

func writeLock(name string, ttl uint64) (bool, error) {
	rc := redisPool.Get()
	defer rc.Close()

	new := time.Now().Unix() + int64(ttl) + 1
	old, err := redis.Int(rc.Do("GETSET", name, new))
	// If the ErrNil is present, the old value is set to 0.
	if err != nil && err != redis.ErrNil && old == 0 {
		return false, err
	}
	// If the new value is greater than the old
	// value, then the old lock is expired.
	return new > int64(old), nil
}
