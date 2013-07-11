package store

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"hash/crc64"
	"l2met/bucket"
	"l2met/encoding"
	"l2met/utils"
	"strconv"
	"time"
)

var lockPrefix, partitionPrefix string

func init() {
	lockPrefix = "lock"
	partitionPrefix = "partition.outlet"
}

var PartitionTable = crc64.MakeTable(crc64.ISO)

type RedisStore struct {
	redisPool     *redis.Pool
	maxPartitions uint64
}

func NewRedisStore(server, pass string, maxPartitions uint64, maxConn int) *RedisStore {
	return &RedisStore{
		maxPartitions: maxPartitions,
		redisPool:     initRedisPool(server, pass, maxConn),
	}
}

func initRedisPool(server, pass string, maxConn int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxConn,
		IdleTimeout: 30 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", pass)
			return c, err
		},
	}
}

func (s *RedisStore) MaxPartitions() uint64 {
	return s.maxPartitions
}

func (s *RedisStore) Health() bool {
	rc := s.redisPool.Get()
	defer rc.Close()
	_, err := rc.Do("PING")
	if err != nil {
		return false
	}
	return true
}

func (s *RedisStore) Scan(schedule time.Time) (<-chan *bucket.Bucket, error) {
	retBuckets := make(chan *bucket.Bucket)
	p, err := s.lockPartition()
	if err != nil {
		return retBuckets, err
	}
	partition := partitionPrefix + "." + strconv.Itoa(int(p))
	go func(out chan *bucket.Bucket) {
		rc := s.redisPool.Get()
		defer rc.Close()
		defer close(out)
		defer s.unlockPartition(p)
		rc.Send("MULTI")
		rc.Send("SMEMBERS", partition)
		rc.Send("DEL", partition)
		reply, err := redis.Values(rc.Do("EXEC"))
		if err != nil {
			fmt.Printf("at=%q error=%s\n", "bucket-store-scan", err)
			return
		}
		var delCount int64
		var members []string
		redis.Scan(reply, &members, &delCount)
		for _, member := range members {
			id, err := bucket.DecodeId(member)
			if err != nil {
				fmt.Printf("at=%q error=%s\n", "bucket-store-parse-key", err)
				continue
			}
			bucketReady := id.Time.Add(id.Resolution)
			if !bucketReady.After(schedule) {
				out <- &bucket.Bucket{Id: id}
			} else {
				if err := s.putback(id); err != nil {
					fmt.Printf("putback-error=%s\n", err)
				}
			}
		}
	}(retBuckets)
	return retBuckets, nil
}

func (s *RedisStore) putback(id *bucket.Id) error {
	defer utils.MeasureT("bucket.putback", time.Now())
	rc := s.redisPool.Get()
	defer rc.Close()
	key := id.Encode()
	partition := s.bucketPartition([]byte(key))
	rc.Send("MULTI")
	rc.Send("SADD", partition, key)
	rc.Send("EXPIRE", partition, 300)
	_, err := rc.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func (s *RedisStore) Put(b *bucket.Bucket) error {
	defer utils.MeasureT("bucket.put", time.Now())

	rc := s.redisPool.Get()
	defer rc.Close()

	b.Lock()
	key := b.Id.Encode()
	value := b.Vals
	b.Unlock()

	//TODO(ryandotsmith): Ensure consistent keys are being written.
	partition := s.bucketPartition([]byte(key))
	rc.Send("MULTI")
	rc.Send("RPUSH", key, value)
	rc.Send("EXPIRE", key, 300)
	rc.Send("SADD", partition, key)
	rc.Send("EXPIRE", partition, 300)
	_, err := rc.Do("EXEC")
	if err != nil {
		return err
	}
	return nil
}

func (s *RedisStore) Get(b *bucket.Bucket) error {
	defer utils.MeasureT("bucket.get", time.Now())

	rc := s.redisPool.Get()
	defer rc.Close()

	//Fill in the vals.
	reply, err := redis.Values(rc.Do("LRANGE", b.Id.Encode(), 0, -1))
	if err != nil {
		return err
	}
	for _, item := range reply {
		v, ok := item.([]byte)
		if !ok {
			continue
		}
		err = encoding.DecodeArray(v, &b.Vals, '[', ']', ' ')
	}
	return nil
}

func (s *RedisStore) bucketPartition(b []byte) string {
	check := crc64.Checksum(b, PartitionTable)
	return fmt.Sprintf("%s.%d", partitionPrefix, check%s.MaxPartitions())
}

func (s *RedisStore) lockPartition() (uint64, error) {
	for {
		for p := uint64(0); p < s.MaxPartitions(); p++ {
			name := fmt.Sprintf("%s.%d", lockPrefix, p)
			//TODO(ryandotsmith): remove magic number.
			locked, err := s.writeLock(name, 5)
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

func (s *RedisStore) writeLock(name string, ttl uint64) (bool, error) {
	rc := s.redisPool.Get()
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

func (s *RedisStore) unlockPartition(p uint64) error {
	rc := s.redisPool.Get()
	defer rc.Close()
	_, err := rc.Do("DEL", lockPrefix+"."+strconv.Itoa(int(p)))
	return err
}
