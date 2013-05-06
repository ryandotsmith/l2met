package store

import (
	"l2met/bucket"
	"time"
)

type Store interface {
	MaxPartitions() uint64
	Put(*bucket.Bucket) error
	Get(*bucket.Bucket) error
	Scan(time.Time) (<-chan *bucket.Bucket, error)
	Health() bool
}
