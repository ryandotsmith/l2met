package outlet

import "github.com/ryandotsmith/l2met/bucket"

type Reader interface {
	Start(chan *bucket.Bucket)
}
