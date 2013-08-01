package parser

import (
	"bytes"
	"github.com/kr/logfmt"
)

type tuples []*tuple

func (t *tuples) HandleLogfmt(k, v []byte) error {
	*t = append(*t, &tuple{k, v})
	return nil
}

type logData struct {
	Tuples tuples
}

func NewLogData() *logData {
	ld := new(logData)
	ld.Tuples = make([]*tuple, 0)
	return ld
}

func (ld *logData) Read(d []byte) error {
	if err := logfmt.Unmarshal(d, &ld.Tuples); err != nil {
		return err
	}
	return nil
}

// Resets the slice of the log data.
func (ld *logData) Reset() {
	ld.Tuples = ld.Tuples[:0]
}

func (ld *logData) Source() string {
	for _, tuple := range ld.Tuples {
		if bytes.Equal(tuple.Key, []byte("source")) {
			return tuple.String()
		}
		//The Heroku router fills in the host key, if the host
		//is present, we will use this as the source.
		if bytes.Equal(tuple.Key, []byte("host")) {
			return tuple.String()
		}
	}
	return ""
}
