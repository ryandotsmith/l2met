package parser

import (
	"errors"
	"strconv"
	"strings"
)

type tuple struct {
	Key []byte
	Val []byte
}

func (t *tuple) Name() string {
	return string(t.Key)
}

func (t *tuple) Float64() (float64, error) {
	//If the caller is asking for the float value of a key
	//and the key is blank, we return a 1. It is idiomatic
	//for logs to contain data like: measure.hello. This is
	//interpreted by l2met as: measure.hello=1. That feature
	//is implemented here.
	if len(t.Val) == 0 {
		t.Val = []byte("1")
	}
	digits := make([]byte, 0)
	foundDecimal := false
	for i := range t.Val {
		b := t.Val[i]
		if b == '.' && !foundDecimal {
			foundDecimal = true
			digits = append(digits, b)
			continue
		}
		if b < '0' || b > '9' {
			break
		}
		digits = append(digits, b)
	}
	if len(digits) > 0 {
		v, err := strconv.ParseFloat(string(digits), 10)
		if err != nil {
			return 0, err
		}
		return v, nil
	}
	return 0, errors.New("Unable to parse float.")
}

func (t *tuple) String() string {
	return string(t.Val)
}

func (t *tuple) Units() string {
	_, err := t.Float64()
	if err != nil {
		return ""
	}
	return strings.TrimFunc(string(t.Val), trimToChar)
}

func trimToChar(r rune) bool {
	// objective here is to return true if a rune is 0-9 or .
	return !(r != '.' && r < '0' || r > '9')
}
