package metchan

import (
	"encoding/json"
	"github.com/ryandotsmith/l2met/conf"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func serve(buf *[]string) (*url.URL, *httptest.Server) {
	f := func(w http.ResponseWriter, r *http.Request) {
		tmp, _ := ioutil.ReadAll(r.Body)
		*buf = append(*buf, string(tmp))
	}
	srv := httptest.NewServer(http.HandlerFunc(f))
	u, _ := url.Parse(srv.URL)
	u.User = url.UserPassword("", "")
	return u, srv
}

var metTests = []struct {
	inName  string
	out     []string
	start   time.Time
}{

	{
		"simple.test",
		[]string{
			"l2met-test.simple.test",
			"l2met-test.simple.test.median",
			"l2met-test.simple.test.perc95",
			"l2met-test.simple.test.perc99",
		},
		time.Now(),
	},
}

func TestMetchan(t *testing.T) {
	for _, ts := range metTests {
		actual := make([]string, len(ts.out))
		u, srv := serve(&actual)
		c := &conf.D{
			AppName:    "l2met-test",
			Verbose:    false,
			MetchanUrl: u,
		}
		mchan := New(c)
		mchan.FlushInterval = time.Millisecond * 500
		mchan.Start()
		mchan.Time(ts.inName, ts.start)
		time.Sleep(mchan.FlushInterval * 2)
		srv.Close()
		for i := range actual {
			compareResult(t, actual[i], ts.out)
		}
	}
}

func compareResult(t *testing.T, actual string, possible []string) {
	if len(actual) == 0 {
		return
	}
	p := new(libratoGauge)
	if err := json.Unmarshal([]byte(actual), &p); err != nil {
		t.Fatalf("input=%s error=%s\n", actual, err)
	}
	for i := range possible {
		for j := range p.Gauges {
			if possible[i] == p.Gauges[j].Name {
				return
			}
		}
	}
	t.Fatalf("Expected to find %s in %v\n", p.Gauges, possible)
}
