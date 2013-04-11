package outlet

import (
	"fmt"
	"l2met/bucket"
	"l2met/store"
	"l2met/utils"
	"math"
	"net/http"
	"strconv"
	"time"
)

type HttpOutlet struct {
	Store store.Store
}

func (h *HttpOutlet) ServeReadBucket(w http.ResponseWriter, r *http.Request) {
	// need to extract: token, source, name, time
	// https://l2met:token@l2met.net/buckets/:name
	user, pass, err := utils.ParseAuth(r)
	if err != nil {
		fmt.Printf("authentication-error=%s\n", err)
		http.Error(w, "Inavalid Authentication", 401)
		return
	}

	q := r.URL.Query()
	src := q.Get("source") // It is ok if src is blank.
	name := q.Get("name")
	if len(name) == 0 {
		http.Error(w, "Invalid Request. Name is required.", 400)
		return
	}

	countTmp := q.Get("count")
	if len(countTmp) == 0 {
		countTmp = "60"
	}
	count, err := strconv.Atoi(countTmp)
	if err != nil {
		http.Error(w, "Invalid Request. Count must be an int.", 400)
		return
	}

	tolTmp := q.Get("tol")
	if len(tolTmp) == 0 {
		tolTmp = "60"
	}
	tol, err := strconv.Atoi(tolTmp)
	if err != nil {
		http.Error(w, "Invalid Request. Tollerance must be an int.", 400)
		return
	}

	t := utils.RoundTime(time.Now().Add(-2*time.Second), time.Second)
	id := &bucket.Id{User: user, Pass: pass, Name: name, Source: src, Resolution: time.Second, Time: t, Units: "u"}
	b := &bucket.Bucket{Id: id}
	h.Store.Get(b)
	fmt.Printf("bucket-length=%d\n", len(b.Vals))

	fmt.Printf("b.count=%d count=%d tol=%d\n", b.Count(), count, tol)
	if math.Abs(float64(b.Count()-count)) <= float64(tol) {
		utils.WriteJson(w, 200, b)
		return
	}

	http.Error(w, "Unable to find data that matched criteria.", 404)
}
