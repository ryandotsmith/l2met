package outlet

import (
	"errors"
	"fmt"
	"l2met/bucket"
	"l2met/store"
	"l2met/utils"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type HttpOutlet struct {
	Store store.Store
	Query url.Values
}

func (h *HttpOutlet) ServeReadBucket(w http.ResponseWriter, r *http.Request) {
	// need to extract: token, source, name, time
	// https://l2met:token@l2met.net/buckets/:name
	user, pass, err := utils.ParseAuth(r)
	if err != nil {
		http.Error(w, "Inavalid Authentication", 401)
		return
	}

	// Shortcut so we can quickly access query params.
	h.Query = r.URL.Query()

	// It is ok if src is blank.
	src := h.Query.Get("source")

	name := h.Query.Get("name")
	if len(name) == 0 {
		http.Error(w, "Invalid Request. Name is required.", 400)
		return
	}

	countAssertion, err := h.parseAssertion("count", -1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}

	meanAssertion, err := h.parseAssertion("mean", -1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}

	sumAssertion, err := h.parseAssertion("sum", -1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}

	tol, err := h.parseAssertion("tol", 0)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}

	limit, err := h.parseAssertion("limit", 1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}

	offset, err := h.parseAssertion("offset", 1)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}

	res, err := h.parseAssertion("tol", 60)
	if err != nil {
		http.Error(w, "Invalid Request.", 400)
		return
	}
	resolution := time.Second * time.Duration(res)

	id := &bucket.Id{
		User:       user,
		Pass:       pass,
		Name:       name,
		Source:     src,
		Resolution: resolution,
		Units:      "u",
	}
	resBucket := &bucket.Bucket{Id: id}
	anchorTime := time.Now()
	for i := 0; i < limit; i++ {
		x := time.Duration((i + offset) * -1) * resolution
		id.Time = utils.RoundTime(anchorTime.Add(x), resolution)
		fmt.Printf("time=%d\n", id.Time)
		b := &bucket.Bucket{Id: id}
		//Fetch the bucket from our store.
		//This will fill in the vals.
		h.Store.Get(b)
		resBucket.Add(b)
	}

	//If any of the assertion values are -1 then they were not
	//defined in the request query params. Thus, we only do our assertions
	//if the assertion parameter is > 0.

	if countAssertion > 0 {
		if math.Abs(float64(resBucket.Count()-countAssertion)) > float64(tol) {
			http.Error(w, "Count assertion failed.", 404)
			return
		}
	}

	if meanAssertion > 0 {
		if math.Abs(float64(resBucket.Mean()-float64(meanAssertion))) > float64(tol) {
			http.Error(w, "Mean assertion failed.", 404)
			return
		}
	}

	if sumAssertion > 0 {
		if math.Abs(float64(resBucket.Sum()-float64(sumAssertion))) > float64(tol) {
			http.Error(w, "Sum assertion failed.", 404)
			return
		}
	}

	utils.WriteJson(w, 200, resBucket)
}

// Returns -1 when a value was specified in data.
func (h *HttpOutlet) parseAssertion(name string, defaultVal int) (int, error) {
	tmpVal := h.Query.Get(name)
	if len(tmpVal) == 0 {
		return defaultVal, nil
	}
	val, err := strconv.Atoi(tmpVal)
	if err != nil {
		return -1, errors.New("Assertion must be a positive integer.")
	}
	if val < 0 {
		return -1, errors.New("Assertion must be a positive integer.")
	}
	return val, nil
}

func (h *HttpOutlet) computeTime(res time.Duration, offset int) time.Time {
	t := time.Now().Add(time.Duration(-1 * offset) * res)
	return utils.RoundTime(t, res)
}
