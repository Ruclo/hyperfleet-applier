//go:build bench

package benchmark

import (
	"hash/fnv"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"
)

// httpLog records the duration of every HTTP round trip during a pass.
// wrap must be called once per client: envtest builds a dynamic client and a
// discovery client from the same config, and each needs its own base transport.
type httpLog struct {
	mu   sync.Mutex
	durs []time.Duration
}

func (l *httpLog) wrap(base http.RoundTripper) http.RoundTripper {
	return &recordingTripper{base: base, log: l}
}

type recordingTripper struct {
	base http.RoundTripper
	log  *httpLog
}

func (r *recordingTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := r.base.RoundTrip(req)
	r.log.add(time.Since(start))
	return resp, err
}

func (l *httpLog) add(d time.Duration) {
	l.mu.Lock()
	l.durs = append(l.durs, d)
	l.mu.Unlock()
}

func (l *httpLog) reset() {
	l.mu.Lock()
	l.durs = nil
	l.mu.Unlock()
}

func (l *httpLog) snapshot() []time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]time.Duration, len(l.durs))
	copy(out, l.durs)
	return out
}

// percentiles returns the nearest-rank p50 and p99 of durs.
// An empty slice yields zero durations.
func percentiles(durs []time.Duration) (p50, p99 time.Duration) {
	if len(durs) == 0 {
		return 0, 0
	}
	sorted := append([]time.Duration(nil), durs...)
	slices.Sort(sorted)
	return sorted[percentileIndex(len(sorted), 0.50)], sorted[percentileIndex(len(sorted), 0.99)]
}

func percentileIndex(n int, p float64) int {
	idx := int(math.Ceil(p*float64(n))) - 1
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

// latencySleep draws a seeded lognormal delay: median 100ms, p99 about 400ms.
// mu = ln(0.1). sigma = ln(4) / normsinv(0.99) ≈ 0.596, so the mean is about 120ms.
type latencySleep struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func newLatencySleep(seedKey string) *latencySleep {
	h := fnv.New64a()
	_, _ = h.Write([]byte(seedKey))
	seed := h.Sum64()
	return &latencySleep{rng: rand.New(rand.NewPCG(seed, seed))}
}

func (l *latencySleep) sleep() {
	const (
		mu    = -2.302585092994046 // ln(0.1), median 100ms
		sigma = 0.595819           // ln(4) / 2.32635, p99 ≈ 400ms
	)
	l.mu.Lock()
	z := l.rng.NormFloat64()
	l.mu.Unlock()
	seconds := math.Exp(mu + sigma*z)
	time.Sleep(time.Duration(seconds * float64(time.Second)))
}
