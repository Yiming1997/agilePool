package agilepool

type bucketDef struct {
	low  int64
	high int64 // -1 means infinity
}

// histogram implements a fixed-bucket histogram with FIFO eviction.
// It tracks the frequency distribution of samples over the last N windows.
type histogram struct {
	buckets []bucketDef
	counts  []int64    // current count per bucket
	total   int64      // total samples currently tracked
	samples []int      // ring buffer of bucket index for each sample
	pos     int        // next write position
	filled  bool       // true once ring buffer has wrapped
}

func newHistogram(buckets []bucketDef, windowSize int) *histogram {
	return &histogram{
		buckets: buckets,
		counts:  make([]int64, len(buckets)),
		samples: make([]int, windowSize),
	}
}

func (h *histogram) add(value int64) {
	idx := h.bucketIndex(value)

	// If we're overwriting an old sample, decrement its bucket count
	if h.filled {
		oldIdx := h.samples[h.pos]
		h.counts[oldIdx]--
		h.total--
	}

	// Record new sample
	h.samples[h.pos] = idx
	h.counts[idx]++
	h.total++

	h.pos++
	if h.pos >= len(h.samples) {
		h.pos = 0
		h.filled = true
	}
}

// median returns the approximate median value by finding which bucket
// contains the middle element, then returning the bucket midpoint.
func (h *histogram) median() float64 {
	if h.total == 0 {
		return 0
	}

	mid := h.total / 2
	var cum int64
	for i, cnt := range h.counts {
		cum += cnt
		if cum > mid {
			b := h.buckets[i]
			if b.high == -1 {
				return float64(b.low) // bottom of open-ended bucket
			}
			return float64(b.low+b.high) / 2.0
		}
	}
	return 0
}

func (h *histogram) bucketIndex(value int64) int {
	for i, b := range h.buckets {
		if value >= b.low && (b.high == -1 || value <= b.high) {
			return i
		}
	}
	// Fallback: last bucket
	return len(h.buckets) - 1
}

// default bucket definitions (counts per 100ms sample window)
var (
	submitBuckets = []bucketDef{
		{low: 0, high: 0},
		{low: 1, high: 5},
		{low: 6, high: 20},
		{low: 21, high: 100},
		{low: 101, high: 500},
		{low: 501, high: 2000},
		{low: 2001, high: 10000},
		{low: 10001, high: -1},
	}
	consumeBuckets = []bucketDef{
		{low: 0, high: 0},
		{low: 1, high: 5},
		{low: 6, high: 20},
		{low: 21, high: 100},
		{low: 101, high: 500},
		{low: 501, high: 2000},
		{low: 2001, high: 10000},
		{low: 10001, high: -1},
	}
	exitBuckets = []bucketDef{
		{low: 0, high: 0},
		{low: 1, high: 2},
		{low: 3, high: 5},
		{low: 6, high: 10},
		{low: 11, high: 20},
		{low: 21, high: 50},
		{low: 51, high: 100},
		{low: 101, high: -1},
	}
)
