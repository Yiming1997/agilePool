package agilepool

import "slices"

// medianWindow keeps the most recent n exact samples of a counter and reports
// their median.
//
// The scaler derives its worker target from the ratio of two measured rates,
// so the precision of those rates decides the precision of the target. The
// bucketed histogram this replaces rounded every sample to a bucket
// midpoint and reported the lower bound of its open-ended top bucket, so a
// measured rate of 40000 came back as 10001 while a rate of 200 came back as
// 300.5. The ratio of two such estimates is off by up to a factor of five,
// which is enough to make the scaler overshoot badly or stall completely
// depending on which side of a bucket boundary the samples happened to fall.
//
// The median itself still filters short spikes: it is the middle of the last
// n samples, so one anomalous window does not move it.
type medianWindow struct {
	samples []int64
	pos     int
	count   int
	scratch []int64
}

// newMedianWindow returns a window holding up to n samples. A non-positive n
// yields a single-sample window.
func newMedianWindow(n int) *medianWindow {
	if n <= 0 {
		n = 1
	}
	return &medianWindow{
		samples: make([]int64, n),
		scratch: make([]int64, n),
	}
}

// add records a sample, evicting the oldest one once the window is full.
func (w *medianWindow) add(v int64) {
	w.samples[w.pos] = v
	w.pos++
	if w.pos >= len(w.samples) {
		w.pos = 0
	}
	if w.count < len(w.samples) {
		w.count++
	}
}

// median returns the median of the samples currently held, or 0 when the
// window is empty. With an even number of samples it returns the upper
// middle one, which is what the previous bucketed implementation did.
func (w *medianWindow) median() float64 {
	if w.count == 0 {
		return 0
	}
	// The ring buffer holds the valid samples in slots [0, count): before the
	// ring wraps the first count slots are the ones written, and afterwards
	// every slot is valid because the window is full.
	sorted := w.scratch[:w.count]
	copy(sorted, w.samples[:w.count])
	slices.Sort(sorted)
	return float64(sorted[w.count/2])
}
