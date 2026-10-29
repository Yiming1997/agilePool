package agilepool

import (
	"testing"
	"time"
)

// TestZeroConsumeScaleDefaults verifies the fallback knobs default to the
// documented values.
func TestZeroConsumeScaleDefaults(t *testing.T) {
	c := NewConfig()
	if got, want := c.zeroConsumeScaleInterval, defaultZeroConsumeScaleInterval; got != want {
		t.Fatalf("zeroConsumeScaleInterval = %v, want %v", got, want)
	}
	if got, want := c.maxZeroConsumeScaleStep, defaultMaxZeroConsumeScaleStep; got != want {
		t.Fatalf("maxZeroConsumeScaleStep = %d, want %d", got, want)
	}
}

// TestZeroConsumeScaleOptions verifies the options override the defaults and
// that non-positive values are ignored.
func TestZeroConsumeScaleOptions(t *testing.T) {
	c := NewConfig(
		WithZeroConsumeScaleInterval(50*time.Millisecond),
		WithMaxZeroConsumeScaleStep(8),
	)
	if got, want := c.zeroConsumeScaleInterval, 50*time.Millisecond; got != want {
		t.Fatalf("zeroConsumeScaleInterval = %v, want %v", got, want)
	}
	if got, want := c.maxZeroConsumeScaleStep, int64(8); got != want {
		t.Fatalf("maxZeroConsumeScaleStep = %d, want %d", got, want)
	}

	d := NewConfig(
		WithZeroConsumeScaleInterval(0),
		WithZeroConsumeScaleInterval(-time.Second),
		WithMaxZeroConsumeScaleStep(0),
		WithMaxZeroConsumeScaleStep(-5),
	)
	if got, want := d.zeroConsumeScaleInterval, defaultZeroConsumeScaleInterval; got != want {
		t.Fatalf("non-positive interval changed the default: got %v, want %v", got, want)
	}
	if got, want := d.maxZeroConsumeScaleStep, defaultMaxZeroConsumeScaleStep; got != want {
		t.Fatalf("non-positive step changed the default: got %d, want %d", got, want)
	}
}
