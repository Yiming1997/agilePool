package agilepool_test

import (
	"testing"

	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/stretchr/testify/assert"
)

func TestAgilePoolWorkerNumCapacityBoundary(t *testing.T) {
	defaultCapacity := agilepool.NewPool(agilepool.NewConfig())
	defer defaultCapacity.Close()

	tests := []struct {
		name     string
		capacity int64
		want     int64
	}{
		{"zero keeps the default", 0, defaultCapacity.GetCapacity()},
		{"negative keeps the default", -1, defaultCapacity.GetCapacity()},
		{"one is the smallest valid capacity", 1, 1},
		{"two is applied", 2, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := agilepool.NewPool(agilepool.NewConfig(agilepool.WithWorkerNumCapacity(tt.capacity)))
			defer p.Close()

			assert.Equal(t, tt.want, p.GetCapacity())
		})
	}
}
