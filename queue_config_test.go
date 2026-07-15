package agilepool

import "testing"

func TestTaskQueueSizeConfiguresHandoffCapacity(t *testing.T) {
	tests := []struct {
		name string
		size int64
		want int
	}{
		{name: "default", want: 10000},
		{name: "custom", size: 7, want: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []ConfigOption{}
			if tt.size > 0 {
				opts = append(opts, WithTaskQueueSize(tt.size))
			}
			pool := NewPool(NewConfig(opts...))
			defer pool.Close()

			if got := cap(pool.taskQueue); got != tt.want {
				t.Fatalf("task queue capacity = %d, want %d", got, tt.want)
			}
		})
	}
}
