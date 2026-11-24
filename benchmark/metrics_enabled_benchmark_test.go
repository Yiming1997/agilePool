//go:build metrics_enabled

package benchmark_test

import (
	agilepool "github.com/Yiming1997/agilePool/v2"
	"github.com/Yiming1997/agilePool/v2/hook"
	"github.com/Yiming1997/agilePool/v2/metrics"
)

// Only the candidate's enabled binary includes this file. The baseline and
// candidate-disabled binaries run the same common fixture with no setup hook.
func init() {
	metricsBenchmarkSetup = func(p *agilepool.Pool) error {
		collector := metrics.NewCollector(p)
		hooks := hook.NewHooks()
		collector.Register(hooks)
		return p.SetHook(hooks)
	}
}
