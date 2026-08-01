package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	agilepool "github.com/Yiming1997/agilePool"
	"github.com/shirou/gopsutil/v3/cpu"
)

// Couter collects memory, GC, CPU and pool metrics at a fixed interval.
func Couter(args FlagArgs, p *agilepool.Pool, f *os.File, start time.Time) {
	var memStats runtime.MemStats
	tick := time.NewTicker(time.Duration(args.TakeTime * float64(time.Second)))
	defer tick.Stop()

	headerWritten := false

	for range tick.C {
		runtime.ReadMemStats(&memStats)

		cpuPercent, _ := cpu.Percent(time.Second, false)
		var cpuUsage float64
		if len(cpuPercent) > 0 {
			cpuUsage = cpuPercent[0]
		}

		running := p.GetRunningWorkersNum()
		idle := p.GetIdleWorkerCount()
		created := p.GetWorkerCreateCount()
		queueLen := p.GetTaskQueueLen()

		runSec := time.Since(start).Seconds()

		var lastGCRel float64
		if memStats.NumGC > 0 {
			lastGCRel = float64(memStats.LastGC-uint64(start.UnixNano())) / 1e9
		}

		var avgPauseMs float64
		totalPauseMs := float64(memStats.PauseTotalNs) / 1e6
		if memStats.NumGC > 0 {
			avgPauseMs = totalPauseMs / float64(memStats.NumGC)
		}

		s, e, st, c := readHookCounters()

		switch args.LogFormat {
		case "csv":
			if !headerWritten {
				hdr := "run_sec,goroutines,heap_alloc_mb,total_alloc_mb,sys_mb," +
					"gc_total,gc_pause_total_ms,gc_pause_avg_ms,gc_cpu_pct," +
					"last_gc_sec,next_gc_mb," +
					"workers_running,workers_idle,workers_created,task_queue_len," +
					"cpu_pct"
				if hookEnabled {
					hdr += ",hook_submitted,hook_enqueued,hook_started,hook_completed"
				}
				fmt.Fprintln(f, hdr)
				headerWritten = true
			}

			row := fmt.Sprintf("%.3f,%d,%.2f,%.2f,%.2f,%d,%.2f,%.2f,%.4f,%.3f,%.2f,%d,%d,%d,%d,%.2f",
				runSec,
				runtime.NumGoroutine(),
				float64(memStats.Alloc)/1024/1024,
				float64(memStats.TotalAlloc)/1024/1024,
				float64(memStats.Sys)/1024/1024,
				memStats.NumGC,
				totalPauseMs,
				avgPauseMs,
				memStats.GCCPUFraction*100,
				lastGCRel,
				float64(memStats.NextGC)/1024/1024,
				running, idle, created, queueLen,
				cpuUsage)
			if hookEnabled {
				row += fmt.Sprintf(",%d,%d,%d,%d", s, e, st, c)
			}
			fmt.Fprintln(f, row)
		case "json":
			line := fmt.Sprintf(`{"run_sec":%.3f,"goroutines":%d,"heap_alloc_mb":%.2f,`+
				`"total_alloc_mb":%.2f,"sys_mb":%.2f,"gc_total":%d,`+
				`"gc_pause_total_ms":%.2f,"gc_pause_avg_ms":%.2f,`+
				`"gc_cpu_pct":%.4f,"last_gc_sec":%.3f,"next_gc_mb":%.2f,`+
				`"workers_running":%d,"workers_idle":%d,`+
				`"workers_created":%d,"task_queue_len":%d,`+
				`"cpu_pct":%.2f`,
				runSec,
				runtime.NumGoroutine(),
				float64(memStats.Alloc)/1024/1024,
				float64(memStats.TotalAlloc)/1024/1024,
				float64(memStats.Sys)/1024/1024,
				memStats.NumGC,
				totalPauseMs,
				avgPauseMs,
				memStats.GCCPUFraction*100,
				lastGCRel,
				float64(memStats.NextGC)/1024/1024,
				running, idle, created, queueLen,
				cpuUsage)
			if hookEnabled {
				line += fmt.Sprintf(`,"hook_submitted":%d,"hook_enqueued":%d,"hook_started":%d,"hook_completed":%d`, s, e, st, c)
			}
			fmt.Fprintln(f, line+"}")
		}
	}
}
