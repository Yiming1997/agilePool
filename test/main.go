package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	agilepool "github.com/Yiming1997/agilePool"
)

func main() {
	args := parseFlags()
	printConfig(args)

	var logFileOut *os.File
	if args.TakeTime > 0 {
		var err error
		logFileOut, err = os.OpenFile(args.LogFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create log file %s: %v\n", args.LogFileName, err)
			os.Exit(1)
		}
		defer logFileOut.Close()
		log.SetOutput(logFileOut)
	}

	// CPU profiling
	if args.CPUProfile {
		f, err := os.Create("cpu_profile.prof")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create CPU profile: %v", err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			fmt.Fprintf(os.Stderr, "failed to start CPU profile: %v", err)
			os.Exit(1)
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
			fmt.Println("  CPU profile written to cpu_profile.prof")
		}()
	}

	// Memory profiling
	if args.MemProfile {
		defer func() {
			runtime.GC()
			f, err := os.Create("mem_profile.prof")
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to create mem profile: %v", err)
				os.Exit(1)
			}
			defer f.Close()
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Fprintf(os.Stderr, "failed to write heap profile: %v", err)
				os.Exit(1)
			}
			fmt.Println("  Mem profile written to mem_profile.prof")
		}()
	}

	start := time.Now()

	pool := newPool(args)
	defer pool.Close()

	// Start metrics collector
	if args.TakeTime > 0 && logFileOut != nil {
		go Couter(args, pool, logFileOut, start)
	}

	// Submit tasks according to task-type + submit-type
	durFn := buildDurationFn(args.TaskCfg)
	runSubmitter(pool, args.SubmitCfg, args.NumTasks, durFn)

	// Three-stage shutdown
	// Wait for all workers to finish
	for pool.GetRunningWorkersNum() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// Wait one tick for final metrics collection
	if args.TakeTime > 0 {
		fmt.Printf("  Waiting %ds for final metrics collection...\n", args.TakeTime)
		time.Sleep(time.Duration(args.TakeTime) * time.Second)
	}

	// Extra observation window
	if args.WaitExitTime > 0 {
		fmt.Printf("  Waiting %ds to observe memory changes...\n", args.WaitExitTime)
		time.Sleep(time.Duration(args.WaitExitTime) * time.Second)
	}

	elapsed := time.Since(start)
	fmt.Printf("  Elapsed:       %s\n", elapsed)
	fmt.Println("  Done.")
}

// newPool creates a pool from args.
func newPool(args FlagArgs) *agilepool.Pool {
	return agilepool.NewPool(agilepool.NewConfig(
		agilepool.WithCleanPeriod(args.CleanPeriod),
		agilepool.WithTaskQueueSize(args.TaskQueueSize),
		agilepool.WithWorkerNumCapacity(args.WorkerCapacity),
		agilepool.WithIdleContainerType(args.IdleContainerType),
		agilepool.WithBlockMode(args.WorkMode),
	))
}
