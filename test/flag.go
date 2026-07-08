package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	agilepool "github.com/Yiming1997/agilePool"
)

// TaskType controls how simulated task durations are generated.
type TaskType int

const (
	TaskFixed   TaskType = iota // constant: BaseMs
	TaskUniform                 // uniform: BaseMs + rand.Float64() * ExtraMs
	TaskNormal                  // normal: max(0, NormFloat64()*SigmaMs + MeanMs)
)

// TaskConfig holds parameters for task duration generation.
type TaskConfig struct {
	Type    TaskType
	BaseMs  int // fixed / uniform base (ms)
	ExtraMs int // uniform random range (ms)
	MeanMs  int // normal mean (ms)
	SigmaMs int // normal stddev (ms)
}

// SubmitType controls how tasks are submitted to the pool.
type SubmitType int

const (
	SubmitImmediate SubmitType = iota // submit all N at once
	SubmitLinear                      // interval = IntervalMs + rand*JitterMs
	SubmitConstant                    // fixed interval = IntervalMs
	SubmitPoisson                     // poisson: -PoissonMeanMs * ln(rand)
	SubmitPhased                      // multi-phase burst via token channels
)

// Phase defines one stage of a multi-phase burst.
type Phase struct {
	StartSec    int // relative start time (seconds)
	DurationSec int // duration (seconds)
	RatePerSec  int // submissions per second
}

// SubmitConfig holds submission control parameters.
type SubmitConfig struct {
	Type          SubmitType
	IntervalMs    int     // linear/constant interval (ms)
	JitterMs      int     // linear jitter range (ms)
	PoissonMeanMs int     // poisson mean interval (ms)
	Phases        []Phase // multi-phase schedule
	Shards        int     // parallel shards (phased)
	Submitters    int     // submitters per shard (phased)
}

// FlagArgs stores all parsed command-line arguments.
type FlagArgs struct {
	// Pool config
	WorkerCapacity    int64
	CleanPeriod       time.Duration
	TaskQueueSize     int64
	IdleContainerType agilepool.IdleContainerType
	WorkMode          agilepool.WorkMode

	// Task & submit
	TaskCfg   TaskConfig
	SubmitCfg SubmitConfig
	NumTasks  int

	// Profiling
	CPUProfile bool
	MemProfile bool

	// Metric sampling
	TakeTime    int
	LogFileName string
	LogFormat   string

	// Post-run wait
	WaitExitTime int
}

var taskTypeNames = map[string]TaskType{
	"fixed": TaskFixed, "uniform": TaskUniform, "normal": TaskNormal,
}

var submitTypeNames = map[string]SubmitType{
	"immediate": SubmitImmediate, "linear": SubmitLinear,
	"constant": SubmitConstant, "poisson": SubmitPoisson, "phased": SubmitPhased,
}

func parseTaskType(s string) TaskType {
	if t, ok := taskTypeNames[s]; ok {
		return t
	}
	return TaskFixed
}

func parseSubmitType(s string) SubmitType {
	if t, ok := submitTypeNames[s]; ok {
		return t
	}
	return SubmitImmediate
}

// parsePhases parses a phase string "startSec,durSec,ratePerSec;...".
func parsePhases(s string) ([]Phase, error) {
	if s == "" {
		return nil, nil
	}
	segments := strings.Split(s, ";")
	phases := make([]Phase, 0, len(segments))
	for i, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		parts := strings.Split(seg, ",")
		if len(parts) != 3 {
			return nil, fmt.Errorf("phase %d: expected 3 comma-separated values, got %d", i, len(parts))
		}
		start, e1 := strconv.Atoi(strings.TrimSpace(parts[0]))
		dur, e2 := strconv.Atoi(strings.TrimSpace(parts[1]))
		rate, e3 := strconv.Atoi(strings.TrimSpace(parts[2]))
		if e1 != nil || e2 != nil || e3 != nil {
			return nil, fmt.Errorf("phase %d: invalid number", i)
		}
		phases = append(phases, Phase{StartSec: start, DurationSec: dur, RatePerSec: rate})
	}
	return phases, nil
}

func parseFlags() FlagArgs {
	var (
		workers                                  int64
		tasks                                    int
		cleanPeriod                              time.Duration
		queueSize                                int64
		container, mode                          string
		cpuProf, memProf                         bool
		takeTime, waitExit                       int
		logFile, logFormat                       string
		taskTypeStr                              string
		taskBase, taskExtra, taskMean, taskSigma int
		submitTypeStr                            string
		submitInterval, submitJitter             int
		submitMeanIntvl, phasesStr               string
		submitShards, submitSubmitters           int
	)

	flag.Int64Var(&workers, "workers", 20000, "max concurrent workers")
	flag.Int64Var(&workers, "w", 20000, "max workers (short)")
	flag.IntVar(&tasks, "tasks", 1000000, "total tasks to submit")
	flag.IntVar(&tasks, "t", 1000000, "total tasks (short)")
	flag.DurationVar(&cleanPeriod, "clean-period", 500*time.Millisecond, "idle worker cleanup interval")
	flag.Int64Var(&queueSize, "queue-size", 10000, "task queue size hint")
	flag.StringVar(&container, "container", "linkedlist", "idle container: linkedlist, minheap, slice, ringqueue")
	flag.StringVar(&container, "c", "linkedlist", "idle container (short)")
	flag.StringVar(&mode, "mode", "block", "work mode: block, nonblock")
	flag.StringVar(&mode, "m", "block", "work mode (short)")

	flag.StringVar(&taskTypeStr, "task-type", "fixed", "task type: fixed, uniform, normal")
	flag.StringVar(&taskTypeStr, "T", "fixed", "task type (short)")
	flag.IntVar(&taskBase, "task-base", 10, "base task duration (ms)")
	flag.IntVar(&taskExtra, "task-extra", 0, "random extra range (ms, uniform)")
	flag.IntVar(&taskMean, "task-mean", 10, "normal mean (ms)")
	flag.IntVar(&taskSigma, "task-sigma", 5, "normal stddev (ms)")

	flag.StringVar(&submitTypeStr, "submit-type", "immediate", "submit mode: immediate, linear, constant, poisson, phased")
	flag.StringVar(&submitTypeStr, "U", "immediate", "submit mode (short)")
	flag.IntVar(&submitInterval, "submit-interval", 10, "submit interval (ms)")
	flag.IntVar(&submitJitter, "submit-jitter", 0, "random jitter (ms, linear)")
	flag.StringVar(&submitMeanIntvl, "submit-mean-interval", "50", "poisson mean interval (ms)")
	flag.StringVar(&phasesStr, "submit-phases", "", "phases: offset,dur,rate;... (phased)")
	flag.StringVar(&phasesStr, "P", "", "phases (short)")
	flag.IntVar(&submitShards, "submit-shards", 1, "parallel shards (phased)")
	flag.IntVar(&submitSubmitters, "submit-submitters", 1, "submitters per shard (phased)")

	flag.BoolVar(&cpuProf, "cpuprofile", false, "enable CPU profiling")
	flag.BoolVar(&memProf, "memprofile", false, "enable memory profiling")
	flag.IntVar(&takeTime, "take-time", 0, "metric sampling interval (0=disabled)")
	flag.IntVar(&takeTime, "i", 0, "sampling interval (short)")
	flag.StringVar(&logFile, "log-file", "", "output file path (empty=auto)")
	flag.StringVar(&logFile, "o", "", "output file (short)")
	flag.StringVar(&logFormat, "log-format", "csv", "output format: csv, json")
	flag.StringVar(&logFormat, "f", "csv", "output format (short)")
	flag.IntVar(&waitExit, "wait-exit", 0, "extra seconds to wait before exit")
	flag.IntVar(&waitExit, "e", 0, "extra wait (short)")

	flag.Parse()

	// Auto-generate log filename
	if logFile == "" && takeTime > 0 {
		logFile = fmt.Sprintf("metrics_%s_w%d_t%d_%s.%s",
			strings.ToLower(taskTypeStr), workers, tasks,
			strings.ToLower(strings.ReplaceAll(container, "_", "")),
			strings.ToLower(logFormat))
		if dir := filepath.Dir(logFile); dir != "." {
			os.MkdirAll(dir, 0755)
		}
	}

	// Map container type
	var ctype agilepool.IdleContainerType
	switch strings.ToLower(container) {
	case "minheap":
		ctype = agilepool.MinHeapType
	case "slice":
		ctype = agilepool.SliceType
	case "ringqueue":
		ctype = agilepool.RingQueueType
	default:
		ctype = agilepool.LinkedListType
	}

	// Map work mode
	var wm agilepool.WorkMode
	if strings.ToLower(mode) == "nonblock" {
		wm = agilepool.NONBLOCK
	} else {
		wm = agilepool.BLOCK
	}

	// Parse phases
	poissonMean, _ := strconv.Atoi(submitMeanIntvl)
	phases, err := parsePhases(phasesStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --submit-phases: %v\n", err)
		os.Exit(1)
	}

	return FlagArgs{
		WorkerCapacity:    workers,
		NumTasks:          tasks,
		CleanPeriod:       cleanPeriod,
		TaskQueueSize:     queueSize,
		IdleContainerType: ctype,
		WorkMode:          wm,
		TaskCfg: TaskConfig{
			Type:   parseTaskType(strings.ToLower(taskTypeStr)),
			BaseMs: taskBase, ExtraMs: taskExtra, MeanMs: taskMean, SigmaMs: taskSigma,
		},
		SubmitCfg: SubmitConfig{
			Type:          parseSubmitType(strings.ToLower(submitTypeStr)),
			IntervalMs:    submitInterval,
			JitterMs:      submitJitter,
			PoissonMeanMs: poissonMean,
			Phases:        phases,
			Shards:        submitShards,
			Submitters:    submitSubmitters,
		},
		CPUProfile: cpuProf, MemProfile: memProf,
		TakeTime: takeTime, LogFileName: logFile, LogFormat: logFormat,
		WaitExitTime: waitExit,
	}
}

func printConfig(args FlagArgs) {
	fmt.Println("╔═══════════════════════════════════════╗")
	fmt.Println("║       agilePool Test Harness          ║")
	fmt.Println("╚═══════════════════════════════════════╝")
	fmt.Printf("  Workers:       %d\n", args.WorkerCapacity)
	fmt.Printf("  Tasks:         %d\n", args.NumTasks)
	fmt.Printf("  Container:     %s\n", containerTypeName(args.IdleContainerType))
	fmt.Printf("  Mode:          %s\n", workModeName(args.WorkMode))
	fmt.Printf("  Task type:     %s", taskTypeName(args.TaskCfg.Type))
	switch args.TaskCfg.Type {
	case TaskFixed:
		fmt.Printf(" (%d ms)\n", args.TaskCfg.BaseMs)
	case TaskUniform:
		fmt.Printf(" (%d + rand×%d ms)\n", args.TaskCfg.BaseMs, args.TaskCfg.ExtraMs)
	case TaskNormal:
		fmt.Printf(" (N(%d,%d) ms)\n", args.TaskCfg.MeanMs, args.TaskCfg.SigmaMs)
	}
	fmt.Printf("  Submit type:   %s\n", submitTypeName(args.SubmitCfg.Type))
	fmt.Printf("  Clean period:  %s\n", args.CleanPeriod)
	fmt.Printf("  Queue size:    %d\n", args.TaskQueueSize)
	if args.CPUProfile {
		fmt.Println("  CPU profile:   enabled")
	}
	if args.MemProfile {
		fmt.Println("  Mem profile:   enabled")
	}
	if args.TakeTime > 0 {
		fmt.Printf("  Sampling:      %ds → %s (%s)\n", args.TakeTime, args.LogFileName, args.LogFormat)
	}
	fmt.Println()
}

func taskTypeName(t TaskType) string {
	switch t {
	case TaskFixed:
		return "fixed"
	case TaskUniform:
		return "uniform"
	case TaskNormal:
		return "normal"
	}
	return "fixed"
}

func submitTypeName(s SubmitType) string {
	switch s {
	case SubmitImmediate:
		return "immediate"
	case SubmitLinear:
		return "linear"
	case SubmitConstant:
		return "constant"
	case SubmitPoisson:
		return "poisson"
	case SubmitPhased:
		return "phased"
	}
	return "immediate"
}

func containerTypeName(t agilepool.IdleContainerType) string {
	switch t {
	case agilepool.MinHeapType:
		return "MinHeap"
	case agilepool.SliceType:
		return "Slice"
	case agilepool.RingQueueType:
		return "RingQueue"
	}
	return "LinkedList"
}

func workModeName(m agilepool.WorkMode) string {
	if m == agilepool.NONBLOCK {
		return "NONBLOCK"
	}
	return "BLOCK"
}

// buildDurationFn returns a closure that produces random task durations.
// Safe for concurrent use.
func buildDurationFn(cfg TaskConfig) func() time.Duration {
	switch cfg.Type {
	case TaskFixed:
		d := time.Duration(cfg.BaseMs) * time.Millisecond
		return func() time.Duration { return d }

	case TaskUniform:
		base := float64(cfg.BaseMs)
		extra := float64(cfg.ExtraMs)
		return func() time.Duration {
			return time.Duration(base+rand.Float64()*extra) * time.Millisecond
		}

	case TaskNormal:
		mean := float64(cfg.MeanMs)
		sigma := float64(cfg.SigmaMs)
		return func() time.Duration {
			v := math.Max(0, rand.NormFloat64()*sigma+mean)
			return time.Duration(v) * time.Millisecond
		}
	}
	d := time.Duration(cfg.BaseMs) * time.Millisecond
	return func() time.Duration { return d }
}

// newTask wraps durFn into an agilepool.Task.
func newTask(durFn func() time.Duration) agilepool.Task {
	return agilepool.TaskFunc(func() error {
		time.Sleep(durFn())
		return nil
	})
}

// newTaskWithWG wraps durFn into a task that calls wg.Done on completion.
func newTaskWithWG(durFn func() time.Duration, wg *sync.WaitGroup) agilepool.Task {
	return agilepool.TaskFunc(func() error {
		defer wg.Done()
		time.Sleep(durFn())
		return nil
	})
}

// runSubmitter dispatches task submission according to SubmitConfig.
// Blocks until all tasks are submitted and processed.
func runSubmitter(pool *agilepool.Pool, cfg SubmitConfig, numTasks int, durFn func() time.Duration) {
	switch cfg.Type {
	case SubmitImmediate:
		runImmediate(pool, numTasks, durFn)
	case SubmitLinear:
		runTimedSubmit(pool, numTasks, durFn, time.Duration(cfg.IntervalMs)*time.Millisecond, time.Duration(cfg.JitterMs)*time.Millisecond, false)
	case SubmitConstant:
		runTimedSubmit(pool, numTasks, durFn, time.Duration(cfg.IntervalMs)*time.Millisecond, 0, true)
	case SubmitPoisson:
		runTimedSubmit(pool, numTasks, durFn, time.Duration(cfg.PoissonMeanMs)*time.Millisecond, 0, false)
	case SubmitPhased:
		runPhased(pool, cfg)
	}
}

func runImmediate(pool *agilepool.Pool, n int, durFn func() time.Duration) {
	for i := 0; i < n; i++ {
		pool.Submit(newTask(durFn))
	}
	pool.Wait()
}

func runTimedSubmit(pool *agilepool.Pool, n int, durFn func() time.Duration, baseInterval, jitter time.Duration, isConstant bool) {
	var wg sync.WaitGroup
	wg.Add(n)

	go func() {
		baseNs := float64(baseInterval)
		jitterNs := float64(jitter)
		for i := 0; i < n; i++ {
			pool.Submit(newTaskWithWG(durFn, &wg))

			var delay time.Duration
			switch {
			case isConstant:
				delay = baseInterval
			case baseInterval == 0:
				delay = 0
			case jitter == 0:
				delay = time.Duration(-baseNs * math.Log(rand.Float64())) // poisson
			default:
				delay = time.Duration(baseNs + rand.Float64()*jitterNs) // linear
			}
			if delay > 0 {
				time.Sleep(delay)
			}
		}
	}()

	wg.Wait()
	pool.Wait()
}

func runPhased(pool *agilepool.Pool, cfg SubmitConfig) {
	if len(cfg.Phases) == 0 {
		return
	}
	shards := cfg.Shards
	if shards < 1 {
		shards = 1
	}
	submittersPerShard := cfg.Submitters / shards
	if submittersPerShard < 1 {
		submittersPerShard = 1
	}

	var totalSubmitted int64
	var submitWG sync.WaitGroup

	for s := 0; s < shards; s++ {
		tokenCh := make(chan struct{}, 10000)
		go dispenseTokens(tokenCh, cfg.Phases, shards)
		submitPhasedTasks(pool, tokenCh, submittersPerShard, &submitWG, &totalSubmitted)
	}

	submitWG.Wait()
	pool.Wait()
	fmt.Printf("  Total submitted: %d\n", atomic.LoadInt64(&totalSubmitted))
}

func dispenseTokens(tokenCh chan<- struct{}, phases []Phase, shards int) {
	for _, p := range phases {
		ticks := p.DurationSec * 1000
		tokensPerTick := p.RatePerSec / shards / 1000
		if tokensPerTick < 1 {
			tokensPerTick = 1
		}
		for tick := 0; tick < ticks; tick++ {
			for t := 0; t < tokensPerTick; t++ {
				tokenCh <- struct{}{}
			}
			time.Sleep(time.Millisecond)
		}
	}
	close(tokenCh)
}

func submitPhasedTasks(pool *agilepool.Pool, tokenCh <-chan struct{}, submitters int, wg *sync.WaitGroup, total *int64) {
	for g := 0; g < submitters; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tokenCh {
				atomic.AddInt64(total, 1)
				pool.Submit(agilepool.TaskFunc(func() error { return nil }))
			}
		}()
	}
}
