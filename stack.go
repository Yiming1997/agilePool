package agilepool

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
)

// Stack returns formatted call stack info including file, line, PC, and source code.
//
// skip: frames to skip (1 = skip Stack itself, 2 = also skip caller)
//
// Example output:
//
//	/home/user/main.go:25 (0x45a6f8)
//	    main.main: fmt.Println("hello world")
//	/home/user/main.go:30 (0x45a8a2)
//	    main.testFunc
//
// Notes:
//   - Requires readable source files, otherwise shows "Unknown"
//   - Same consecutive files omit repeating source lines
//   - Performs file I/O, not recommended for frequent calls in production
func Stack(skip int) []byte {
	buf := new(bytes.Buffer)
	var lastFile string
	dunno := "Unknown"

	// Iterate through call stack frames
	for i := skip; ; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break // Reached top of stack
		}

		// Print file, line number, and PC address
		fmt.Fprintf(buf, "%s:%d (0x%x)\n", file, line, pc)

		// Print source line only for new files to avoid duplication
		if file != lastFile {
			sourceLine, err := readNthLine(file, line-1)
			if err != nil {
				sourceLine = dunno
			}
			fmt.Fprintf(buf, "\t%s: %s\n", function(pc), sourceLine)
			lastFile = file
		} else {
			// Same file, just print function name
			fmt.Fprintf(buf, "\t%s\n", function(pc))
		}
	}
	return buf.Bytes()
}

// function returns the function name for given program counter (PC).
// Returns "unknown" if no function info is found (e.g., inlined or optimized).
func function(pc uintptr) string {
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return "unknown"
	}
	return fn.Name()
}

// readNthLine reads line n (0-indexed) from the given file.
// Returns error if file cannot be opened, read fails, or line doesn't exist.
// Note: Opens file and scans from beginning each call - not efficient for frequent use.
func readNthLine(file string, n int) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		if lineNum == n {
			return scanner.Text(), nil
		}
		lineNum++
	}
	return "", scanner.Err()
}
