package agilepool

import (
	"strings"
	"testing"
)

// TestStack verifies basic stack trace functionality
func TestStack(t *testing.T) {
	// Get stack trace skipping this function frame
	stack := Stack(1)
	stackStr := string(stack)

	// Verify non-empty result
	if len(stackStr) == 0 {
		t.Error("Stack returned empty content")
	}

	// Verify test function name appears in trace
	if !strings.Contains(stackStr, "TestStack") {
		t.Errorf("Stack should contain TestStack function, got:\n%s", stackStr)
	}

	// Verify format includes file path and line number
	if !strings.Contains(stackStr, ".go:") {
		t.Errorf("Stack format error, should contain file path and line number:\n%s", stackStr)
	}

	t.Logf("Stack trace:\n%s", stackStr)
}

// TestNestedCall verifies stack trace captures multiple nested function calls
func TestNestedCall(t *testing.T) {
	result := level1(t)
	resultStr := string(result)

	if len(resultStr) == 0 {
		t.Error("Nested call returned empty content")
	}

	// All nested functions should appear in trace
	if !strings.Contains(resultStr, "level1") {
		t.Error("Should contain level1 function")
	}
	if !strings.Contains(resultStr, "level2") {
		t.Error("Should contain level2 function")
	}
	if !strings.Contains(resultStr, "TestNestedCall") {
		t.Error("Should contain TestNestedCall function")
	}

	t.Logf("Nested call stack:\n%s", resultStr)
}

// level1 is a helper for testing nested stack traces
func level1(t *testing.T) []byte {
	return level2(t)
}

// level2 captures full stack trace (skip=0 includes all frames)
func level2(t *testing.T) []byte {
	return Stack(0)
}
