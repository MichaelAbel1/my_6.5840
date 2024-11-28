package kvraft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
