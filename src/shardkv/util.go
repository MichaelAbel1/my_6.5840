package shardkv

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func max(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}
