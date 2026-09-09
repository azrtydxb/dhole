//go:build unix

package process

import (
	"os"
	"runtime"
	"syscall"
	"time"
)

// usageOf reads what a finished process and its waited-for children consumed,
// out of the rusage the kernel returned with the exit status. It is free —
// os/exec already has it — and it is the only place the figure exists: once
// the process is reaped nothing else can be asked.
//
// ok is false when this platform's ProcessState carries no rusage, and the
// caller then reports nothing at all rather than a zero.
func usageOf(state *os.ProcessState) (cpuSeconds float64, maxRSSBytes int64, ok bool) {
	rusage, ok := state.SysUsage().(*syscall.Rusage)
	if !ok {
		return 0, 0, false
	}
	cpu := time.Duration(rusage.Utime.Nano() + rusage.Stime.Nano())
	return cpu.Seconds(), maxRSSBytes64(rusage.Maxrss), true
}

// maxRSSBytes64 converts ru_maxrss to bytes. Linux and the BSDs report it in
// kilobytes; macOS reports it in bytes. Getting this wrong is a memory graph
// wrong by a factor of a thousand, which is the kind of error that gets
// believed rather than noticed.
func maxRSSBytes64(maxrss int64) int64 {
	if runtime.GOOS == "darwin" || runtime.GOOS == "ios" {
		return maxrss
	}
	return maxrss * 1024
}
