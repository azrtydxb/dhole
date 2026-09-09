//go:build !unix

package process

import "os"

// usageOf reports nothing off unix: there is no rusage on a ProcessState here.
// Saying so is the point — a fabricated zero would sit in the same metric as
// real measurements and be indistinguishable from a step that used no CPU.
func usageOf(*os.ProcessState) (cpuSeconds float64, maxRSSBytes int64, ok bool) {
	return 0, 0, false
}
