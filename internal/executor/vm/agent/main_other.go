//go:build !linux

// The guest agent is the init of a Linux microVM, so there is nothing to build
// for any other GOOS. It still has to COMPILE everywhere, because `make check`
// vets every package in the module on whatever machine the gate runs on, and a
// package that only exists on Linux would take the gate down on a developer's
// laptop.
package main

import (
	"fmt"
	"os"
	"runtime"
)

func main() {
	fmt.Fprintf(os.Stderr, "dhole-vm-agent runs as the init of a Linux guest; this binary is %s/%s\n",
		runtime.GOOS, runtime.GOARCH)
	os.Exit(1)
}
