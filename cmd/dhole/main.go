// Command dhole is the Dhole control plane binary and its command line.
//
// The whole command tree lives in cmd/dhole/cli, which builds it from the
// protobuf descriptors so that every RPC the GUI can call has a command here
// too (ADR 0013). This file is the process: arguments in, exit code out.
package main

import (
	"os"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

func main() {
	os.Exit(cli.Main(os.Args[1:], os.Stdout, os.Stderr))
}
