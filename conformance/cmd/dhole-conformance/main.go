// Command dhole-conformance runs the engine conformance suite against any
// engine command, in any language.
//
// It lives beside the suite rather than under cmd/ because it is the suite's
// own front door: `make conformance ENGINE=<cmd>` is the supported way for a
// third-party engine author to find out whether their engine complies.
package main

import (
	"context"
	"os"

	"github.com/azrtydxb/dhole/conformance"
)

func main() {
	os.Exit(conformance.Main(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}
