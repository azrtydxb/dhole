// Command dhole is the Dhole control plane binary.
package main

import (
	"fmt"
	"os"

	"github.com/azrtydxb/dhole/internal/version"
)

func main() {
	// Serving the control plane arrives with the server package; until then
	// the binary exists so the build stamps a version into something.
	// A failed write to stdout loses nothing worth reporting an error over.
	_, _ = fmt.Fprintf(os.Stdout, "dhole %s (%s)\n", version.Version(), version.Commit())
}
