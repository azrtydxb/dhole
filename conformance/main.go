package conformance

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

// Main is the command-line front of the suite, kept here rather than in a
// main package so it can be exercised like anything else. The thin wrapper in
// conformance/cmd/dhole-conformance calls it.
//
// It returns a process exit status: 0 when every case passed, 1 when the
// engine failed one, and 2 when the harness itself could not run. The
// difference matters in CI — a broken harness must not read as a broken
// engine.
func Main(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dhole-conformance", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		tier     = fs.String("tier", "trusted", "trust tier the engine runs in")
		engineID = fs.String("engine-id", "conformance-engine", "identity the engine registers under")
		only     = fs.String("case", "", "comma-separated case names to run (a partial run is not a pass)")
		dir      = fs.String("dir", "", "working directory for the engine command")
		timeout  = fs.Duration("timeout", 10*time.Minute, "bound on the whole run")
		verbose  = fs.Bool("v", false, "log progress and the engine's own output")
	)
	fs.Usage = func() {
		_, _ = fmt.Fprint(stderr, "usage: dhole-conformance [flags] <engine command> [args...]\n\n"+
			"Runs the engine conformance suite from docs/wire-contract.md against the given\n"+
			"command. The suite starts its own NATS and object store and plays the control\n"+
			"plane; the engine under test needs nothing but the environment it is handed:\n"+
			"DHOLE_BUS_URL, DHOLE_ENGINE_ID, DHOLE_TIER, DHOLE_SLOTS,\n"+
			"DHOLE_OBJECT_STORE=filesystem with DHOLE_BLOB_DIR, DHOLE_DISPATCH_STREAM,\n"+
			"DHOLE_SECRET_SUBJECT.\n"+
			"The older spellings DHOLE_NATS_URL, DHOLE_ENGINE_TIER and\n"+
			"DHOLE_ENGINE_SLOTS are also set, and are deprecated.\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return 2
	}

	cfg := Config{
		Engine:   fs.Args(),
		Dir:      *dir,
		Tier:     *tier,
		EngineID: *engineID,
	}
	if *only != "" {
		cfg.Only = strings.Split(*only, ",")
	}
	if *verbose {
		cfg.Logf = func(format string, a ...any) { _, _ = fmt.Fprintf(stderr, format+"\n", a...) }
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	report, err := Run(ctx, cfg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "dhole-conformance: %v\n", err)
		if errors.Is(err, context.DeadlineExceeded) {
			_, _ = fmt.Fprintf(stderr, "the run exceeded -timeout %s\n", *timeout)
		}
		return 2
	}
	_, _ = fmt.Fprintln(stdout, report.String())
	if report.Failed > 0 {
		return 1
	}
	return 0
}
