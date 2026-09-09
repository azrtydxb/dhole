// Command dhole is the Dhole control plane binary.
//
// `dhole serve` is the whole product in one process: with no flags it starts
// an embedded NATS server, a SQLite run store, filesystem object stores and an
// engine beside the plane, and that engine reaches the scheduler over the bus
// exactly as one in another datacentre would. Pointing --mode at distributed
// takes the same control plane and puts it in front of somebody else's
// Postgres, NATS and engines.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/version"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "dhole: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr *os.File) error {
	if len(args) == 0 {
		usage(stderr)
		return errors.New("a subcommand is required")
	}

	switch args[0] {
	case "serve":
		return serve(args[1:], stdout, stderr)
	case "version", "--version", "-version":
		// A binary must always be able to name itself in a bug report; a
		// failed write to stdout loses nothing worth an error.
		_, _ = fmt.Fprintf(stdout, "dhole %s (%s)\n", version.Version(), version.Commit())
		return nil
	case "help", "--help", "-h":
		usage(stdout)
		return nil
	default:
		usage(stderr)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func usage(w *os.File) {
	_, _ = fmt.Fprint(w, `usage: dhole <command> [flags]

  serve     run the control plane
  version   print the version and commit of this build

`)
}

// serve runs the control plane until the process is asked to stop.
func serve(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	mode := fs.String("mode", string(server.ModeEmbedded),
		"embedded (single binary: in-process bus, SQLite, a hosted engine) or distributed")
	storeDSN := fs.String("store-dsn", defaultStoreDSN(),
		"Postgres DSN, or a SQLite file path for anything else")
	busURL := fs.String("bus-url", "", "NATS server to dial; ignored in embedded mode")
	blobRoot := fs.String("blob-root", defaultBlobRoot(),
		"directory the object stores and the embedded bus keep their data under")
	if err := fs.Parse(args); err != nil {
		return err
	}

	srv, err := server.New(server.Config{
		Mode:     server.Mode(*mode),
		StoreDSN: *storeDSN,
		BusURL:   *busURL,
		BlobRoot: *blobRoot,
	})
	if err != nil {
		return err
	}

	// SIGINT and SIGTERM stop the plane rather than killing it: in-flight
	// dispatches stay unacknowledged and come back to somebody else, and the
	// outbox rows that have not gone out are still owed.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := srv.Start(ctx); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "dhole %s (%s): %s control plane, bus %s\n",
		version.Version(), version.Commit(), *mode, srv.BusURL())

	<-ctx.Done()

	// Shutdown runs on a context of its own: the one above is already
	// cancelled, and a Stop that could not wait would be no Stop at all.
	return srv.Stop(context.WithoutCancel(context.Background()))
}

// defaultStoreDSN and defaultBlobRoot put a development deployment somewhere
// predictable, so `dhole serve` with no flags works and says where its data is.
func defaultStoreDSN() string { return filepath.Join(stateDir(), "dhole.db") }

func defaultBlobRoot() string { return stateDir() }

func stateDir() string {
	if dir, err := os.UserConfigDir(); err == nil {
		return filepath.Join(dir, "dhole")
	}
	return filepath.Join(os.TempDir(), "dhole")
}
