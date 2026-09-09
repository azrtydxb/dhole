package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/version"
)

// serveCmd runs the control plane until the process is asked to stop.
//
// It is the whole product in one process: with no flags it starts an embedded
// NATS server, a SQLite run store, filesystem object stores and an engine
// beside the plane, and that engine reaches the scheduler over the bus exactly
// as one in another datacentre would. Pointing --mode at distributed takes the
// same control plane and puts it in front of somebody else's Postgres, NATS
// and engines.
func serveCmd(o *options) *cobra.Command {
	var mode, storeDSN, busURL, blobRoot string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "run the control plane",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			srv, err := server.New(server.Config{
				Mode:     server.Mode(mode),
				StoreDSN: storeDSN,
				BusURL:   busURL,
				BlobRoot: blobRoot,
			})
			if err != nil {
				return err
			}

			// SIGINT and SIGTERM stop the plane rather than killing it:
			// in-flight dispatches stay unacknowledged and come back to
			// somebody else, and the outbox rows that have not gone out are
			// still owed.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			if err := srv.Start(ctx); err != nil {
				return err
			}
			_, _ = fmt.Fprintf(o.env.Stdout, "dhole %s (%s): %s control plane, bus %s\n",
				version.Version(), version.Commit(), mode, srv.BusURL())

			<-ctx.Done()

			// Shutdown runs on a context of its own: the one above is already
			// cancelled, and a Stop that could not wait would be no Stop.
			return srv.Stop(context.WithoutCancel(context.Background()))
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&mode, "mode", string(server.ModeEmbedded),
		"embedded (single binary: in-process bus, SQLite, a hosted engine) or distributed")
	flags.StringVar(&storeDSN, "store-dsn", filepath.Join(defaultStateDir(), "dhole.db"),
		"Postgres DSN, or a SQLite file path for anything else")
	flags.StringVar(&busURL, "bus-url", "", "NATS server to dial; ignored in embedded mode")
	flags.StringVar(&blobRoot, "blob-root", defaultStateDir(),
		"directory the object stores and the embedded bus keep their data under")
	return cmd
}

// versionCmd names this build. A binary must always be able to identify itself
// in a bug report.
func versionCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "print the version and commit of this build",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			_, err := fmt.Fprintf(o.env.Stdout, "dhole %s (%s)\n", version.Version(), version.Commit())
			return err
		},
	}
}
