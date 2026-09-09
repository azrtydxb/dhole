package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/azrtydxb/dhole/internal/obs"
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
	var mode, storeDSN, busURL, blobRoot, deploymentID, otlpEndpoint string
	var otlpInsecure bool
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "run the control plane",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Telemetry is installed before anything can emit, and its
			// shutdown is deferred immediately: spans are batched, so what is
			// not flushed is not exported, and the unflushed tail belongs to
			// whatever someone is about to investigate. An unconfigured
			// collector is not an error — a broken one must not take the
			// control plane down with it.
			shutdownObs, err := obs.Init(cmd.Context(), obs.Config{
				ServiceName:  "dhole-control-plane",
				OTLPEndpoint: otlpEndpoint,
				Insecure:     otlpInsecure,
			})
			if err != nil {
				return fmt.Errorf("telemetry: %w", err)
			}
			defer func() {
				flush, cancel := context.WithTimeout(context.WithoutCancel(cmd.Context()), 10*time.Second)
				defer cancel()
				if err := shutdownObs(flush); err != nil {
					_, _ = fmt.Fprintf(o.env.Stderr, "dhole: telemetry shutdown: %v\n", err)
				}
			}()

			srv, err := server.New(server.Config{
				Mode:     server.Mode(mode),
				StoreDSN: storeDSN,
				BusURL:   busURL,
				BlobRoot: blobRoot,

				DeploymentID: deploymentID,
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
	// Two control planes sharing a database must not share this. It is what
	// scopes the outbox claim, and an unscoped claim makes each plane publish
	// the other's dispatches onto its own bus.
	flags.StringVar(&deploymentID, "deployment-id", os.Getenv("DHOLE_DEPLOYMENT_ID"),
		"name of this control plane; two planes sharing a database must not share it "+
			"(derived from --blob-root when unset)")
	flags.StringVar(&otlpEndpoint, "otlp-endpoint", os.Getenv("DHOLE_OTLP_ENDPOINT"),
		"OTLP collector address for traces and metrics; unset means telemetry goes nowhere")
	flags.BoolVar(&otlpInsecure, "otlp-insecure", false,
		"send to the OTLP collector without TLS")
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
