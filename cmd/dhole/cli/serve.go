package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/azrtydxb/dhole/internal/blobstore"
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
	var mode, storeDSN, busURL, blobRoot, deploymentID, otlpEndpoint, apiAddr, triggerFile string
	var otlpInsecure, noAPI bool
	var apiOrigins []string
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

			// The object store every process in this deployment shares.
			// Chosen here rather than inside the server so that `dhole serve`
			// and `dhole-engine` read the SAME variables and cannot end up
			// pointed at different stores — which looks like success and
			// loses every log and artifact.
			blobs, sharedBlobs, err := blobstore.FromEnv(filepath.Join(blobRoot, "blobs"))
			if err != nil {
				return err
			}
			if server.Mode(mode) == server.ModeDistributed && !sharedBlobs {
				_, _ = fmt.Fprintf(o.env.Stderr,
					"dhole: WARNING: a distributed plane on a local object store. "+
						"Engines run elsewhere and write their logs and artifacts to their own disks, "+
						"where this plane cannot read them. Set DHOLE_OBJECT_STORE=s3.\n")
			}

			// The event sources this plane runs. There is no trigger table
			// and no RPC that creates one, so a file is how a deployment
			// declares them; an unreadable or invalid file is an error
			// rather than a warning, because a trigger nobody notices is
			// missing is a pipeline that silently never runs.
			triggers, err := loadTriggers(triggerFile)
			if err != nil {
				return err
			}

			srv, err := server.New(server.Config{
				Mode:     server.Mode(mode),
				StoreDSN: storeDSN,
				BusURL:   busURL,
				BlobRoot: blobRoot,
				Blobs:    blobs,

				EnvironmentIdentity: os.Getenv("DHOLE_ENVIRONMENT_IDENTITY"),

				DeploymentID: deploymentID,

				Triggers: triggers,

				APIAddr:           apiAddr,
				NoAPI:             noAPI,
				APIAllowedOrigins: apiOrigins,
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
			_, _ = fmt.Fprintf(o.env.Stdout, "dhole %s (%s): %s control plane, bus %s, API %s\n",
				version.Version(), version.Commit(), mode, srv.BusURL(), apiEndpoint(srv))

			// The bootstrap credential, once, at the moment it is minted.
			//
			// This API has no unauthenticated call, so a plane that printed
			// nothing here would be a plane its own operator cannot reach —
			// and the pressure that creates is how a control plane ends up
			// with an anonymous mode "just until it is configured". It goes
			// to stdout because that is where the person who just started it
			// by hand is looking, and to a 0600 file beside the database
			// because that is what a supervised process leaves for a script.
			if token := srv.BootstrapToken(); token != "" {
				_, _ = fmt.Fprintf(o.env.Stdout,
					"bootstrap credential (valid %s, also written to %s):\n  export DHOLE_TOKEN=%s\n",
					server.BootstrapTTL(), filepath.Join(blobRoot, server.BootstrapTokenFile), token)
			}

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
	flags.StringVar(&apiAddr, "api-addr", envOr("DHOLE_API_ADDR", server.DefaultAPIAddr),
		"address to serve the API on; the one contract the GUI, the CLI and agents share")
	flags.BoolVar(&noAPI, "no-api", false,
		"serve no API at all; the CLI and the web client then have nothing to talk to")
	flags.StringArrayVar(&apiOrigins, "api-allowed-origin", nil,
		"browser origin allowed to make cross-origin API calls; repeatable, and none by default")
	flags.StringVar(&triggerFile, "triggers", os.Getenv("DHOLE_TRIGGERS"),
		"YAML file declaring the cron schedules and webhook endpoints this plane runs")
	return cmd
}

// loadTriggers reads the trigger declarations a plane runs.
//
// A file rather than a table because there is no table: nothing in proto/,
// internal/defstore or internal/api can describe a trigger, so an operator has
// no way to create one through the contract. That is a gap this flag works
// around rather than closes, and it is recorded with Task 27b.
func loadTriggers(path string) ([]server.TriggerSpec, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path) //nolint:gosec // the operator names this file
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	var doc struct {
		Triggers []server.TriggerSpec `json:"triggers"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	if len(doc.Triggers) == 0 {
		return nil, fmt.Errorf("%s declares no triggers", path)
	}
	return doc.Triggers, nil
}

// apiEndpoint is the API address as a URL a person can paste into --server,
// or a plain statement that there is none. A plane whose contract is switched
// off has to SAY so: "API " followed by nothing reads as a formatting bug and
// sends the reader looking in the wrong place.
func apiEndpoint(srv *server.Server) string {
	addr := srv.APIAddr()
	if addr == "" {
		return "not served (--no-api)"
	}
	return "http://" + addr
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
