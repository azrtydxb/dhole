package cli

import (
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
)

// runStartCmd starts a run of an approved revision.
func runStartCmd(o *options) *cobra.Command {
	var revision string
	var follow bool
	cmd := &cobra.Command{
		Use:   "start <pipeline-id>",
		Short: "start a run of the pipeline's active revision",
		Long: "A run pins the revision it started from, so what it executes cannot\n" +
			"change under it. Only an active revision may run.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			client := o.client()
			res, err := client.StartRun(ctx, connect.NewRequest(&dholev1.StartRunRequest{
				PipelineId: args[0], RevisionId: revision,
			}))
			if err != nil {
				return o.fail("start run", err)
			}
			if err := o.emit(res.Msg, func(w io.Writer) {
				_, _ = fmt.Fprintf(w, "run %s started from revision %s\n",
					res.Msg.GetRunId(), res.Msg.GetRevisionId())
			}); err != nil {
				return err
			}
			if !follow {
				return nil
			}
			return o.followRun(cmd, client, res.Msg.GetRunId(), false)
		},
	}
	cmd.Flags().StringVar(&revision, "revision", "", "revision to run; default is the active one")
	cmd.Flags().BoolVar(&follow, "watch", false, "follow the run's event log until it ends")
	return cmd
}

// runWatchCmd follows a run's event log.
func runWatchCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "watch <run-id>",
		Short: "follow a run's event log from its beginning",
		Long: "The log is the run's only position, and the stream replays it from the\n" +
			"start before following, so connecting late misses nothing.\n" +
			"With --output json each event is one JSON object on its own line.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			return o.followRun(cmd, o.client(), args[0], false)
		},
	}
}

// runLogsCmd is `watch` narrowed to the steps' own reports.
//
// It is a second view of WatchRun rather than an endpoint of its own, because
// the contract has no log RPC: a step's authoritative log lives in the object
// store and its terminal status names the key. Inventing a CLI-only path to
// the bytes would be exactly the privileged corner ADR 0013 refuses.
func runLogsCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:         "logs <run-id>",
		Short:       "the step-level entries of a run's log",
		Args:        exactArgs(1),
		Annotations: map[string]string{RPCAnnotation: "WatchRun"},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			return o.followRun(cmd, o.client(), args[0], true)
		},
	}
	return cmd
}

// runCancelCmd exists, and refuses.
//
// There is no CancelRun in the contract. The command is here rather than
// absent because a person will type it, and "no such command" tells them
// nothing, while this tells them exactly where the gap is. What it must never
// do is reach around the API to stop a run some other way: that would make the
// CLI able to do something the GUI and an agent cannot, which is the same bug
// as the reverse and is what ADR 0013 forbids.
func runCancelCmd(_ *options) *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <run-id>",
		Short: "cancel a run (the contract has no CancelRun yet)",
		Args:  exactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return fmt.Errorf(
				"cannot cancel %s: dhole.v1.PipelineService declares no CancelRun, and the CLI "+
					"will not reach around the API to stop a run (ADR 0013). Add the RPC first",
				args[0])
		},
	}
}

// followRun streams the run's log to stdout. stepsOnly drops the run-level
// bookkeeping and keeps what the steps reported.
func (o *options) followRun(
	cmd *cobra.Command, client dholev1connect.PipelineServiceClient, runID string, stepsOnly bool,
) error {
	ctx, cancel := o.context(cmd)
	defer cancel()

	stream, err := client.WatchRun(ctx, connect.NewRequest(&dholev1.WatchRunRequest{RunId: runID}))
	if err != nil {
		return o.fail("watch run", err)
	}
	defer func() { _ = stream.Close() }()

	failed := false
	for stream.Receive() {
		event := stream.Msg()
		if stepsOnly && event.GetStepId() == "" {
			continue
		}
		if event.GetType() == runFailedEvent {
			failed = true
		}
		if err := o.emitEvent(event); err != nil {
			return err
		}
	}
	if err := stream.Err(); err != nil {
		return o.fail("watch run", err)
	}
	if failed {
		// A run that failed is a failure of the command that watched it, or
		// no script could tell the difference.
		return fmt.Errorf("run %s failed", runID)
	}
	return nil
}

// runFailedEvent is the stored event type that ends a run badly. It is a
// string because that is how the wire carries it.
const runFailedEvent = "RUN_FAILED"

// emitEvent writes one event: JSON per line for a program, one line of text
// for a person.
func (o *options) emitEvent(event *dholev1.WatchRunResponse) error {
	if o.output == outputJSON {
		raw, err := protojson.Marshal(event)
		if err != nil {
			return fmt.Errorf("render json: %w", err)
		}
		_, err = fmt.Fprintf(o.env.Stdout, "%s\n", raw)
		return err
	}
	at := time.Unix(0, event.GetAtUnixNano()).UTC().Format(time.RFC3339)
	_, err := fmt.Fprintf(o.env.Stdout, "%s %6d %-22s %s\n",
		at, event.GetSequence(), event.GetType(), event.GetStepId())
	return err
}
