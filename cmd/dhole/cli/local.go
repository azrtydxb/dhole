package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/mirror"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/server"
)

// maxPrintedOutput bounds what `local run` reads back out of the
// content-addressed store per port. A step is allowed to produce a gigabyte;
// a terminal is not.
const maxPrintedOutput = 1 << 20

// localCmd is the group of things that need no control plane.
func localCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "local",
		Short: "run pipelines on this machine, with no server to point at",
		Args:  noArgs,
		RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	cmd.AddCommand(localRunCmd(o))
	return cmd
}

// localRunCmd runs a definition here and now.
//
// It is not a simulator. The pipeline goes through the same embedded control
// plane `dhole serve` starts — the real scheduler, a real NATS server, a real
// engine that receives its work over a bus subject — because a "local mode"
// that took a shortcut would make every green local run say nothing about the
// cluster.
func localRunCmd(o *options) *cobra.Command {
	var stateDir, tenant string
	cmd := &cobra.Command{
		Use:   "run <pipeline.yaml>",
		Short: "run a pipeline definition on this machine",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			raw, err := os.ReadFile(args[0]) //nolint:gosec // the path is the user's own argument
			if err != nil {
				return fmt.Errorf("read %s: %w", args[0], err)
			}
			pipeline, err := mirror.FromYAML(raw)
			if err != nil {
				return fmt.Errorf("parse %s: %w", args[0], err)
			}
			if stateDir == "" {
				stateDir = filepath.Join(defaultStateDir(), "local")
			}
			if err := os.MkdirAll(stateDir, 0o700); err != nil {
				return fmt.Errorf("prepare %s: %w", stateDir, err)
			}

			result, err := runLocally(cmd.Context(), o, pipeline, stateDir, tenant)
			if err != nil {
				return err
			}
			if err := o.emitLocalRun(result); err != nil {
				return err
			}
			if !result.Succeeded {
				return fmt.Errorf("run %s failed", result.RunID)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&stateDir, "state-dir", "",
		"where the run's store, bus and blobs live; reused between runs, which is what makes the cache hit")
	cmd.Flags().StringVar(&tenant, "tenant", server.DefaultTenant, "tenant to run under")
	return cmd
}

// localResult is one local run, in the shape both renderings are built from.
type localResult struct {
	RunID     string      `json:"run_id"`
	Succeeded bool        `json:"succeeded"`
	Steps     []localStep `json:"steps"`
}

// localStep is one step's terminal report.
type localStep struct {
	StepID   string            `json:"step_id"`
	Status   string            `json:"status"`
	ExitCode int32             `json:"exit_code"`
	Error    string            `json:"error,omitempty"`
	Outputs  map[string]string `json:"outputs,omitempty"`
}

// runLocally brings a single-binary control plane up, submits the definition,
// waits for the run to end, and reads back what the steps produced.
func runLocally(
	ctx context.Context, o *options, pipeline *dholev1.Pipeline, stateDir, tenant string,
) (localResult, error) {
	srv, err := server.New(server.Config{
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(stateDir, "dhole.db"),
		BlobRoot: stateDir,
		// No contract on this one. `dhole local run` runs one pipeline and
		// exits; a listener on the well-known port would collide with the
		// `dhole serve` the developer already has up, and nobody could reach
		// this plane in the seconds it exists anyway.
		NoAPI: true,
	})
	if err != nil {
		return localResult{}, err
	}

	startCtx, cancelStart := context.WithTimeout(ctx, o.timeout)
	defer cancelStart()
	if err := srv.Start(startCtx); err != nil {
		return localResult{}, fmt.Errorf("start the local control plane: %w", err)
	}
	defer func() {
		// The stop context is its own: the run's deadline may already have
		// passed, and a Stop that could not wait would leave the embedded bus
		// listening after the command returned.
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		if err := srv.Stop(stopCtx); err != nil {
			_, _ = fmt.Fprintf(o.env.Stderr, "dhole: stopping the local control plane: %v\n", err)
		}
	}()

	runID, err := srv.Submit(ctx, tenant, pipeline)
	if err != nil {
		return localResult{}, fmt.Errorf("submit %s: %w", pipeline.GetId(), err)
	}

	events, err := awaitRun(ctx, o, srv, tenant, runID)
	if err != nil {
		return localResult{}, err
	}
	return collectResult(ctx, srv, tenant, runID, events), nil
}

// awaitRun polls the run's event log, which is the run's only position: there
// is no in-memory progress to ask instead (ADR 0003).
func awaitRun(
	ctx context.Context, o *options, srv *server.Server, tenant, runID string,
) ([]runstore.Event, error) {
	deadline := time.Now().Add(o.timeout)
	for {
		events, err := srv.Events(ctx, tenant, runID)
		if err != nil {
			return nil, fmt.Errorf("read the run log: %w", err)
		}
		for _, e := range events {
			if e.Type == runstore.RunCompleted || e.Type == scheduler.RunFailed {
				return events, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf(
				"run %s did not finish within %s; its log so far:%s", runID, o.timeout, describe(events))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// collectResult turns the log into what a person or a script asked to see,
// reading each output's bytes back out of the content-addressed store by the
// digest the step reported.
func collectResult(
	ctx context.Context, srv *server.Server, tenant, runID string, events []runstore.Event,
) localResult {
	result := localResult{RunID: runID}
	for _, e := range events {
		if e.Type == runstore.RunCompleted {
			result.Succeeded = true
		}
		if e.Type != runstore.StepSucceeded && e.Type != runstore.StepFailed {
			continue
		}
		status := &dholev1.JobStatus{}
		if err := proto.Unmarshal(e.Payload, status); err != nil {
			continue
		}
		step := localStep{
			StepID:   e.StepID,
			Status:   string(e.Type),
			ExitCode: status.GetExitCode(),
			Error:    status.GetError(),
			Outputs:  map[string]string{},
		}
		for _, out := range status.GetOutputs() {
			step.Outputs[out.GetPort()] = readOutput(ctx, srv, tenant, out.GetDigest())
		}
		result.Steps = append(result.Steps, step)
	}
	return result
}

// readOutput fetches one output's bytes. A failure to read is reported in
// place rather than raised: the run happened, and the log is worth printing.
func readOutput(ctx context.Context, srv *server.Server, tenant string, digest *dholev1.Digest) string {
	store := srv.CAS()
	if store == nil {
		return ""
	}
	rc, err := store.Get(ctx, tenant, digest)
	if err != nil {
		return fmt.Sprintf("<unreadable: %v>", err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(io.LimitReader(rc, maxPrintedOutput))
	if err != nil {
		return fmt.Sprintf("<unreadable: %v>", err)
	}
	return string(data)
}

// emitLocalRun writes the result. The JSON here is this command's own shape
// rather than a protobuf message, because a local run is not an RPC — there is
// no response message to render.
func (o *options) emitLocalRun(result localResult) error {
	if o.output == outputJSON {
		raw, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("render json: %w", err)
		}
		_, err = fmt.Fprintf(o.env.Stdout, "%s\n", raw)
		return err
	}
	w := o.env.Stdout
	_, _ = fmt.Fprintf(w, "run %s\n", result.RunID)
	for _, step := range result.Steps {
		_, _ = fmt.Fprintf(w, "%s %s (exit %d)\n", step.StepID, step.Status, step.ExitCode)
		if step.Error != "" {
			_, _ = fmt.Fprintf(w, "  error: %s\n", step.Error)
		}
		for port, value := range step.Outputs {
			_, _ = fmt.Fprintf(w, "  %s: %s\n", port, value)
		}
	}
	return nil
}

// describe renders a log for a failure message. A stuck run is the failure
// mode this design fears most, so the error says where it stopped.
func describe(events []runstore.Event) string {
	if len(events) == 0 {
		return " (empty)"
	}
	out := ""
	for _, e := range events {
		out += "\n  " + string(e.Type) + " " + e.StepID
	}
	return out
}

// defaultStateDir is where a deployment that has not been told otherwise keeps
// its data, matching what `dhole serve` uses.
func defaultStateDir() string {
	if dir, err := os.UserConfigDir(); err == nil {
		return filepath.Join(dir, "dhole")
	}
	return filepath.Join(os.TempDir(), "dhole")
}
