package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/mirror"
)

// pipelineCreateCmd creates a pipeline and its first revision.
//
// It is the entry point every other pipeline command needs and nothing used to
// provide: an edit is applied against a base_revision, and until this RPC
// existed no call in the contract wrote a first one — so a pipeline could only
// be brought into being by writing to the definition store behind the API's
// back, which the GUI cannot do.
func pipelineCreateCmd(o *options) *cobra.Command {
	var definition string
	cmd := &cobra.Command{
		Use:   "create <pipeline-id>",
		Short: "create a pipeline and write its first revision",
		Long: "--definition takes a dhole.v1.Pipeline as JSON, or @file, or @- for\n" +
			"stdin; without it the pipeline starts empty, which is the normal case\n" +
			"because everything after the first revision is an operation.\n" +
			"The tenant is the credential's and is never taken from the definition.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			msg := &dholev1.CreatePipelineRequest{PipelineId: args[0]}
			if definition != "" {
				raw, err := readArgument(definition, cmd.InOrStdin())
				if err != nil {
					return err
				}
				msg.Pipeline = &dholev1.Pipeline{}
				if err := protojson.Unmarshal(raw, msg.GetPipeline()); err != nil {
					return fmt.Errorf("--definition is not a dhole.v1.Pipeline: %w", err)
				}
			}

			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().CreatePipeline(ctx, connect.NewRequest(msg))
			if err != nil {
				return o.fail("create pipeline", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				printRevision(w, res.Msg.GetRevision())
			})
		},
	}
	cmd.Flags().StringVar(&definition, "definition", "",
		"a dhole.v1.Pipeline as JSON, @file or @- ; default is an empty pipeline")
	return cmd
}

// pipelineGetCmd reads one revision of one pipeline.
func pipelineGetCmd(o *options) *cobra.Command {
	var revision string
	cmd := &cobra.Command{
		Use:   "get <pipeline-id>",
		Short: "read a pipeline definition",
		Long: "Reads the pipeline's editing head, or the revision named by\n" +
			"--revision. A run pins a revision, so this is how you read back\n" +
			"exactly what a run executed rather than what the pipeline says today.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().GetPipeline(ctx, connect.NewRequest(&dholev1.GetPipelineRequest{
				PipelineId: args[0], RevisionId: revision,
			}))
			if err != nil {
				return o.fail("get pipeline", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				printRevision(w, res.Msg.GetRevision())
				yaml, err := mirror.ToYAML(res.Msg.GetPipeline())
				if err != nil {
					// The revision is already printed; the definition being
					// unrenderable is worth saying, not worth hiding.
					_, _ = fmt.Fprintf(o.env.Stderr, "dhole: rendering definition: %v\n", err)
					return
				}
				_, _ = w.Write(yaml)
			})
		},
	}
	cmd.Flags().StringVar(&revision, "revision", "", "revision to read; default is the editing head")
	return cmd
}

// pipelineApplyCmd applies one operation-level edit.
//
// Operation-level rather than document-level because that is what the contract
// offers, and the CLI is not allowed a shortcut the GUI does not have: an edit
// that sent the whole document could not merge a concurrent one and could not
// be undone (ADR 0013).
func pipelineApplyCmd(o *options) *cobra.Command {
	var base, operation string
	cmd := &cobra.Command{
		Use:   "apply <pipeline-id>",
		Short: "apply one edit and print the diff and its inverse",
		Long: "--operation takes a dhole.v1.Operation as JSON, or @file, or @- for\n" +
			"stdin. The response carries the operation that undoes this one, which\n" +
			"is what makes undo a property of the API rather than of one client.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			if base == "" {
				return &usageError{cmd: cmd, err: fmt.Errorf(
					"--base is required: an edit that cannot conflict overwrites somebody else's silently")}
			}
			raw, err := readArgument(operation, cmd.InOrStdin())
			if err != nil {
				return err
			}
			op := &dholev1.Operation{}
			if err := protojson.Unmarshal(raw, op); err != nil {
				return fmt.Errorf("--operation is not a dhole.v1.Operation: %w", err)
			}

			ctx, cancel := o.context(cmd)
			defer cancel()
			res, err := o.client().ApplyOperation(ctx, connect.NewRequest(&dholev1.ApplyOperationRequest{
				PipelineId: args[0], BaseRevision: base, Operation: op,
			}))
			if err != nil {
				return o.fail("apply operation", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				printRevision(w, res.Msg.GetRevision())
				for _, change := range res.Msg.GetDiff().GetChanges() {
					_, _ = fmt.Fprintf(w, "%s %s\n", changeKind(change.GetKind()), change.GetSummary())
				}
				inverse, err := protojson.Marshal(res.Msg.GetInverse())
				if err == nil {
					_, _ = fmt.Fprintf(w, "undo with: --operation '%s'\n", inverse)
				}
			})
		},
	}
	cmd.Flags().StringVar(&base, "base", "", "the revision this edit was made against")
	cmd.Flags().StringVar(&operation, "operation", "", "dhole.v1.Operation as JSON, @file or @-")
	return cmd
}

// pipelineValidateCmd asks for structured diagnostics.
func pipelineValidateCmd(o *options) *cobra.Command {
	var revision, file string
	cmd := &cobra.Command{
		Use:   "validate [pipeline-id]",
		Short: "structured diagnostics for a revision or an unsaved definition",
		Long: "With --file, validates a definition that has not been saved, which is\n" +
			"what an editor and an agent both do while typing.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			req := &dholev1.ValidateRequest{RevisionId: revision}
			if len(args) == 1 {
				req.PipelineId = args[0]
			}
			if file != "" {
				raw, err := os.ReadFile(file) //nolint:gosec // the path is the user's own argument
				if err != nil {
					return fmt.Errorf("read %s: %w", file, err)
				}
				p, err := mirror.FromYAML(raw)
				if err != nil {
					return fmt.Errorf("parse %s: %w", file, err)
				}
				req.Pipeline = p
			}
			if req.GetPipelineId() == "" && req.GetPipeline() == nil {
				return &usageError{cmd: cmd, err: fmt.Errorf("a pipeline id or --file is required")}
			}

			ctx, cancel := o.context(cmd)
			defer cancel()
			res, err := o.client().Validate(ctx, connect.NewRequest(req))
			if err != nil {
				return o.fail("validate", err)
			}
			if err := o.emit(res.Msg, func(w io.Writer) {
				if len(res.Msg.GetDiagnostics()) == 0 {
					_, _ = fmt.Fprintln(w, "no diagnostics")
					return
				}
				for _, d := range res.Msg.GetDiagnostics() {
					_, _ = fmt.Fprintf(w, "%s %s %s: %s\n",
						d.GetSeverity(), d.GetStepId(), d.GetPort(), d.GetMessage())
				}
			}); err != nil {
				return err
			}
			// An error diagnostic is a failed validation, and a script has to
			// be able to tell without parsing prose.
			for _, d := range res.Msg.GetDiagnostics() {
				if strings.EqualFold(d.GetSeverity(), "error") {
					return fmt.Errorf("validation failed with %d diagnostic(s)", len(res.Msg.GetDiagnostics()))
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&revision, "revision", "", "revision to validate")
	cmd.Flags().StringVar(&file, "file", "", "validate this unsaved definition instead")
	return cmd
}

// pipelinePlanCmd is the dry run.
func pipelinePlanCmd(o *options) *cobra.Command {
	var revision string
	cmd := &cobra.Command{
		Use:   "plan <pipeline-id>",
		Short: "dry run: what would execute, what is cached, where each step lands",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().Plan(ctx, connect.NewRequest(&dholev1.PlanRequest{
				PipelineId: args[0], RevisionId: revision,
			}))
			if err != nil {
				return o.fail("plan", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				for _, step := range res.Msg.GetSteps() {
					state := "run"
					if step.GetCacheHit() {
						state = "cached"
					}
					_, _ = fmt.Fprintf(w, "%-8s %-20s %s", state, step.GetStepId(), step.GetEngineKind())
					if reason := step.GetNonCacheableReason(); reason != "" {
						_, _ = fmt.Fprintf(w, " (not cacheable: %s)", reason)
					}
					_, _ = fmt.Fprintln(w)
				}
			})
		},
	}
	cmd.Flags().StringVar(&revision, "revision", "", "revision to plan; default is the editing head")
	return cmd
}

// pipelineRevisionsCmd lists a pipeline's history.
func pipelineRevisionsCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "revisions <pipeline-id>",
		Short: "the pipeline's revision history, oldest first",
		Args:  exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().ListRevisions(ctx, connect.NewRequest(&dholev1.ListRevisionsRequest{
				PipelineId: args[0],
			}))
			if err != nil {
				return o.fail("list revisions", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				for _, rev := range res.Msg.GetRevisions() {
					printRevision(w, rev)
				}
			})
		},
	}
}

// pipelineApproveCmd promotes a revision to active.
func pipelineApproveCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "approve <revision-id>",
		Short: "promote a revision to active",
		Long: "The approver is the authenticated caller and is never taken from the\n" +
			"request, so there is no flag here to name somebody else.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().ApproveRevision(ctx, connect.NewRequest(&dholev1.ApproveRevisionRequest{
				RevisionId: args[0],
			}))
			if err != nil {
				return o.fail("approve revision", err)
			}
			return o.emit(res.Msg, func(w io.Writer) { printRevision(w, res.Msg.GetRevision()) })
		},
	}
}

// context is the deadline every RPC runs under.
func (o *options) context(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return context.WithTimeout(cmd.Context(), o.timeout)
}

func printRevision(w io.Writer, rev *dholev1.Revision) {
	if rev == nil {
		return
	}
	_, _ = fmt.Fprintf(w, "revision %s  pipeline %s  %s", rev.GetId(), rev.GetPipelineId(), rev.GetState())
	if rev.GetApprover() != "" {
		_, _ = fmt.Fprintf(w, "  approved by %s", rev.GetApprover())
	}
	_, _ = fmt.Fprintln(w)
}

func changeKind(k dholev1.ChangeKind) string {
	return strings.ToLower(strings.TrimPrefix(k.String(), "CHANGE_KIND_"))
}

// readArgument reads a flag that may be a literal, @file, or @- for stdin.
func readArgument(value string, stdin io.Reader) ([]byte, error) {
	switch {
	case value == "":
		return nil, fmt.Errorf("a value is required")
	case value == "@-":
		raw, err := io.ReadAll(stdin)
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		return raw, nil
	case strings.HasPrefix(value, "@"):
		path := strings.TrimPrefix(value, "@")
		raw, err := os.ReadFile(path) //nolint:gosec // the path is the user's own argument
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		return raw, nil
	default:
		return []byte(value), nil
	}
}
