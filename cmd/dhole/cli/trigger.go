package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// triggerCreateCmd stores an event source through the contract.
//
// It exists in this shape rather than as a command that edits the plane's
// `--triggers` file because a trigger only the holder of that file could
// create is a capability the GUI and an agent can never have (ADR 0013). It
// sends the same request the canvas would.
func triggerCreateCmd(o *options) *cobra.Command {
	var (
		kind       string
		pipelineID string
		expression string
		secret     string
		untrusted  bool
		bindings   []string
	)
	cmd := &cobra.Command{
		Use:   "create <trigger-id>",
		Short: "create an event source that starts runs of a pipeline",
		Long: "A trigger does not start a pipeline: it supplies that pipeline's\n" +
			"declared inputs, and the run follows. --bind maps one of those\n" +
			"inputs to a field of this trigger's own event, and a binding naming\n" +
			"an input the pipeline does not declare is refused HERE rather than\n" +
			"inside the first step of a run at 3am.\n" +
			"The pipeline needs an active revision: a trigger drives whatever is\n" +
			"active when it fires, never what was current when it was created.\n" +
			"A trigger this plane DECLARES in its --triggers file wins, and\n" +
			"creating one under that id is refused.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			mapping, err := parseBindings(bindings)
			if err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().CreateTrigger(ctx, connect.NewRequest(&dholev1.CreateTriggerRequest{
				Trigger: &dholev1.Trigger{
					Id:           args[0],
					Kind:         kind,
					PipelineId:   pipelineID,
					InputMapping: mapping,
					Expression:   expression,
					Secret:       secret,
					Untrusted:    untrusted,
				},
			}))
			if err != nil {
				return o.fail("create trigger", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				_, _ = fmt.Fprintf(w, "created %s\n", describeTrigger(res.Msg.GetTrigger()))
			})
		},
	}
	cmd.Flags().StringVar(&kind, "kind", "", "schedule, http or git")
	cmd.Flags().StringVar(&pipelineID, "pipeline", "", "the pipeline this trigger drives")
	cmd.Flags().StringVar(&expression, "expression", "",
		"a five- or six-field cron expression (schedules only)")
	cmd.Flags().StringVar(&secret, "secret", "",
		"the shared secret the forge signs with (git triggers; required for them)")
	cmd.Flags().BoolVar(&untrusted, "untrusted", false,
		"mark every value this trigger produces as tainted")
	cmd.Flags().StringArrayVar(&bindings, "bind", nil,
		"input=event_field, repeatable: which of the pipeline's inputs this trigger fills")
	return cmd
}

// triggerListCmd shows what this tenant's triggers are, stored and declared.
func triggerListCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "list this tenant's triggers",
		Long: "Both the stored triggers and the ones the plane's own --triggers\n" +
			"file declares, marked `declared`. Both, because an operator\n" +
			"mid-migration has some of each and an invisible trigger that starts\n" +
			"runs is the worst kind.\n" +
			"No secret is ever returned; a trigger that holds one says so.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().ListTriggers(ctx,
				connect.NewRequest(&dholev1.ListTriggersRequest{}))
			if err != nil {
				return o.fail("list triggers", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				if len(res.Msg.GetTriggers()) == 0 {
					_, _ = fmt.Fprintln(w, "this tenant has no triggers")
					return
				}
				for _, t := range res.Msg.GetTriggers() {
					_, _ = fmt.Fprintln(w, describeTrigger(t))
				}
			})
		},
	}
}

// triggerDeleteCmd removes one stored trigger.
func triggerDeleteCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <trigger-id>",
		Short: "remove a stored trigger",
		Long: "A DECLARED trigger is refused: it lives in the plane's --triggers\n" +
			"file, so a delete would appear to work and the trigger would be back\n" +
			"at the next restart.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().DeleteTrigger(ctx, connect.NewRequest(&dholev1.DeleteTriggerRequest{
				TriggerId: args[0],
			}))
			if err != nil {
				return o.fail("delete trigger", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				_, _ = fmt.Fprintf(w, "deleted %s\n", args[0])
			})
		},
	}
}

// parseBindings turns --bind input=field into the mapping the contract takes.
// The pipeline's input is the key, because the pipeline's vocabulary is the
// stable one and the trigger's is what varies per kind.
func parseBindings(bindings []string) (map[string]string, error) {
	if len(bindings) == 0 {
		return nil, nil
	}
	mapping := make(map[string]string, len(bindings))
	for _, binding := range bindings {
		input, field, ok := strings.Cut(binding, "=")
		if !ok || input == "" || field == "" {
			return nil, fmt.Errorf(
				"--bind %q is not input=event_field", binding)
		}
		mapping[input] = field
	}
	return mapping, nil
}

// describeTrigger is one line of `trigger list`.
func describeTrigger(t *dholev1.Trigger) string {
	parts := []string{fmt.Sprintf("%-20s %-9s %s", t.GetId(), t.GetKind(), t.GetPipelineId())}
	if t.GetExpression() != "" {
		parts = append(parts, "expression="+t.GetExpression())
	}
	if mapping := t.GetInputMapping(); len(mapping) > 0 {
		names := make([]string, 0, len(mapping))
		for input, field := range mapping {
			names = append(names, input+"="+field)
		}
		sort.Strings(names)
		parts = append(parts, "bind "+strings.Join(names, ","))
	}
	if t.GetUntrusted() {
		parts = append(parts, "untrusted")
	}
	if t.GetHasSecret() {
		// The secret itself never travels; that it exists does, because a git
		// trigger without one is refused and an operator needs to see which
		// of theirs holds what.
		parts = append(parts, "signed")
	}
	if t.GetDeclared() {
		parts = append(parts, "declared")
	}
	return strings.Join(parts, "  ")
}
