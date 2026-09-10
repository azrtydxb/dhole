package cli

import (
	"fmt"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// runApproveCmd decides an approval gate a run is waiting at.
//
// It takes no approver. The approver is whoever the credential this command
// sends authenticates as, because an approval attributed to a name typed on a
// command line is an approval anybody can forge — and "who approved this" is
// the only question the trail is ever asked.
//
// Not to be confused with `dhole pipeline approve`, which promotes a
// definition to active before anything runs. This decides a gate a RUN has
// stopped at.
func runApproveCmd(o *options) *cobra.Command {
	var deny bool
	cmd := &cobra.Command{
		Use:   "approve <run-id> <step-id>",
		Short: "decide an approval gate a run is waiting at",
		Long: "The step is named because a run may hold more than one gate. The\n" +
			"decision is recorded against the principal this credential\n" +
			"authenticates as, and a gate that already has one is refused rather\n" +
			"than decided twice: the two decisions may disagree.\n" +
			"With --deny the run fails, naming the approver and the step.",
		Args: exactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().DecideApproval(ctx, connect.NewRequest(&dholev1.DecideApprovalRequest{
				RunId: args[0], StepId: args[1], Approved: !deny,
			}))
			if err != nil {
				return o.fail("decide approval", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				verdict := "approved"
				if !res.Msg.GetApproved() {
					verdict = "denied"
				}
				_, _ = fmt.Fprintf(w, "%s/%s %s by %s\n",
					res.Msg.GetRunId(), res.Msg.GetStepId(), verdict, res.Msg.GetApprover())
			})
		},
	}
	cmd.Flags().BoolVar(&deny, "deny", false, "refuse the gate, failing the run")
	return cmd
}
