package cli

import (
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// engineListCmd lists the live fleet.
//
// It goes through EngineService and not through internal/registry, which the
// binary also contains. A CLI able to read the fleet while the GUI and an
// agent cannot is the same failure as a GUI-only endpoint seen from the other
// side (ADR 0013), and it is why this command spent a task refusing rather
// than reaching around the API.
func engineListCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "list the engines registered for this tenant",
		Long: "The fleet is live state, not a roster: an engine that stops\n" +
			"heartbeating disappears from it, and a draining one is still here.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.engineClient().ListEngines(ctx,
				connect.NewRequest(&dholev1.ListEnginesRequest{}))
			if err != nil {
				return o.fail("list engines", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				if len(res.Msg.GetEngines()) == 0 {
					_, _ = fmt.Fprintln(w, "no engines are registered")
					return
				}
				for _, e := range res.Msg.GetEngines() {
					_, _ = fmt.Fprintf(w, "%-24s %-12s %s/%s  %d slot(s)  %d in flight  %s\n",
						e.GetId(), e.GetState(), e.GetOs(), e.GetArch(), e.GetSlots(),
						len(e.GetInFlight()), capabilityNames(e.GetCapabilities()))
				}
			})
		},
	}
}

// engineDrainCmd stops new work reaching one engine.
func engineDrainCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "drain <engine-id>",
		Short: "stop giving an engine new work",
		Long: "A drain kills nothing. The engine stays in the fleet, alive and\n" +
			"heartbeating, until its last job finishes — which is what makes a\n" +
			"rolling upgrade something other than an outage.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.engineClient().DrainEngine(ctx,
				connect.NewRequest(&dholev1.DrainEngineRequest{EngineId: args[0]}))
			if err != nil {
				return o.fail("drain engine", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				engine := res.Msg.GetEngine()
				if engine == nil {
					// The registry no longer holds it, which is the outcome
					// the operator wanted and not an error.
					_, _ = fmt.Fprintf(w, "engine %s is no longer in the fleet\n", args[0])
					return
				}
				_, _ = fmt.Fprintf(w, "engine %s is %s, holding %d job(s)\n",
					engine.GetId(), engine.GetState(), len(engine.GetInFlight()))
			})
		},
	}
}

// capabilityNames renders a capability set the way the contract names it.
func capabilityNames(caps []dholev1.Capability) string {
	if len(caps) == 0 {
		return "no capabilities"
	}
	out := make([]string, 0, len(caps))
	for _, c := range caps {
		out = append(out, c.String())
	}
	return strings.Join(out, ",")
}
