package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

// attachExtras adds everything that is not one RPC each: the second views of
// an RPC (`run logs`), the surfaces the contract does not have yet and which
// say so (`run cancel`, `engine`), the local tools that talk to no server
// (`local run`, `policy test`), and the two commands the binary had before
// this package existed (`serve`, `version`).
func attachExtras(root *cobra.Command, o *options) {
	if run := findCommand(root, groupRun); run != nil {
		run.AddCommand(runLogsCmd(o), runCancelCmd(o))
	}
	root.AddCommand(
		engineCmd(o),
		policyCmd(o),
		localCmd(o),
		serveCmd(o),
		versionCmd(o),
	)
}

// findCommand returns the child of root with this name, if it has one.
func findCommand(root *cobra.Command, name string) *cobra.Command {
	for _, cmd := range root.Commands() {
		if cmd.Name() == name {
			return cmd
		}
	}
	return nil
}

// engineCmd is the fleet, which the contract does not expose yet.
//
// The commands exist and refuse. They could each read internal/registry
// directly — the binary contains it — and that is precisely what must not
// happen: a CLI that can list engines while the GUI and an agent cannot is the
// same failure as a GUI-only endpoint, seen from the other side (ADR 0013).
// The gap belongs in the proto, and until it is there this says so.
func engineCmd(_ *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "engine",
		Short: "the engine fleet (no EngineService in the contract yet)",
		Args:  noArgs,
		RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	cmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "list registered engines",
			Args:  cobra.NoArgs,
			RunE: func(*cobra.Command, []string) error {
				return errNoEngineService("listing engines", "ListEngines")
			},
		},
		&cobra.Command{
			Use:   "drain <engine-id>",
			Short: "stop giving an engine new work",
			Args:  exactArgs(1),
			RunE: func(_ *cobra.Command, args []string) error {
				return errNoEngineService("draining "+args[0], "DrainEngine")
			},
		},
	)
	return cmd
}

func errNoEngineService(what, rpc string) error {
	return fmt.Errorf(
		"%s needs an RPC the contract does not declare: there is no dhole.v1 %s. "+
			"The CLI will not read the fleet registry behind the API's back (ADR 0013); "+
			"add the RPC and this command follows from the descriptors", what, rpc)
}
