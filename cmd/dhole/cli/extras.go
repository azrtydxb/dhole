package cli

import (
	"github.com/spf13/cobra"
)

// attachExtras adds everything that is not one RPC each: the second views of
// an RPC (`run logs`), the local tools that talk to no server (`local run`,
// `policy test`), and the two commands the binary had before this package
// existed (`serve`, `version`).
//
// It used to add `run cancel` and `engine` as commands that existed and
// refused, because the contract declared no CancelRun and no EngineService.
// It declares both now, so those commands are generated from the descriptors
// like every other one.
func attachExtras(root *cobra.Command, o *options) {
	if run := findCommand(root, groupRun); run != nil {
		run.AddCommand(runLogsCmd(o))
	}
	root.AddCommand(
		policyCmd(o),
		localCmd(o),
		serveCmd(o),
		tokenCmd(o),
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
