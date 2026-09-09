package cli

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	// Imported for the side effect of registering the dhole.v1 descriptors,
	// which is what the command tree below is generated from.
	_ "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// wireProtoPackage is the protobuf package that IS the public contract. Every
// service in it, in whichever file, has to be reachable from the CLI.
const wireProtoPackage = "dhole.v1"

// surface is the hand-written command that serves one RPC: which group it
// belongs under, and how to build it.
type surface struct {
	group string
	build func(*options) *cobra.Command
}

// surfaces maps RPC name to the command that serves it.
//
// This map is an implementation, not the source of truth: the LIST of RPCs is
// read from the descriptors below, and an RPC missing from this map does not
// quietly disappear — it becomes a stub that refuses, and it fails
// coverage_test.go until somebody writes it a real command. That asymmetry is
// the whole point. A CLI whose surface was this map alone would keep passing
// its own tests while the API grew a corner it could not reach.
func surfaces() map[string]surface {
	return map[string]surface{
		"CreatePipeline":  {group: groupPipeline, build: pipelineCreateCmd},
		"GetPipeline":     {group: groupPipeline, build: pipelineGetCmd},
		"ApplyOperation":  {group: groupPipeline, build: pipelineApplyCmd},
		"Validate":        {group: groupPipeline, build: pipelineValidateCmd},
		"Plan":            {group: groupPipeline, build: pipelinePlanCmd},
		"ListRevisions":   {group: groupPipeline, build: pipelineRevisionsCmd},
		"ApproveRevision": {group: groupPipeline, build: pipelineApproveCmd},
		"StartRun":        {group: groupRun, build: runStartCmd},
		"WatchRun":        {group: groupRun, build: runWatchCmd},
	}
}

// The command groups the contract's RPCs land in.
const (
	groupPipeline = "pipeline"
	groupRun      = "run"
)

// groupHelp is what each group says for itself.
var groupHelp = map[string]string{
	groupPipeline: "read, edit, validate, plan and approve pipeline definitions",
	groupRun:      "start, follow and inspect runs",
}

// contractCommands builds one command per RPC of every dhole.v1 service, from
// the descriptors.
//
// Generating the tree from the descriptors rather than writing it out is what
// makes parity structural: an RPC added to the contract appears here on the
// next build, either as its hand-written command or — if nobody wrote one — as
// a stub under `dhole api` that names itself and refuses. Either way it is
// visible in `dhole --help`, which is the opposite of an RPC only the GUI
// knows about.
func contractCommands(o *options) []*cobra.Command {
	groups := map[string]*cobra.Command{}
	var order []string
	var stubs []*cobra.Command

	for _, svc := range contractServices() {
		methods := svc.Methods()
		for i := range methods.Len() {
			method := methods.Get(i)
			name := string(method.Name())

			s, ok := surfaces()[name]
			if !ok {
				stubs = append(stubs, stubCommand(method))
				continue
			}
			group, seen := groups[s.group]
			if !seen {
				group = &cobra.Command{
					Use:   s.group,
					Short: groupHelp[s.group],
					Args:  noArgs,
					RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
				}
				groups[s.group] = group
				order = append(order, s.group)
			}
			cmd := s.build(o)
			if cmd.Annotations == nil {
				cmd.Annotations = map[string]string{}
			}
			cmd.Annotations[RPCAnnotation] = name
			group.AddCommand(cmd)
		}
	}

	sort.Strings(order)
	out := make([]*cobra.Command, 0, len(order)+1)
	for _, name := range order {
		out = append(out, groups[name])
	}
	if len(stubs) > 0 {
		api := &cobra.Command{
			Use:   "api",
			Short: "RPCs the contract declares that have no command yet",
			Long: "Every RPC below is part of the API and has no CLI surface, which\n" +
				"ADR 0013 does not allow to stand. They are listed here so the gap is\n" +
				"visible rather than silent; each one refuses when run.",
			Args: noArgs,
			RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
		}
		api.AddCommand(stubs...)
		out = append(out, api)
	}
	return out
}

// stubCommand is what an RPC with no hand-written command gets: a command that
// exists, documents the gap, and fails.
//
// It fails rather than doing something approximate because a stub that quietly
// succeeded would be worse than the missing command it stands in for.
func stubCommand(method protoreflect.MethodDescriptor) *cobra.Command {
	name := string(method.Name())
	return &cobra.Command{
		Use:   name,
		Short: fmt.Sprintf("%s — declared by the contract, not yet served by the CLI", method.FullName()),
		Args:  cobra.ArbitraryArgs,
		RunE: func(*cobra.Command, []string) error {
			return fmt.Errorf(
				"%s has no CLI command yet: the contract declares it (%s -> %s) and ADR 0013 "+
					"says the CLI must be able to call it; add a surface in cmd/dhole/cli",
				name, method.Input().FullName(), method.Output().FullName())
		},
	}
}

// contractServices is every service declared in the dhole.v1 package, sorted
// so the command tree is stable between builds.
func contractServices() []protoreflect.ServiceDescriptor {
	var out []protoreflect.ServiceDescriptor
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if fd.Package() != wireProtoPackage {
			return true
		}
		services := fd.Services()
		for i := range services.Len() {
			out = append(out, services.Get(i))
		}
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].FullName() < out[j].FullName() })
	return out
}
