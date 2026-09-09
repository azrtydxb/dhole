// Package cli_test holds the test that this whole package exists to satisfy.
//
// ADR 0013 says the GUI has no privileged endpoints: it is one client of the
// contract among the CLI and agents. That claim decays quietly. Nobody ever
// decides to give the web app a corner of the API the CLI cannot reach; it
// happens one RPC at a time, each of them "GUI-only for now". The test below
// is the mechanism that makes it stop happening: it reads the PROTOBUF
// DESCRIPTORS, not a list anybody maintains here, so an RPC added to the
// contract tomorrow fails this package until somebody gives it a command.
package cli_test

import (
	"bytes"
	"sort"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	// Imported for its side effect of registering dhole.v1 in the global
	// descriptor registry, which is what the scan below reads.
	_ "github.com/azrtydxb/dhole/gen/dhole/v1"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

// wireProtoPackage is the protobuf package that IS the public contract.
const wireProtoPackage = "dhole.v1"

// TestCLICoversEveryRPC requires a command for every RPC of every service in
// the contract — today PipelineService, which is where the run RPCs live too
// (StartRun and WatchRun are methods of it; there is no RunService).
//
// The list of RPCs comes from protoregistry, so this test has never heard of
// any particular RPC and cannot go stale.
func TestCLICoversEveryRPC(t *testing.T) {
	services := contractServices(t)
	require.NotEmpty(t, services, "no dhole.v1 service descriptors were found at all")

	root := cli.Root(cli.Env{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})
	covered := coveredRPCs(root)

	missing := uncovered(services, covered)
	require.Empty(t, missing, "these RPCs have no CLI command, so the API has a corner the "+
		"CLI and agents cannot reach (ADR 0013):\n  %s", strings.Join(missing, "\n  "))

	// The reverse direction: an annotation naming an RPC that does not exist
	// would let a typo count as coverage forever.
	declared := map[string]bool{}
	for _, svc := range services {
		methods := svc.Methods()
		for i := range methods.Len() {
			declared[string(methods.Get(i).Name())] = true
		}
	}
	for rpc, cmds := range covered {
		require.True(t, declared[rpc],
			"command %q claims to serve RPC %q, which is in no dhole.v1 service", cmds[0], rpc)
	}
}

// TestCoverageIsDescriptorDrivenNotAList proves the check above notices an RPC
// it has never heard of.
//
// A synthetic service is compiled here and fed to the same uncovered() the
// real test uses. If coverage were ever reduced to a hand-written list of
// today's eight RPCs, this test would keep passing for that list and fail
// here — which is the point of writing it.
func TestCoverageIsDescriptorDrivenNotAList(t *testing.T) {
	svc := syntheticService(t)
	root := cli.Root(cli.Env{Stdout: &bytes.Buffer{}, Stderr: &bytes.Buffer{}})

	missing := uncovered([]protoreflect.ServiceDescriptor{svc}, coveredRPCs(root))
	require.Equal(t, []string{"dhole.v1.InventedService.InventedRPC"}, missing,
		"an RPC the CLI has never heard of must be reported as uncovered")
}

// contractServices is every service in the dhole.v1 package, whichever file it
// was declared in — so a RunService added later in a new .proto is picked up
// without this test being edited.
func contractServices(t *testing.T) []protoreflect.ServiceDescriptor {
	t.Helper()
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

// coveredRPCs maps an RPC name to the command paths that serve it.
func coveredRPCs(root *cobra.Command) map[string][]string {
	covered := map[string][]string{}
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		if rpc := c.Annotations[cli.RPCAnnotation]; rpc != "" {
			covered[rpc] = append(covered[rpc], c.CommandPath())
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(root)
	return covered
}

// uncovered names every RPC with no command, fully qualified so the failure
// says which service is short.
func uncovered(services []protoreflect.ServiceDescriptor, covered map[string][]string) []string {
	var missing []string
	for _, svc := range services {
		methods := svc.Methods()
		for i := range methods.Len() {
			m := methods.Get(i)
			if len(covered[string(m.Name())]) == 0 {
				missing = append(missing, string(m.FullName()))
			}
		}
	}
	sort.Strings(missing)
	return missing
}

// syntheticService compiles a service that exists nowhere in the tree.
func syntheticService(t *testing.T) protoreflect.ServiceDescriptor {
	t.Helper()
	str := func(s string) *string { return &s }
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    str("dhole/v1/invented_test.proto"),
		Package: str(wireProtoPackage),
		Syntax:  str("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: str("InventedRequest")},
			{Name: str("InventedResponse")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: str("InventedService"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name:       str("InventedRPC"),
				InputType:  str(".dhole.v1.InventedRequest"),
				OutputType: str(".dhole.v1.InventedResponse"),
			}},
		}},
	}, nil)
	require.NoError(t, err)
	return fd.Services().Get(0)
}
