// Package cli is the Dhole command line, and it is deliberately not a
// convenience wrapper over a favourite subset of the API.
//
// ADR 0013 says one contract serves the GUI, the CLI and agents equally, and
// that an editor-only affordance is a bug rather than a roadmap item. A CLI
// covering most of the API is how that decays: nobody decides to give the web
// app a privileged corner, it arrives one RPC at a time. So the command tree
// here is BUILT FROM THE PROTOBUF DESCRIPTORS (gen.go) rather than written
// out, coverage_test.go reads the same descriptors and fails on an RPC with
// no command, and an RPC nobody has written a command for still appears — as
// a stub that says so out loud.
//
// Three properties every command shares, because a CLI without them cannot be
// scripted or trusted:
//
//   - Credentials travel with every call. The API has no unauthenticated RPC;
//     a command that forgot the header would fail in production as a bare
//     code, so the refusal is surfaced with the server that gave it.
//   - Failure exits non-zero. A CLI that prints an error and exits 0 breaks
//     every script that uses it.
//   - `--output json` puts machine-readable output on stdout and NOTHING
//     else. Diagnostics go to stderr, always.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/version"
)

// RPCAnnotation marks a command as the CLI surface of one RPC. It is what the
// coverage test reads, and it is exported so that test can be written against
// the command tree rather than against a list of names kept beside it.
const RPCAnnotation = "dhole.rpc"

// The exit codes. Two rather than one, because "you typed something that does
// not exist" and "the thing you asked for failed" are different to a script.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
)

// defaultServer is where a control plane on this machine would be.
const defaultServer = "http://127.0.0.1:7777"

// Env is where the command tree writes. It exists so a test can run the whole
// CLI in-process and still see exactly what a terminal would.
type Env struct {
	Stdout io.Writer
	Stderr io.Writer
}

// options is the global state every command shares: which server, which
// credential, and in which shape the answer is wanted.
type options struct {
	env     Env
	server  string
	token   string
	output  string
	timeout time.Duration
}

// outputJSON is the machine-readable shape.
const outputJSON = "json"

// Main runs the CLI and returns the process's exit code. It never panics on
// bad input: an unknown command or flag is usage plus a non-zero code.
func Main(args []string, stdout, stderr io.Writer) int {
	root := Root(Env{Stdout: stdout, Stderr: stderr})
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)

	// ExecuteC rather than Execute, because the command that failed is the
	// one whose usage a person needs to see.
	cmd, err := root.ExecuteC()
	if err == nil {
		return exitOK
	}

	var usage *usageError
	switch {
	case errors.As(err, &usage):
		if usage.cmd != nil {
			cmd = usage.cmd
		}
	case strings.HasPrefix(err.Error(), "unknown command"):
		// Cobra refuses an unknown command before any Args check runs, so
		// this is the one usage case that has to be recognised by what cobra
		// says rather than by a wrapper of ours.
	default:
		_, _ = fmt.Fprintf(stderr, "dhole: %v\n", err)
		return exitError
	}
	if cmd != nil {
		_, _ = fmt.Fprint(stderr, cmd.UsageString())
	}
	_, _ = fmt.Fprintf(stderr, "dhole: %v\n", err)
	return exitUsage
}

// Root is the whole command tree.
func Root(env Env) *cobra.Command {
	if env.Stdout == nil {
		env.Stdout = io.Discard
	}
	if env.Stderr == nil {
		env.Stderr = io.Discard
	}
	o := &options{env: env, output: "text"}

	root := &cobra.Command{
		Use:   "dhole",
		Short: "Dhole: pipelines that run the same on a laptop and in a cluster",
		Long: "dhole drives the whole Dhole API. Every RPC the GUI can call has a\n" +
			"command here, because the contract is one contract (ADR 0013).",
		// `dhole --version` answered before this package existed, and a
		// script that greps for it is not required to notice a rewrite.
		Version:       fmt.Sprintf("%s (%s)", version.Version(), version.Commit()),
		SilenceUsage:  true,
		SilenceErrors: true,
		// A bare `dhole` is a request for help, not a failure; a `dhole
		// nonesuch` is caught by cobra before this runs.
		RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}

	flags := root.PersistentFlags()
	flags.StringVar(&o.server, "server", envOr("DHOLE_SERVER", defaultServer),
		"control plane to talk to (DHOLE_SERVER)")
	flags.StringVar(&o.token, "token", os.Getenv("DHOLE_TOKEN"),
		"bearer credential; this API has no unauthenticated call (DHOLE_TOKEN)")
	flags.StringVar(&o.output, "output", "text", "text or json")
	flags.DurationVar(&o.timeout, "timeout", 60*time.Second, "how long to wait for the server")

	// A mistyped flag is usage, not a crash, wherever it appears.
	root.SetVersionTemplate("dhole {{.Version}}\n")

	root.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		return &usageError{cmd: cmd, err: err}
	})

	// The contract first: these commands are derived from the descriptors, so
	// what the API can do is what the CLI can do.
	for _, cmd := range contractCommands(o) {
		root.AddCommand(cmd)
	}
	// Then the surfaces that are not one RPC each.
	attachExtras(root, o)

	return root
}

// usageError is a mistake in what was typed rather than a failure of what was
// asked for. It carries the command whose usage should be shown.
type usageError struct {
	cmd *cobra.Command
	err error
}

func (e *usageError) Error() string { return e.err.Error() }
func (e *usageError) Unwrap() error { return e.err }

// exactArgs is cobra's argument check, reported as usage so the exit code and
// the printed help match what a mistyped command deserves.
func exactArgs(n int) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) == n {
			return nil
		}
		return &usageError{cmd: cmd, err: fmt.Errorf(
			"%s takes %d argument(s), got %d", cmd.CommandPath(), n, len(args))}
	}
}

// noArgs is the same for a command that is only a group of subcommands: it is
// how `dhole pipeline nonesuch` becomes usage instead of silence.
func noArgs(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	return &usageError{cmd: cmd, err: fmt.Errorf(
		"unknown command %q for %q", args[0], cmd.CommandPath())}
}

// client builds a PipelineService client that carries the credential on every
// call, unary and streaming alike.
func (o *options) client() dholev1connect.PipelineServiceClient {
	return dholev1connect.NewPipelineServiceClient(
		newHTTPClient(), o.server, connect.WithInterceptors(bearer{token: o.token}))
}

// engineClient builds an EngineService client on the same server, with the
// same credential. It is a second generated client rather than a second
// notion of "the server": the contract declares two services and neither is
// more the API than the other.
func (o *options) engineClient() dholev1connect.EngineServiceClient {
	return dholev1connect.NewEngineServiceClient(
		newHTTPClient(), o.server, connect.WithInterceptors(bearer{token: o.token}))
}

// newHTTPClient is the transport every command shares. It is plain HTTP/1.1
// with the default timeouts: the per-call deadline comes from --timeout, so a
// second one here would only make the reason for a hang harder to find.
func newHTTPClient() *http.Client { return &http.Client{} }

// bearer puts the credential on the request.
//
// It is an interceptor rather than a line in each command because "each
// command remembers" is precisely the thing that stops being true.
type bearer struct{ token string }

var _ connect.Interceptor = bearer{}

func (b bearer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if req.Spec().IsClient {
			b.apply(req.Header())
		}
		return next(ctx, req)
	}
}

func (b bearer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		b.apply(conn.RequestHeader())
		return conn
	}
}

// WrapStreamingHandler is the server half, which this client never runs.
func (b bearer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func (b bearer) apply(header http.Header) {
	if b.token == "" {
		// Deliberately nothing: the server refuses the call, and its refusal
		// is the honest one to show. A client-side guess about what the
		// server would accept is how a CLI ends up refusing a credential the
		// server would have taken.
		return
	}
	header.Set("Authorization", "Bearer "+b.token)
}

// emit writes the answer: the protobuf response as JSON when asked, and the
// human rendering otherwise. Nothing else is ever written to stdout.
func (o *options) emit(msg proto.Message, human func(w io.Writer)) error {
	if o.output == outputJSON {
		raw, err := protojson.Marshal(msg)
		if err != nil {
			return fmt.Errorf("render json: %w", err)
		}
		_, err = fmt.Fprintf(o.env.Stdout, "%s\n", raw)
		return err
	}
	human(o.env.Stdout)
	return nil
}

// fail turns an RPC failure into something a person can act on: which server
// refused, which code, and the server's own message — plus, when no credential
// was sent at all, the reason that is almost always the answer.
func (o *options) fail(what string, err error) error {
	if err == nil {
		return nil
	}
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		msg := fmt.Sprintf("%s: %s: %s: %s", what, o.server, cerr.Code(), cerr.Message())
		if cerr.Code() == connect.CodeUnauthenticated && o.token == "" {
			msg += "\n  no credential was sent: pass --token or set DHOLE_TOKEN"
		}
		return errors.New(msg)
	}
	return fmt.Errorf("%s: %s: %w", what, o.server, err)
}

// checkOutput refuses an output shape that does not exist, rather than
// silently printing text to a script expecting JSON.
func (o *options) checkOutput(cmd *cobra.Command) error {
	switch o.output {
	case "text", outputJSON:
		return nil
	default:
		return &usageError{cmd: cmd, err: fmt.Errorf(
			"unknown --output %q: text or json", o.output)}
	}
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
