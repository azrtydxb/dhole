package cli

import (
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// pluginPublishCmd puts a declaration in the catalog.
//
// It is the CLI half of PublishPlugin, and it exists in this shape rather than
// as a command that writes the control plane's database because a publish only
// a process holding that file could perform is a capability the GUI and an
// agent can never have (ADR 0013). It sends the same request the canvas would.
func pluginPublishCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "publish <manifest>",
		Short: "publish a plugin's declaration to the catalog",
		Long: "The manifest is a dhole.v1.Plugin as JSON, or @file, or @- for\n" +
			"stdin. Its namespace, name and version ARE its identity, so the\n" +
			"`ref` field is ignored; the tenant is the credential's and is never\n" +
			"taken from the manifest.\n" +
			"A version is immutable: republishing identical bytes is a no-op, so\n" +
			"a deploy can be retried, and republishing different bytes under the\n" +
			"same version is refused.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			raw, err := readArgument(args[0], cmd.InOrStdin())
			if err != nil {
				return err
			}
			manifest := &dholev1.Plugin{}
			if err := protojson.Unmarshal(raw, manifest); err != nil {
				return fmt.Errorf("the manifest is not a dhole.v1.Plugin: %w", err)
			}

			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().PublishPlugin(ctx, connect.NewRequest(&dholev1.PublishPluginRequest{
				Plugin: manifest,
			}))
			if err != nil {
				return o.fail("publish plugin", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				p := res.Msg.GetPlugin()
				_, _ = fmt.Fprintf(w, "published %s  %s  %s\n",
					p.GetRef(), p.GetKind(), p.GetEffectClass())
			})
		},
	}
}

// pluginGetCmd reads what one published plugin declares.
//
// It is the same call the properties panel makes and the same one an agent
// would make to discover what a step type takes: plugin schemas drive the
// panel, request validation, tool discovery and editor autocomplete from one
// source (ADR 0012), and a source only one client could reach would not be
// that source for long.
func pluginGetCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "get <namespace/name@version>",
		Short: "read a published plugin's declaration",
		Long: "A version is mandatory. Dispatch resolves to an exact version and\n" +
			"digest, so a floating reference has no meaning here and picking\n" +
			"\"the latest\" would make the lockfile a suggestion.\n" +
			"With --output json the whole declaration is printed, schemas included.",
		Args: exactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			ctx, cancel := o.context(cmd)
			defer cancel()

			res, err := o.client().GetPlugin(ctx, connect.NewRequest(&dholev1.GetPluginRequest{
				PluginRef: args[0],
			}))
			if err != nil {
				return o.fail("get plugin", err)
			}
			return o.emit(res.Msg, func(w io.Writer) {
				p := res.Msg.GetPlugin()
				_, _ = fmt.Fprintf(w, "%s  %s  %s  %s\n",
					p.GetRef(), p.GetKind(), p.GetEffectClass(),
					p.GetDigest().GetAlgo()+":"+p.GetDigest().GetHex())
				_, _ = fmt.Fprintf(w, "capabilities: %s\n", capabilityNames(p.GetCapabilities()))
				if len(p.GetEngineTypes()) > 0 {
					_, _ = fmt.Fprintf(w, "engine types: %s\n", strings.Join(p.GetEngineTypes(), ", "))
				}
				if p.GetInputSchema() != "" {
					_, _ = fmt.Fprintf(w, "input schema:\n%s\n", p.GetInputSchema())
				}
				if p.GetOutputSchema() != "" {
					_, _ = fmt.Fprintf(w, "output schema:\n%s\n", p.GetOutputSchema())
				}
			})
		},
	}
}
