package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/policy"
)

// policyCmd is the policy author's half of ADR 0012.
func policyCmd(o *options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "policy",
		Short: "work with trust-tier policy",
		Args:  noArgs,
		RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	cmd.AddCommand(policyTestCmd(o), policyDefaultCmd(o))
	return cmd
}

// policyDefaultCmd prints the dispatch policy `dhole serve` runs when it is
// given none (ADR 0032), exactly as the binary embeds it. It is the document an
// operator copies, tightens, tests with `dhole policy test` and hands back to
// `dhole serve --policy`.
func policyDefaultCmd(o *options) *cobra.Command {
	return &cobra.Command{
		Use:   "default",
		Short: "print the built-in dispatch policy dhole serve runs without --policy",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			_, err := o.env.Stdout.Write(policy.DefaultDocument())
			return err
		},
	}
}

// policyDecision is the answer, in the shape a script reads.
type policyDecision struct {
	Allow  bool   `json:"allow"`
	Rule   string `json:"rule,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// policyTestCmd evaluates a policy against one hypothetical step, locally.
//
// It talks to no server on purpose: this is the loop a policy author needs
// before the policy is anywhere near a deployment, and a rule that only fails
// when a real pipeline is refused is a rule nobody will write carefully. The
// evaluation is internal/policy's own CEL engine, not a re-implementation, so
// what this command says is what the control plane would decide.
func policyTestCmd(o *options) *cobra.Command {
	var (
		file        string
		tier        string
		tenant      string
		subject     string
		pluginRef   string
		effect      string
		capsFlag    []string
		taintFlag   []string
		engineCaps  []string
		signed      bool
		upstream    string
		secretName  string
		expectAllow bool
	)
	cmd := &cobra.Command{
		Use:   "test --policy <file>",
		Short: "evaluate a tier policy against one hypothetical step",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := o.checkOutput(cmd); err != nil {
				return err
			}
			if file == "" {
				return &usageError{cmd: cmd, err: fmt.Errorf("--policy is required")}
			}
			raw, err := os.ReadFile(file) //nolint:gosec // the path is the user's own argument
			if err != nil {
				return fmt.Errorf("read %s: %w", file, err)
			}
			// The reader `dhole serve --policy` uses, so what is tested here
			// is what the plane would load.
			parsed, err := policy.ParseDocument(raw)
			if err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
			tierPolicy, err := parsed.TierPolicy()
			if err != nil {
				// A policy that does not compile is a message to its author,
				// here, rather than an outage shaped like a deny later.
				return fmt.Errorf("%s: %w", file, err)
			}
			source := policy.NewStaticSource()
			if err := source.Set(tier, tierPolicy); err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
			engine, err := policy.New(source, policy.DiscardAudit{})
			if err != nil {
				return err
			}

			caps, err := capabilities(capsFlag)
			if err != nil {
				return &usageError{cmd: cmd, err: err}
			}
			class, err := effectClass(effect)
			if err != nil {
				return &usageError{cmd: cmd, err: err}
			}
			engineCapabilities, err := capabilities(engineCaps)
			if err != nil {
				return &usageError{cmd: cmd, err: err}
			}
			if upstream == "" && parsed.Defaults != nil {
				upstream = parsed.Defaults.Upstream
			}

			decision, err := engine.Evaluate(cmd.Context(), policy.Input{
				Tier:         tier,
				TenantID:     tenant,
				Subject:      subject,
				Capabilities: caps,
				EffectClass:  class,
				PluginRef:    pluginRef,
				Signed:       signed,
				Upstream:     upstream,
				// A dispatch is tainted exactly when something admitted the
				// data, so naming a source is what makes it so: a --tainted
				// flag separate from its sources could say untrusted-by-nobody,
				// which no run can produce.
				Tainted:            len(taintFlag) > 0,
				TaintSources:       taintFlag,
				EngineCapabilities: engineCapabilities,
				SecretName:         secretName,
			})
			if err != nil {
				return err
			}

			if err := o.emitDecision(policyDecision{
				Allow: decision.Allow, Rule: decision.Rule, Reason: decision.Reason,
			}); err != nil {
				return err
			}
			// The exit code IS the answer here: a policy test that always
			// exited zero could not gate anything.
			if decision.Allow != expectAllow {
				return fmt.Errorf("policy %s the step; --expect said %s",
					verdict(decision.Allow), verdict(expectAllow))
			}
			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&file, "policy", "", "policy file: revision plus an ordered list of CEL rules")
	flags.StringVar(&tier, "tier", "trusted", "trust tier the policy belongs to")
	flags.StringVar(&tenant, "tenant", "default", "tenant the decision is scoped to")
	flags.StringVar(&subject, "subject", "step", "what is being decided: a step, a plugin, a secret")
	flags.StringVar(&pluginRef, "plugin-ref", "", "plugin reference the step names")
	flags.StringVar(&effect, "effect-class", "PURE", "effect class the step operates under")
	flags.StringSliceVar(&capsFlag, "capability", nil, "capability the plugin manifest declares; repeatable")
	flags.StringSliceVar(&taintFlag, "taint-source", nil,
		"trigger that admitted untrusted data into this step; repeatable, and naming any "+
			"makes the step tainted")
	flags.StringSliceVar(&engineCaps, "engine-capability", nil,
		"capability the ENGINE the step would run on advertises; repeatable")
	flags.BoolVar(&signed, "signed", false, "the plugin carries a verified signature")
	flags.StringVar(&upstream, "upstream", "", "registry or source the artifact came from")
	flags.StringVar(&secretName, "secret-name", "",
		"the step secret being decided, read as input.secret_name; empty decides the step itself")
	flags.BoolVar(&expectAllow, "expect-allow", true, "fail unless the decision is this")
	return cmd
}

func (o *options) emitDecision(d policyDecision) error {
	if o.output == outputJSON {
		raw, err := json.Marshal(d)
		if err != nil {
			return fmt.Errorf("render json: %w", err)
		}
		_, err = fmt.Fprintf(o.env.Stdout, "%s\n", raw)
		return err
	}
	_, err := fmt.Fprintf(o.env.Stdout, "%s rule=%s %s\n", verdict(d.Allow), d.Rule, d.Reason)
	return err
}

func verdict(allow bool) string {
	if allow {
		return "allow"
	}
	return "deny"
}

// capabilities parses the short names a policy author writes — PRIVILEGED, not
// CAPABILITY_PRIVILEGED, matching what a rule sees.
func capabilities(names []string) ([]dholev1.Capability, error) {
	out := make([]dholev1.Capability, 0, len(names))
	for _, name := range names {
		full := "CAPABILITY_" + strings.ToUpper(strings.TrimPrefix(strings.ToUpper(name), "CAPABILITY_"))
		value, ok := dholev1.Capability_value[full]
		if !ok {
			return nil, fmt.Errorf("unknown capability %q", name)
		}
		out = append(out, dholev1.Capability(value))
	}
	return out, nil
}

func effectClass(name string) (dholev1.EffectClass, error) {
	full := "EFFECT_CLASS_" + strings.ToUpper(strings.TrimPrefix(strings.ToUpper(name), "EFFECT_CLASS_"))
	value, ok := dholev1.EffectClass_value[full]
	if !ok {
		return 0, fmt.Errorf("unknown effect class %q", name)
	}
	return dholev1.EffectClass(value), nil
}
