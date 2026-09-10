package server

// `builtin:agent`: a model given a bounded loop over Dhole's own contract.
//
// ADR 0025 decides what an agent may do, and this file is the whole of the
// answer: "An agent step acts only through Dhole's own public API, as a
// principal of its tenant. The action space is the contract: start a run, read
// a run, decide an approval gate, apply an operation to a pipeline. Nothing
// else."
//
// internal/steps/agent had the action space, the taint check and the
// per-action approval gate and no way to take an action, because an agent step
// needs an INVOKER and nothing supplied one. What this file supplies is a
// Connect client against the plane's own API listener carrying a credential
// minted for the agent's own subject — see agent.ContractInvoker for why it
// goes over the loopback socket rather than reaching into api.Server in the
// same process.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	nethttp "net/http"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/steps/agent"
)

// agentTokenTTL is how long an agent's credential lives.
//
// It is minutes rather than hours because it is minted per ATTEMPT and needs
// to outlive one bounded model loop and nothing else. A token that outlived
// the step would be a credential lying around with an agent's name on it and
// nobody watching the run it belonged to.
const agentTokenTTL = 15 * time.Minute

// agentHTTPTimeout bounds one contract call. It is generous: the call it
// bounds is a loopback request to this same process.
const agentHTTPTimeout = 60 * time.Second

// runAgent is `builtin:agent`. It runs the model's bounded loop and writes the
// final answer to the step's first output port.
func (b *builtins) runAgent(
	ctx context.Context, job builtinJob, _ uint32,
) ([]*dholev1.OutputRef, error) {
	cfg := job.step.GetConfig()
	if b.models == nil {
		return nil, errors.New(
			"this control plane was given no model factory, so it can run no agent step; " +
				"see server.Config.Models")
	}
	grants, err := agentGrants(cfg["grants"])
	if err != nil {
		return nil, err
	}
	ceiling, err := strconv.Atoi(strings.TrimSpace(cfg["max_steps"]))
	if err != nil {
		return nil, fmt.Errorf("an agent step's max_steps %q: %w", cfg["max_steps"], err)
	}
	prompt := strings.TrimSpace(cfg["prompt"])
	if prompt == "" {
		return nil, errors.New("an agent step needs a `prompt` in its config")
	}
	model, err := b.models(ctx, cfg["provider"], cfg["model"])
	if err != nil {
		return nil, fmt.Errorf("resolving model %q of provider %q: %w",
			cfg["model"], cfg["provider"], err)
	}

	invoker, subject, err := b.agentInvoker(ctx, job)
	if err != nil {
		return nil, err
	}
	gate, err := b.gate(job.tenantID)
	if err != nil {
		return nil, err
	}

	step, err := agent.New(agent.Config{
		GrantedSteps: grants,
		MaxSteps:     ceiling,
		// The catalogue is the CONTRACT and not the tenant's pipelines: a
		// grant naming anything else is refused right here, by name, which is
		// the boundary ADR 0025 draws.
		Catalogue:    agent.ContractCatalogue(),
		Instructions: cfg["instructions"],
	}, agent.Options{
		Model:    model,
		Invoker:  invoker,
		Gate:     gate,
		Store:    b.store,
		TenantID: job.tenantID,
		Subject:  subject,
	})
	if err != nil {
		return nil, err
	}

	result, err := step.Run(ctx, job.runID, job.step.GetId(), prompt)
	if err != nil {
		return nil, err
	}
	return b.emit(ctx, job, []byte(result.Text))
}

// agentGrants reads the closed set of contract actions this step was given.
//
// An empty grant list is refused rather than defaulting to all four. The
// defaulting version is the one that goes wrong: a pipeline author who wrote
// `builtin:agent` and nothing else would have handed a model the power to
// start runs and decide approval gates without ever having typed either word.
func agentGrants(raw string) ([]string, error) {
	var granted []string
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			granted = append(granted, name)
		}
	}
	if len(granted) == 0 {
		return nil, fmt.Errorf(
			"an agent step needs a `grants` in its config naming what it may do; "+
				"the contract's action space is [%s]",
			strings.Join(agent.ContractActions(), ", "))
	}
	return granted, nil
}

// agentInvoker mints this agent's credential and points a contract client at
// this plane's own listener. It returns the client and the subject the
// credential was minted for, which is what every action is audited under.
func (b *builtins) agentInvoker(
	ctx context.Context, job builtinJob,
) (agent.Invoker, string, error) {
	if b.tokens == nil {
		return nil, "", errors.New(
			"this control plane has no principal store, so there is nobody an agent could act as")
	}
	if b.apiBase == nil {
		return nil, "", errors.New(
			"this control plane serves no API, so an agent step has no contract to act through")
	}
	addr := b.apiBase()
	if addr == "" {
		return nil, "", errors.New(
			"this control plane's API is not listening, so an agent step has nothing to call")
	}

	subject, err := b.agentSubject(ctx, job)
	if err != nil {
		return nil, "", err
	}
	// The agent becomes a principal of its tenant at the mint (migration
	// 0022), so the subject an approval or an audit row names is a subject the
	// rest of this system will answer for. Kind AGENT, not SERVICE: a policy
	// rule has to be able to refuse an agent what it allows the deploy robot.
	credential, err := b.tokens.IssueToken(ctx, identity.Principal{
		TenantID: job.tenantID,
		Subject:  subject,
		Kind:     identity.PrincipalAgent,
	}, agentTokenTTL)
	if err != nil {
		return nil, "", fmt.Errorf("minting a credential for agent %q: %w", subject, err)
	}

	invoker, err := agent.NewContractInvoker(agent.ContractInvokerConfig{
		HTTPClient: &nethttp.Client{Timeout: agentHTTPTimeout},
		BaseURL:    "http://" + addr,
		Credential: credential,
	})
	if err != nil {
		return nil, "", err
	}
	return invoker, subject, nil
}

// agentSubject is the principal one agent step acts as.
//
// It is per (pipeline, step) rather than per run, because a subject is
// something an operator grants, revokes and reads an audit trail for, and a
// subject that changed every run would be an audit trail with one row in each
// of a thousand identities. Two agent steps of the same pipeline are two
// principals, because they are two different things a person would want to
// revoke separately.
func (b *builtins) agentSubject(ctx context.Context, job builtinJob) (string, error) {
	events, err := b.store.Replay(ctx, job.tenantID, job.runID)
	if err != nil {
		return "", err
	}
	for _, e := range events {
		if e.Type != runstore.RunCreated {
			continue
		}
		created, err := scheduler.UnmarshalRunCreated(e.Payload)
		if err != nil {
			return "", err
		}
		return "agent:" + created.PipelineID + ":" + job.step.GetId(), nil
	}
	return "", fmt.Errorf("run %q has no %s event, so its agent has no identity",
		job.runID, runstore.RunCreated)
}
