package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
)

// The action space, and it is the CONTRACT (ADR 0025): start a run, read a
// run, decide an approval gate, apply an operation to a pipeline. Nothing
// else.
//
// They are the ids of the pseudo-steps ContractCatalogue defines, which makes
// them the names the model sees as tools and the names a refusal quotes. They
// are lower-case and underscored rather than the RPC's own CamelCase because
// a tool name is what a model types, and a name it has to transcribe exactly
// is a name it gets wrong.
const (
	// ActionStartRun starts a run of a pipeline's active revision. It is how
	// an agent runs ANYTHING: the agent decides what to run, and does not
	// become the thing that runs it.
	ActionStartRun = "start_run"
	// ActionReadRun reads a run's event log — the only place a run's state
	// lives (ADR 0003).
	ActionReadRun = "read_run"
	// ActionDecideApproval answers an approval gate a run is waiting at.
	ActionDecideApproval = "decide_approval"
	// ActionApplyOperation applies one editing operation to a pipeline.
	ActionApplyOperation = "apply_operation"
)

// defaultReadTimeout bounds ActionReadRun.
//
// WatchRun is a server stream that FOLLOWS a run: it ends when the run reaches
// a terminal event and not before, so a read of a run still in flight has no
// natural end. Without a bound, an agent that read a three-day wait would hold
// its step open for three days and spend its whole ceiling on one call. The
// read returns what the log holds when the bound expires, which is what "read
// a run" means to a caller.
const defaultReadTimeout = 10 * time.Second

// ContractCatalogue is the step catalogue an agent's action space is built
// from: one pseudo-step per contract action.
//
// They are pseudo-steps because nothing dispatches them — the invoker turns
// each into an RPC — but they are real dholev1.Steps because the effect class
// is what the taint check and the approval rule both read, and an action whose
// class is unknown is refused by both. The classes are the point of this
// function: `read_run` is PURE, so an agent triaging a tainted webhook body
// may still read a run, and `start_run` is AT_MOST_ONCE, so the same agent
// asking to start one is routed to a person.
func ContractCatalogue() []*dholev1.Step {
	return []*dholev1.Step{
		{
			Id:          ActionReadRun,
			Name:        "read a run",
			PluginRef:   PluginRef,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		},
		{
			Id:   ActionApplyOperation,
			Name: "apply an operation to a pipeline",
			// IDEMPOTENT: an edit is a compare-and-set against
			// base_revision, so applying the same operation twice is refused
			// by the head check rather than applied twice.
			PluginRef:   PluginRef,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
		},
		{
			Id:   ActionStartRun,
			Name: "start a run",
			// AT_MOST_ONCE: starting a run twice is two runs, and a run is
			// the only way an agent reaches anything effectful at all.
			PluginRef:   PluginRef,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		},
		{
			Id:   ActionDecideApproval,
			Name: "decide an approval gate",
			// AT_MOST_ONCE, and this one is the sharpest: an agent deciding a
			// gate unasked is an agent approving itself.
			PluginRef:   PluginRef,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		},
	}
}

// ContractActions names the four, sorted, so a refusal and a grant list read
// the same way twice.
func ContractActions() []string {
	names := make([]string, 0, 4)
	for _, step := range ContractCatalogue() {
		names = append(names, step.GetId())
	}
	sort.Strings(names)
	return names
}

// ContractInvokerConfig is what an agent needs in order to act.
type ContractInvokerConfig struct {
	// HTTPClient reaches the plane's own API listener. It is an ordinary HTTP
	// client on an ordinary socket, which is the whole point: see
	// NewContractInvoker.
	HTTPClient connect.HTTPClient
	// BaseURL is the plane's API, "http://127.0.0.1:7777".
	BaseURL string
	// Credential is the bearer token minted for THIS agent's subject. It is
	// short-lived and it is the agent's own: every call it makes is audited
	// and attributed to it, and revoking it revokes the agent.
	Credential string
	// ReadTimeout bounds ActionReadRun. Zero means defaultReadTimeout.
	ReadTimeout time.Duration
}

// ContractInvoker is ADR 0025's invoker: a Connect client against the plane's
// own public API, carrying a credential minted for the agent's subject.
//
// It is a REAL HTTP CLIENT ON THE PLANE'S OWN LISTENER, not a direct call into
// the api.Server in the same process, and that is the decision this type
// exists to record. ADR 0013's whole point is that there is no privileged
// path: the React canvas, the CLI and an agent are one client population of
// one contract. A direct in-process call would be a second way in, and the
// first time somebody added an interceptor — a rate limit, an audit hook, a
// quota — the agent would be the one caller that did not get it, silently.
// Going out over the loopback socket means an agent's call is byte-identical
// to a call from another machine: the same handler, the same
// Server.principal, the same policy, and the same coverage from
// TestEveryRPCRejectsACallWithoutAuthorization. The cost is a loopback round
// trip per action, which is nothing beside a model call.
type ContractInvoker struct {
	client dholev1connect.PipelineServiceClient
	// credential is held rather than baked into an interceptor so that the
	// refusal for a missing one is this package's, with a reason, rather than
	// an Unauthenticated from the far end that reads like an outage.
	credential string
	read       time.Duration
}

// Compile-time proof the contract client is the Invoker an agent step takes.
var _ Invoker = (*ContractInvoker)(nil)

// NewContractInvoker builds the client. It refuses a missing base URL or
// credential: an agent that acted unauthenticated would be refused by every
// RPC anyway, and an agent whose calls were somehow NOT refused would be the
// privileged path ADR 0013 forbids.
func NewContractInvoker(cfg ContractInvokerConfig) (*ContractInvoker, error) {
	if cfg.HTTPClient == nil {
		return nil, errors.New("agent: an HTTP client is required to reach the contract")
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, errors.New("agent: the plane's API address is required")
	}
	if strings.TrimSpace(cfg.Credential) == "" {
		return nil, errors.New(
			"agent: a credential minted for the agent's subject is required; " +
				"an agent holds no capability a person with the same token would not have")
	}
	read := cfg.ReadTimeout
	if read <= 0 {
		read = defaultReadTimeout
	}
	return &ContractInvoker{
		client:     dholev1connect.NewPipelineServiceClient(cfg.HTTPClient, cfg.BaseURL),
		credential: cfg.Credential,
		read:       read,
	}, nil
}

// Invoke performs one contract action.
//
// The switch is CLOSED and its default refuses. That refusal is the last of
// three — Step.Invoke asks the action space, the tool list is built from it,
// and this asks again — and it is the one that would catch an invoker wired
// to an action space somebody widened without widening this. There is no
// branch here that runs a command, and there is no package this one imports
// that could: see TestAnAgentStepHasNoPathToExecutingACommand.
func (i *ContractInvoker) Invoke(ctx context.Context, inv Invocation) (json.RawMessage, error) {
	switch inv.Action {
	case ActionStartRun:
		return i.startRun(ctx, inv.Args)
	case ActionReadRun:
		return i.readRun(ctx, inv.Args)
	case ActionDecideApproval:
		return i.decideApproval(ctx, inv.Args)
	case ActionApplyOperation:
		return i.applyOperation(ctx, inv.Args)
	default:
		return nil, fmt.Errorf("%w: this agent asked to invoke %q through the contract; "+
			"the contract's action space is [%s]",
			ErrOutsideActionSpace, inv.Action, strings.Join(ContractActions(), ", "))
	}
}

// authorize puts the agent's own credential on a request. Every call goes
// through it, and there is no path in this type that builds a request without
// it.
func (i *ContractInvoker) authorize(header interface{ Set(string, string) }) {
	header.Set("Authorization", "Bearer "+i.credential)
}

// startRunArgs is what a model supplies to start a run. There is no tenant
// field, here or anywhere: the tenant comes from the credential, and a tenant
// a caller can type is a tenant a caller can change.
type startRunArgs struct {
	PipelineID string `json:"pipeline_id"`
	RevisionID string `json:"revision_id,omitempty"`
}

func (i *ContractInvoker) startRun(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	var a startRunArgs
	if err := decode(ActionStartRun, args, &a); err != nil {
		return nil, err
	}
	req := connect.NewRequest(&dholev1.StartRunRequest{
		PipelineId: a.PipelineID,
		RevisionId: a.RevisionID,
	})
	i.authorize(req.Header())
	res, err := i.client.StartRun(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("agent: %s: %w", ActionStartRun, err)
	}
	return json.Marshal(map[string]string{
		"run_id":      res.Msg.GetRunId(),
		"revision_id": res.Msg.GetRevisionId(),
	})
}

type readRunArgs struct {
	RunID string `json:"run_id"`
}

// readRun follows the run's log until it ends or the read bound expires, and
// returns what it saw. See defaultReadTimeout for why there is a bound.
func (i *ContractInvoker) readRun(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	var a readRunArgs
	if err := decode(ActionReadRun, args, &a); err != nil {
		return nil, err
	}
	req := connect.NewRequest(&dholev1.WatchRunRequest{RunId: a.RunID})
	i.authorize(req.Header())

	bounded, cancel := context.WithTimeout(ctx, i.read)
	defer cancel()

	stream, err := i.client.WatchRun(bounded, req)
	if err != nil {
		return nil, fmt.Errorf("agent: %s: %w", ActionReadRun, err)
	}
	defer func() { _ = stream.Close() }()

	// The event's PAYLOAD is deliberately not returned. It is a protobuf blob
	// whose meaning depends on the type, and putting it in front of a model
	// as base64 would spend the context window on something it cannot read.
	// A run's shape — which step reached which state — is what "read a run"
	// is for.
	type event struct {
		StepID   string `json:"step_id"`
		Type     string `json:"type"`
		Attempt  uint32 `json:"attempt"`
		Sequence uint64 `json:"sequence"`
	}
	seen := []event{}
	for stream.Receive() {
		msg := stream.Msg()
		seen = append(seen, event{
			StepID:   msg.GetStepId(),
			Type:     msg.GetType(),
			Attempt:  msg.GetAttempt(),
			Sequence: msg.GetSequence(),
		})
	}
	// A stream that ended because the bound expired has still read the run:
	// the events collected are the answer, and only a failure with NOTHING
	// read is a failure to read the run. An error returned alongside events
	// would make an agent retry a call that worked.
	if err := stream.Err(); err != nil && len(seen) == 0 && bounded.Err() == nil {
		return nil, fmt.Errorf("agent: %s: %w", ActionReadRun, err)
	}
	return json.Marshal(map[string]any{"run_id": a.RunID, "events": seen})
}

type decideApprovalArgs struct {
	RunID    string `json:"run_id"`
	StepID   string `json:"step_id"`
	Approved bool   `json:"approved"`
}

func (i *ContractInvoker) decideApproval(
	ctx context.Context, args json.RawMessage,
) (json.RawMessage, error) {
	var a decideApprovalArgs
	if err := decode(ActionDecideApproval, args, &a); err != nil {
		return nil, err
	}
	req := connect.NewRequest(&dholev1.DecideApprovalRequest{
		RunId: a.RunID, StepId: a.StepID, Approved: a.Approved,
	})
	i.authorize(req.Header())
	res, err := i.client.DecideApproval(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("agent: %s: %w", ActionDecideApproval, err)
	}
	// The approver in the answer is the CREDENTIAL's subject, decided by the
	// server and never by the request: it is the agent's own name, which is
	// what makes "who approved this" answerable without trusting the agent.
	return json.Marshal(map[string]any{
		"run_id":   res.Msg.GetRunId(),
		"step_id":  res.Msg.GetStepId(),
		"approver": res.Msg.GetApprover(),
		"approved": res.Msg.GetApproved(),
	})
}

type applyOperationArgs struct {
	PipelineID   string          `json:"pipeline_id"`
	BaseRevision string          `json:"base_revision"`
	Operation    json.RawMessage `json:"operation"`
}

func (i *ContractInvoker) applyOperation(
	ctx context.Context, args json.RawMessage,
) (json.RawMessage, error) {
	var a applyOperationArgs
	if err := decode(ActionApplyOperation, args, &a); err != nil {
		return nil, err
	}
	op := &dholev1.Operation{}
	// protojson, not encoding/json: an Operation is a oneof, and the JSON
	// shape a model has any chance of producing is the one the contract's own
	// schema documents.
	if err := protojson.Unmarshal(a.Operation, op); err != nil {
		return nil, fmt.Errorf("agent: %s: the operation is not a dhole.v1.Operation: %w",
			ActionApplyOperation, err)
	}
	req := connect.NewRequest(&dholev1.ApplyOperationRequest{
		PipelineId:   a.PipelineID,
		BaseRevision: a.BaseRevision,
		Operation:    op,
	})
	i.authorize(req.Header())
	res, err := i.client.ApplyOperation(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("agent: %s: %w", ActionApplyOperation, err)
	}
	return json.Marshal(map[string]any{
		"revision_id": res.Msg.GetRevision().GetId(),
		"changes":     len(res.Msg.GetDiff().GetChanges()),
	})
}

// decode reads a model's arguments, naming the action in the failure. A model
// that produced something else is told which call it got wrong, because the
// error is handed back to it as a tool result and it is the only party that
// can fix it.
func decode(action string, args json.RawMessage, into any) error {
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	if err := json.Unmarshal(args, into); err != nil {
		return fmt.Errorf("agent: %s: arguments: %w", action, err)
	}
	return nil
}
