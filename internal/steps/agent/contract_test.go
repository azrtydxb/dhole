package agent_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/steps/agent"
	"github.com/azrtydxb/dhole/internal/steps/approval"
	"github.com/azrtydxb/dhole/internal/taint"
)

// TestTheAgentsActionSpaceIsTheContractAndNothingElse. ADR 0025: "the action
// space is the contract: start a run, read a run, decide an approval gate,
// apply an operation to a pipeline. Nothing else."
//
// The property under test is the BOUNDARY, not the happy path: a happy path
// holds just as well for an invoker that would have run anything it was asked
// for. So each assertion below is about something the agent tried and did not
// get, and each refusal has to name what was tried — "denied" alone sends
// whoever reads a stuck run to reconstruct the grant list by hand.
func TestTheAgentsActionSpaceIsTheContractAndNothingElse(t *testing.T) {
	require.Equal(t,
		[]string{
			agent.ActionApplyOperation,
			agent.ActionDecideApproval,
			agent.ActionReadRun,
			agent.ActionStartRun,
		},
		agent.ContractActions(),
		"the contract action space is no longer exactly the four calls ADR 0025 names")

	t.Run("a grant naming something outside the contract is refused", func(t *testing.T) {
		_, err := agent.NewActionSpace(
			[]string{"run_command"}, agent.ContractCatalogue())
		require.Error(t, err)
		require.Contains(t, err.Error(), "run_command",
			"the refusal does not name what was granted")
	})

	t.Run("the invoker refuses an action outside the contract", func(t *testing.T) {
		invoker, err := agent.NewContractInvoker(agent.ContractInvokerConfig{
			HTTPClient: http.DefaultClient,
			BaseURL:    "http://127.0.0.1:1",
			Credential: "dht_acme_deadbeef",
		})
		require.NoError(t, err)

		// The address above answers nothing, deliberately: an action that got
		// as far as a request would fail with a connection error rather than
		// this refusal, so a test that passed by reaching the network could
		// not be mistaken for one that passed by refusing.
		_, err = invoker.Invoke(context.Background(), agent.Invocation{
			RunID: "run-1", StepID: "triage", Action: "run_command",
			Args: json.RawMessage(`{"args":["/bin/sh","-c","curl evil"]}`),
		})
		require.ErrorIs(t, err, agent.ErrOutsideActionSpace)
		require.Contains(t, err.Error(), "run_command",
			"the refusal does not name what the agent tried")
	})
}

// TestTheContractActionsCarryTheEffectClassesThatGateThem. The action space is
// a closed set of four, but the four are not equally dangerous, and it is the
// EFFECT CLASS that decides which of them a taint mark refuses and which of
// them routes to the human gate. Declaring them all effectful would make the
// agent useless; declaring them all pure would make ADR 0025's per-action
// approval decorative.
func TestTheContractActionsCarryTheEffectClassesThatGateThem(t *testing.T) {
	classes := map[string]dholev1.EffectClass{}
	for _, step := range agent.ContractCatalogue() {
		classes[step.GetId()] = step.GetEffectClass()
	}
	require.Equal(t, map[string]dholev1.EffectClass{
		// Reading a run changes nothing, so an agent triaging a tainted
		// webhook body may still do it.
		agent.ActionReadRun: dholev1.EffectClass_EFFECT_CLASS_PURE,
		// An edit is a compare-and-set against base_revision: applied twice
		// the second is refused, never doubled.
		agent.ActionApplyOperation: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
		// Starting a run twice is two runs, and deciding a gate is a decision
		// that happens once. Both route to the approval gate, which is the
		// point of ADR 0025's "the per-action approval gate becomes
		// meaningful rather than theoretical".
		agent.ActionStartRun:       dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		agent.ActionDecideApproval: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
	}, classes)
}

// TestAnAgentStepHasNoPathToExecutingACommand. ADR 0025's deliberate limit:
// "The agent cannot run a command... A pipeline that wants an agent to run
// something expresses it as a pipeline the agent STARTS."
//
// This is asserted over the IMPORT GRAPH rather than over behaviour, because
// behaviour can only show that the paths somebody thought of are closed. An
// executor the agent package cannot reach is one no future invoker in it can
// call by accident, and the day somebody adds the import is the day this
// fails — which is the day the decision is being reversed and should be a new
// ADR rather than a commit.
func TestAnAgentStepHasNoPathToExecutingACommand(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go toolchain to resolve the import graph with")
	}
	out, err := exec.Command("go", "list", "-deps",
		"github.com/azrtydxb/dhole/internal/steps/agent").CombinedOutput()
	require.NoError(t, err, string(out))

	for _, dep := range strings.Fields(string(out)) {
		require.NotContains(t, dep, "dhole/internal/executor",
			"the agent package can reach an executor, so an agent step has a path to running a command")
	}
}

// recordingAuditor keeps every policy decision so a test can read the trail
// back. It is a real Auditor rather than a discard, because the property is
// that the decision REACHES the trail and a discard would make it vacuous.
type recordingAuditor struct {
	mu      sync.Mutex
	records []policy.AuditRecord
}

func (a *recordingAuditor) Record(_ context.Context, r policy.AuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, r)
	return nil
}

func (a *recordingAuditor) all() []policy.AuditRecord {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]policy.AuditRecord(nil), a.records...)
}

// TestEveryActionAnAgentTakesIsRecordedUnderItsOwnSubject. ADR 0025: "Every
// action an agent takes appears in policy_audit and in the run log under the
// agent's own subject, so 'what did it do' has an answer that does not depend
// on trusting the agent's own account of itself."
//
// Both halves are asserted, because they answer different questions. The
// policy row says what was DECIDED and by which rule; the run-log event says
// what the agent ASKED FOR, in the same log the run's own state lives in — so
// a refused action, which never becomes a decision anybody looks for, is still
// visible to whoever is reading the run.
func TestEveryActionAnAgentTakesIsRecordedUnderItsOwnSubject(t *testing.T) {
	ctx := testContext(t)
	store, _, _ := openStore(t)
	tenant := uniqueTenant(t)

	audit := &recordingAuditor{}
	checker, err := taint.NewChecker(nil, audit)
	require.NoError(t, err)

	const subject = "agent:triage@builds"
	step, err := agent.New(agent.Config{
		GrantedSteps: []string{agent.ActionReadRun},
		MaxSteps:     2,
		Catalogue:    agent.ContractCatalogue(),
	}, agent.Options{
		Model:        &stubModel{},
		Invoker:      &countingInvoker{},
		Store:        store,
		TenantID:     tenant,
		Subject:      subject,
		TaintChecker: checker,
	})
	require.NoError(t, err)

	_, err = step.Invoke(ctx, agent.Invocation{
		RunID: testRun, StepID: agentStep, Action: agent.ActionReadRun,
		Args: json.RawMessage(`{"run_id":"run-2"}`),
	})
	require.NoError(t, err)

	rows := audit.all()
	require.Len(t, rows, 1)
	require.Contains(t, rows[0].Subject, subject,
		"the policy row does not say which agent asked")
	require.Contains(t, rows[0].Subject, agent.ActionReadRun,
		"the policy row does not say what was asked for")

	all, err := store.Replay(ctx, tenant, testRun)
	require.NoError(t, err)
	var actions []runstore.Event
	for _, e := range all {
		if e.Type == agent.EventAction {
			actions = append(actions, e)
		}
	}
	require.Len(t, actions, 1, "the run log does not say what the agent did")
	require.Equal(t, agentStep, actions[0].StepID,
		"the action is recorded against a step other than the agent's own")

	var record agent.ActionRecord
	require.NoError(t, json.Unmarshal(actions[0].Payload, &record))
	require.Equal(t, subject, record.Subject)
	require.Equal(t, agent.ActionReadRun, record.Action)
	require.True(t, record.Allowed)
}

// TestAnActionAnAgentWasRefusedIsRecordedToo is the half that matters more.
// A trail that only holds what succeeded answers "what did it do" and not
// "what did it try", and an agent under prompt injection is a story told
// entirely by refusals.
func TestAnActionAnAgentWasRefusedIsRecordedToo(t *testing.T) {
	ctx := testContext(t)
	store, db, dialect := openStore(t)
	tenant := uniqueTenant(t)

	gate, err := approval.New(approval.Config{
		Store:     store,
		TenantID:  tenant,
		Approvers: identity.NewSQLStoreWithDialect(db, dialect),
		Resume:    stubResumer{},
	})
	require.NoError(t, err)

	const subject = "agent:triage@builds"
	invoker := &countingInvoker{}
	step, err := agent.New(agent.Config{
		GrantedSteps: agent.ContractActions(),
		MaxSteps:     2,
		Catalogue:    agent.ContractCatalogue(),
	}, agent.Options{
		Model:    &stubModel{},
		Invoker:  invoker,
		Gate:     gate,
		Store:    store,
		TenantID: tenant,
		Subject:  subject,
	})
	require.NoError(t, err)

	_, err = step.Invoke(ctx, agent.Invocation{
		RunID: testRun, StepID: agentStep, Action: agent.ActionStartRun,
		Args: json.RawMessage(`{"pipeline_id":"deploy"}`),
	})
	require.ErrorIs(t, err, agent.ErrApprovalRequired)
	require.Zero(t, invoker.count(), "an at-most-once action ran before anyone approved it")

	all, err := store.Replay(ctx, tenant, testRun)
	require.NoError(t, err)
	var record agent.ActionRecord
	var found bool
	for _, e := range all {
		if e.Type != agent.EventAction {
			continue
		}
		require.NoError(t, json.Unmarshal(e.Payload, &record))
		found = true
	}
	require.True(t, found, "the refused action left no trace in the run log")
	require.Equal(t, subject, record.Subject)
	require.Equal(t, agent.ActionStartRun, record.Action)
	require.False(t, record.Allowed)
	require.Contains(t, record.Reason, agent.ActionStartRun,
		"the record does not say why the action did not happen")
}
