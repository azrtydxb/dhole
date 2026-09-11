package agent_test

// Resuming a parked agent, unit by unit. The end-to-end proof that a plane
// does this is in internal/server; what is here is the four properties the
// resume has to have and the one it must not.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/steps/agent"
	"github.com/azrtydxb/dhole/internal/steps/approval"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

const approver = "release-boss"

// TestAResumedAgentNeverTakesAnActionTheParkedLoopAlreadyTook is the
// replay-safety property, and it is the one that costs real money to get
// wrong: an agent acts through Dhole's public API (ADR 0025), so a replayed
// call is a second run started or a second operation applied.
//
// The mechanism is that the model is never re-prompted. The parked
// conversation comes back verbatim, with every earlier call already answered
// in it, and the SDK runs only the one unanswered batch. Nothing here decides
// what to skip, because nothing is skipped — it was never asked for again.
func TestAResumedAgentNeverTakesAnActionTheParkedLoopAlreadyTook(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(t, config(format, deploy), nil)
	h.model.script(
		toolCall("c1", format, `{"what":"the log"}`),
		toolCall("c2", deploy, `{"env":"prod"}`),
	)

	parked, err := h.step.Run(ctx, testRun, agentStep, "tidy the log then ship it")
	require.NoError(t, err)
	require.NotNil(t, parked.Parked)
	require.Equal(t, []string{format}, h.invoker.actions(),
		"the pure call happened and the gated one did not")
	before := h.model.callCount()

	out, err := h.step.Resume(ctx, testRun, agentStep, *parked.Parked,
		agent.Decision{Approver: approver, Approved: true})
	require.NoError(t, err)
	require.Nil(t, out.Parked)

	require.Equal(t, []string{format, deploy}, h.invoker.actions(),
		"the resumed loop took an action the parked loop had already taken")
	require.Equal(t, before+1, h.model.callCount(),
		"the model was asked to decide again what it had already decided")

	// The log says on whose authority the at-most-once action happened.
	var allowed agent.ActionRecord
	for _, e := range h.eventsOfType(ctx, t, agent.EventAction) {
		var record agent.ActionRecord
		require.NoError(t, json.Unmarshal(e.Payload, &record))
		if record.Action == deploy && record.Allowed {
			allowed = record
		}
	}
	require.Equal(t, approver, allowed.Approver,
		"the log says an agent deployed and not who let it")
}

// TestAResumedAgentGetsOnlyWhatIsLeftOfItsStepCeiling. MaxSteps is the agent's
// half of ADR 0015's bound. A resume that handed back a fresh ceiling would
// make "ask for something gated" the way around it: park, get approved, start
// again with a full budget, repeat.
func TestAResumedAgentGetsOnlyWhatIsLeftOfItsStepCeiling(t *testing.T) {
	ctx := testContext(t)

	t.Run("a loop that parked on its last step has nothing left", func(t *testing.T) {
		cfg := config(format, deploy)
		cfg.MaxSteps = 2
		h := newHarnessWith(ctx, t, openStore, cfg, nil)
		h.model.script(
			toolCall("c1", format, `{}`),
			toolCall("c2", deploy, `{}`),
		)

		parked, err := h.step.Run(ctx, testRun, agentStep, "tidy then ship")
		require.NoError(t, err)
		require.Equal(t, 2, parked.Parked.StepsUsed,
			"the park record did not carry what the loop had already spent")

		before := h.invoker.count()
		_, err = h.step.Resume(ctx, testRun, agentStep, *parked.Parked,
			agent.Decision{Approver: approver, Approved: true})
		require.ErrorIs(t, err, agent.ErrCeilingSpent)
		require.Equal(t, before, h.invoker.count(),
			"an approval bought the agent a ceiling it had already spent")
	})

	t.Run("a loop with room left keeps only that room", func(t *testing.T) {
		cfg := config(format, deploy)
		cfg.MaxSteps = 4
		h := newHarnessWith(ctx, t, openStore, cfg, nil)
		h.model.script(toolCall("c1", deploy, `{}`))

		parked, err := h.step.Run(ctx, testRun, agentStep, "ship it")
		require.NoError(t, err)
		require.Equal(t, 1, parked.Parked.StepsUsed)

		// After the approved batch the model calls a tool forever. It gets
		// the three steps it had left and not the four it started with.
		h.model.always(toolCall("c", format, `{}`))
		before := h.model.callCount()
		_, err = h.step.Resume(ctx, testRun, agentStep, *parked.Parked,
			agent.Decision{Approver: approver, Approved: true})
		require.NoError(t, err)
		require.Equal(t, 3, h.model.callCount()-before,
			"the resumed loop was given a fresh ceiling instead of what was left of the old one")
	})
}

// TestADeniedGateEndsTheAgentStepRatherThanBeingHandedBackToTheModel.
//
// Both halves are deliberate. A refusal returned to the model as a tool
// result leaves an agent that has just been told "no" running, free to reach
// the same effect another way, and the person who refused has no say in what
// happens next. And a gate is keyed on (run, step) — there is exactly one per
// agent step per run — so there is no second gate to route whatever it tried
// next to.
func TestADeniedGateEndsTheAgentStepRatherThanBeingHandedBackToTheModel(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(t, config(deploy), nil)
	h.model.script(toolCall("c1", deploy, `{"env":"prod"}`))

	parked, err := h.step.Run(ctx, testRun, agentStep, "ship it")
	require.NoError(t, err)
	before := h.model.callCount()

	_, err = h.step.Resume(ctx, testRun, agentStep, *parked.Parked,
		agent.Decision{Approver: approver, Approved: false})
	require.ErrorIs(t, err, agent.ErrDenied)
	require.Contains(t, err.Error(), approver, "the refusal does not say who refused")
	require.Zero(t, h.invoker.count(), "a denied action happened anyway")
	require.Equal(t, before, h.model.callCount(),
		"the model was given another turn after a person refused it")
}

// TestAResumeWithNoApproverIsRefused: the resume is the one path that lets an
// at-most-once action through, and it does so on the strength of a person's
// name. An empty one is an approval by nobody.
func TestAResumeWithNoApproverIsRefused(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(t, config(deploy), nil)
	h.model.script(toolCall("c1", deploy, `{}`))

	parked, err := h.step.Run(ctx, testRun, agentStep, "ship it")
	require.NoError(t, err)

	_, err = h.step.Resume(ctx, testRun, agentStep, *parked.Parked,
		agent.Decision{Approved: true})
	require.ErrorIs(t, err, agent.ErrDenied)
	require.Zero(t, h.invoker.count())
}

// TestTheGateReleasesOnlyTheCallsItWasAskedAbout. The window a decision opens
// closes at the first model call of the resumed segment, and within that batch
// it covers only the calls the person was shown. A model that asks for the
// same action again after the batch is a new question and gets a new gate —
// which, there being only one gate per step per run, is refused by name rather
// than granted silently.
func TestTheGateReleasesOnlyTheCallsItWasAskedAbout(t *testing.T) {
	ctx := testContext(t)
	cfg := config(deploy)
	cfg.MaxSteps = 6
	h := newHarnessWith(ctx, t, openStore, cfg, nil)
	h.model.script(toolCall("c1", deploy, `{"env":"prod"}`))

	parked, err := h.step.Run(ctx, testRun, agentStep, "ship it")
	require.NoError(t, err)

	// A person decides, exactly as the plane does before it resumes the step.
	h.decide(ctx, t, approver, true)

	// After the approved call, it asks for the very same action again.
	h.model.script(toolCall("c2", deploy, `{"env":"prod-again"}`))
	_, err = h.step.Resume(ctx, testRun, agentStep, *parked.Parked,
		agent.Decision{Approver: approver, Approved: true})
	require.Error(t, err, "one approval let a second at-most-once action through")
	require.ErrorIs(t, err, approval.ErrAlreadyDecided,
		"the second ask was not refused by the standing decision")
	require.Equal(t, []string{deploy}, h.invoker.actions(),
		"the approved action ran once and the unapproved one did not run at all")
}

// TestAParkedConversationIsReplayedFromItsOwnPayloadAndNotFromTheLog is the
// determinism rule. The transcript is stored verbatim in the park event, so an
// event appended afterwards — another agent's action, another step's verdict,
// anything at all — cannot change how that transcript replays. A transcript
// folded back out of the run's events could not promise that.
func TestAParkedConversationIsReplayedFromItsOwnPayloadAndNotFromTheLog(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(t, config(format, deploy), nil)
	h.model.script(
		toolCall("c1", format, `{"what":"the log"}`),
		toolCall("c2", deploy, `{"env":"prod"}`),
	)

	parked, err := h.step.Run(ctx, testRun, agentStep, "tidy then ship")
	require.NoError(t, err)
	before, err := parked.Parked.Messages()
	require.NoError(t, err)

	// The run carries on being a run: more events land after the park.
	require.NoError(t, h.store.Append(ctx, h.tenant, runstore.Event{
		RunID: testRun, StepID: "somebody-else", Type: runstore.StepSucceeded,
		At: time.Now().UTC(),
	}))
	require.NoError(t, h.store.Append(ctx, h.tenant, runstore.Event{
		RunID: testRun, StepID: agentStep, Type: agent.EventAction,
		Payload: []byte(`{"subject":"someone","action":"format","allowed":true}`),
		At:      time.Now().UTC(),
	}))

	// Read back the way the plane reads it: off the park event in the log.
	events := h.eventsOfType(ctx, t, agent.EventParked)
	require.Len(t, events, 1)
	record, err := agent.UnmarshalPark(events[0].Payload)
	require.NoError(t, err)
	after, err := record.Messages()
	require.NoError(t, err)

	require.Equal(t, before, after,
		"an event appended later changed how an older transcript replays")
	require.Equal(t, []string{deploy}, record.Actions)
	require.Len(t, record.ToolCallIDs, 1)
	require.Equal(t, "c2", record.ToolCallIDs[0])
}

// TestAConversationSurvivesTheRoundTripThroughTheLog. provider.ContentPart is
// an interface, so a transcript needs a tagged encoding to survive being
// written down. A part that came back as something else — or did not come back
// at all — would resume a model into a conversation it never had.
func TestAConversationSurvivesTheRoundTripThroughTheLog(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(t, config(format, deploy), nil)
	h.model.script(
		&provider.Response{
			Content: []provider.ContentPart{
				provider.ReasoningPart{Text: "the log is noisy", Signature: "sig"},
				provider.TextPart{Text: "tidying first"},
				provider.ToolCallPart{
					ID: "c1", Name: format, Args: json.RawMessage(`{"what":"the log"}`),
				},
			},
			FinishReason: provider.FinishToolCalls,
		},
		toolCall("c2", deploy, `{"env":"prod"}`),
	)

	parked, err := h.step.Run(ctx, testRun, agentStep, "tidy then ship")
	require.NoError(t, err)
	messages, err := parked.Parked.Messages()
	require.NoError(t, err)

	var sawReasoning, sawResult bool
	for _, m := range messages {
		for _, part := range m.Content {
			switch p := part.(type) {
			case provider.ReasoningPart:
				sawReasoning = true
				require.Equal(t, "sig", p.Signature,
					"a signature Anthropic requires for a round trip was dropped")
			case provider.ToolResultPart:
				sawResult = true
				require.Equal(t, "c1", p.ToolCallID)
			}
		}
	}
	require.True(t, sawReasoning, "the model's reasoning did not survive being written down")
	require.True(t, sawResult, "the answer to the call the loop already made was lost")
}

// TestAPartOfAnUnknownKindIsRefusedRatherThanDropped. A transcript is a tagged
// encoding because provider.ContentPart is an interface, and the tempting
// failure is to skip what a build does not recognise — a newer plane's
// reasoning block, say, read back by an older one. A dropped part resumes the
// model into a conversation it never had, so the resume refuses by name and
// the step fails readably instead.
func TestAPartOfAnUnknownKindIsRefusedRatherThanDropped(t *testing.T) {
	record, err := agent.UnmarshalPark([]byte(
		`{"transcript":[{"role":"assistant","parts":[{"kind":"telepathy","text":"hi"}]}]}`))
	require.NoError(t, err)

	_, err = record.Messages()
	require.ErrorIs(t, err, agent.ErrTranscript)
	require.Contains(t, err.Error(), "telepathy", "the refusal does not name what it could not read")
}
