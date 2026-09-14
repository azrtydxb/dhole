package api_test

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/secrets"
)

// The tests in this file are ADR 0028's: a step's secret bindings, its
// capabilities and its remaining scalars are each reachable by an operation
// that edits one element, and every such operation has an exact inverse.

func stepSecretOp(op *dholev1.SetStepSecret) *dholev1.Operation {
	return &dholev1.Operation{Kind: &dholev1.Operation_SetStepSecret{SetStepSecret: op}}
}

func stepCapabilityOp(op *dholev1.SetStepCapability) *dholev1.Operation {
	return &dholev1.Operation{Kind: &dholev1.Operation_SetStepCapability{SetStepCapability: op}}
}

func index(i uint32) *uint32 { return &i }

// stepOf returns the step with that id, failing the test when there is none.
func stepOf(t *testing.T, p *dholev1.Pipeline, id string) *dholev1.Step {
	t.Helper()
	for _, step := range p.GetSteps() {
		if step.GetId() == id {
			return step
		}
	}
	t.Fatalf("no step %q in the definition", id)
	return nil
}

func envs(step *dholev1.Step) []string {
	out := make([]string, 0, len(step.GetSecrets()))
	for _, s := range step.GetSecrets() {
		out = append(out, s.GetEnv()+"="+s.GetName())
	}
	return out
}

// roundTripOp applies op to a freshly seeded pipeline through the real server,
// applies the inverse it returned, and requires the revision id to be the one
// it started from. Revision identity IS the content hash, so that is exact
// equality rather than a resemblance. It returns the definition the operation
// produced, so a caller can say what it should look like.
func roundTripOp(t *testing.T, op *dholev1.Operation) (*dholev1.Pipeline, *dholev1.Change) {
	t.Helper()
	h := newRealHarness(t)
	original, base := seed(t, h, tenantA)
	ctx := context.Background()

	applied, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId: original.GetId(), BaseRevision: base.ID, Operation: op,
	}, tokenAlice))
	require.NoError(t, err)
	require.NotEqual(t, base.ID, applied.Msg.GetRevision().GetId(), "the operation changed nothing")
	require.Len(t, applied.Msg.GetDiff().GetChanges(), 1)

	undone, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
		PipelineId:   original.GetId(),
		BaseRevision: applied.Msg.GetRevision().GetId(),
		Operation:    applied.Msg.GetInverse(),
	}, tokenAlice))
	require.NoError(t, err)
	require.Equal(t, base.ID, undone.Msg.GetRevision().GetId(),
		"the inverse did not restore the definition it was taken from; inverse was %v",
		applied.Msg.GetInverse())
	return applied.Msg.GetPipeline(), applied.Msg.GetDiff().GetChanges()[0]
}

// refused requires op to be refused by the server as an invalid argument whose
// message contains want, and by Apply itself — the refusal belongs to the
// operation, not to some later check.
func refused(t *testing.T, p *dholev1.Pipeline, op *dholev1.Operation, want string) {
	t.Helper()
	_, _, _, err := api.Apply(p, op)
	require.Error(t, err)
	require.Contains(t, err.Error(), want)

	h := newRealHarness(t)
	rev, err := h.defs.Save(context.Background(), tenantA, p, "alice")
	require.NoError(t, err)
	_, err = h.client.ApplyOperation(context.Background(), authed(&dholev1.ApplyOperationRequest{
		PipelineId: p.GetId(), BaseRevision: rev.ID, Operation: op,
	}, tokenAlice))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), want)
}

// TestSetStepSecretInvertsInEveryDirection: binding, rebinding and unbinding a
// step's secret each land on the definition asked for, and each inverse lands
// back on the revision it started from — including the position of a binding
// removed from the front of the list.
func TestSetStepSecretInvertsInEveryDirection(t *testing.T) {
	t.Run("BindingANewEnvAppendsIt", func(t *testing.T) {
		got, change := roundTripOp(t, stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "NEXUS_PASSWORD", Name: "nexus-push",
		}))
		require.Equal(t, []string{
			"REGISTRY_USER=registry-robot", "REGISTRY_PASSWORD=registry-robot-password",
			"NEXUS_PASSWORD=nexus-push",
		}, envs(stepOf(t, got, "b")))
		require.Equal(t, dholev1.ChangeKind_CHANGE_KIND_ADDED, change.GetKind())
		require.Contains(t, change.GetSummary(), "NEXUS_PASSWORD")
		require.Contains(t, change.GetSummary(), "nexus-push")
	})

	t.Run("BindingANewEnvAtAnIndexInsertsItThere", func(t *testing.T) {
		got, _ := roundTripOp(t, stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "NEXUS_PASSWORD", Name: "nexus-push", Index: index(1),
		}))
		require.Equal(t, []string{
			"REGISTRY_USER=registry-robot", "NEXUS_PASSWORD=nexus-push",
			"REGISTRY_PASSWORD=registry-robot-password",
		}, envs(stepOf(t, got, "b")))
	})

	t.Run("BindingTheFirstSecretOfAStepThatHadNone", func(t *testing.T) {
		// Step "c" has no secrets: the inverse must leave the field unset,
		// not an empty list, or the round trip is a different revision.
		got, _ := roundTripOp(t, stepSecretOp(&dholev1.SetStepSecret{
			StepId: "c", Env: "TOKEN", Name: "api-token",
		}))
		require.Equal(t, []string{"TOKEN=api-token"}, envs(stepOf(t, got, "c")))
	})

	t.Run("RebindingAnEnvKeepsItsPlaceAndRestoresTheOldName", func(t *testing.T) {
		got, change := roundTripOp(t, stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "REGISTRY_USER", Name: "nexus-user",
		}))
		require.Equal(t, []string{
			"REGISTRY_USER=nexus-user", "REGISTRY_PASSWORD=registry-robot-password",
		}, envs(stepOf(t, got, "b")))
		require.Equal(t, dholev1.ChangeKind_CHANGE_KIND_CHANGED, change.GetKind())
		require.Contains(t, change.GetSummary(), "registry-robot")
		require.Contains(t, change.GetSummary(), "nexus-user")
	})

	t.Run("RemovingTheFirstBindingRestoresItAtTheFront", func(t *testing.T) {
		got, change := roundTripOp(t, stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "REGISTRY_USER", Remove: true,
		}))
		require.Equal(t, []string{"REGISTRY_PASSWORD=registry-robot-password"}, envs(stepOf(t, got, "b")))
		require.Equal(t, dholev1.ChangeKind_CHANGE_KIND_REMOVED, change.GetKind())
	})

	t.Run("RemovingTheLastBindingThereIsLeavesTheFieldUnset", func(t *testing.T) {
		_, _, inverse, err := api.Apply(basePipeline(tenantA), stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "REGISTRY_USER", Remove: true,
		}))
		require.NoError(t, err)
		next, _, _, err := api.Apply(basePipeline(tenantA), stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "REGISTRY_USER", Remove: true,
		}))
		require.NoError(t, err)
		next, _, _, err = api.Apply(next, stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "REGISTRY_PASSWORD", Remove: true,
		}))
		require.NoError(t, err)
		require.Nil(t, stepOf(t, next, "b").Secrets, "an emptied list must encode as no list")
		require.NotNil(t, inverse.GetSetStepSecret().Index, "the inverse of a removal names where it was")
		require.Equal(t, uint32(0), inverse.GetSetStepSecret().GetIndex())
	})

	base := basePipeline(tenantA)
	cases := map[string]struct {
		op   *dholev1.SetStepSecret
		want string
	}{
		"a step that does not exist": {
			op: &dholev1.SetStepSecret{StepId: "nope", Env: "X", Name: "x"}, want: "nope",
		},
		"an empty env": {
			op: &dholev1.SetStepSecret{StepId: "b", Name: "x"}, want: "not an environment variable name",
		},
		"an env that is not a variable name": {
			op: &dholev1.SetStepSecret{StepId: "b", Env: "1-BAD", Name: "x"}, want: "not an environment variable name",
		},
		"removing an env that is not bound": {
			op: &dholev1.SetStepSecret{StepId: "b", Env: "ABSENT", Remove: true}, want: "binds no secret to ABSENT",
		},
		"an index past the end": {
			op: &dholev1.SetStepSecret{StepId: "b", Env: "NEW", Name: "x", Index: index(3)}, want: "index 3",
		},
		"an index on a rebind": {
			op:   &dholev1.SetStepSecret{StepId: "b", Env: "REGISTRY_USER", Name: "x", Index: index(0)},
			want: "keeps its place",
		},
	}
	for name, tc := range cases {
		t.Run("Refuses/"+name, func(t *testing.T) {
			refused(t, proto.Clone(base).(*dholev1.Pipeline), stepSecretOp(tc.op), tc.want)
		})
	}

	t.Run("Refuses/an env the step already binds twice", func(t *testing.T) {
		// Only add_step can produce this. Any choice of occurrence would make
		// some inverse land on the other one, so the operation names nothing.
		p := basePipeline(tenantA)
		stepOf(t, p, "b").Secrets = append(stepOf(t, p, "b").Secrets,
			&dholev1.StepSecret{Name: "again", Env: "REGISTRY_USER"})
		refused(t, p, stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "REGISTRY_USER", Remove: true,
		}), "more than once")
		refused(t, p, stepSecretOp(&dholev1.SetStepSecret{
			StepId: "b", Env: "REGISTRY_USER", Name: "x",
		}), "more than once")
	})

	t.Run("BindingASecretOnAStepWithoutTheCapabilityIsAccepted", func(t *testing.T) {
		// ADR 0028: the capability rule is Validate's, not the operation's. A
		// refusal here would refuse the inverse of the edit that repairs a
		// step added broken — undo would fail on exactly the fix.
		roundTripOp(t, stepSecretOp(&dholev1.SetStepSecret{StepId: "c", Env: "TOKEN", Name: "api-token"}))
	})
}

// TestSetStepCapabilityInvertsInEveryDirection is the same property for a
// step's capabilities.
func TestSetStepCapabilityInvertsInEveryDirection(t *testing.T) {
	caps := func(step *dholev1.Step) []dholev1.Capability { return step.GetCapabilities() }

	t.Run("DeclaringOneAppendsIt", func(t *testing.T) {
		got, change := roundTripOp(t, stepCapabilityOp(&dholev1.SetStepCapability{
			StepId: "b", Capability: dholev1.Capability_CAPABILITY_PRIVILEGED,
		}))
		require.Equal(t, []dholev1.Capability{
			dholev1.Capability_CAPABILITY_NETWORK, dholev1.Capability_CAPABILITY_SECRETS,
			dholev1.Capability_CAPABILITY_PRIVILEGED,
		}, caps(stepOf(t, got, "b")))
		require.Equal(t, dholev1.ChangeKind_CHANGE_KIND_ADDED, change.GetKind())
		require.Contains(t, change.GetSummary(), "CAPABILITY_PRIVILEGED")
	})

	t.Run("DeclaringOneAtAnIndexInsertsItThere", func(t *testing.T) {
		got, _ := roundTripOp(t, stepCapabilityOp(&dholev1.SetStepCapability{
			StepId: "b", Capability: dholev1.Capability_CAPABILITY_HOST_MOUNT, Index: index(0),
		}))
		require.Equal(t, []dholev1.Capability{
			dholev1.Capability_CAPABILITY_HOST_MOUNT,
			dholev1.Capability_CAPABILITY_NETWORK, dholev1.Capability_CAPABILITY_SECRETS,
		}, caps(stepOf(t, got, "b")))
	})

	t.Run("DeclaringTheFirstOfAStepThatHadNone", func(t *testing.T) {
		got, _ := roundTripOp(t, stepCapabilityOp(&dholev1.SetStepCapability{
			StepId: "c", Capability: dholev1.Capability_CAPABILITY_SECRETS,
		}))
		require.Equal(t, []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}, caps(stepOf(t, got, "c")))
	})

	t.Run("WithdrawingTheFirstRestoresItAtTheFront", func(t *testing.T) {
		got, change := roundTripOp(t, stepCapabilityOp(&dholev1.SetStepCapability{
			StepId: "b", Capability: dholev1.Capability_CAPABILITY_NETWORK, Remove: true,
		}))
		require.Equal(t, []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}, caps(stepOf(t, got, "b")))
		require.Equal(t, dholev1.ChangeKind_CHANGE_KIND_REMOVED, change.GetKind())
	})

	t.Run("WithdrawingSecretsFromAStepThatStillBindsOneIsAccepted", func(t *testing.T) {
		// Validate reports the result; the operation does not refuse it, for
		// the reason binding a secret without the capability is accepted.
		roundTripOp(t, stepCapabilityOp(&dholev1.SetStepCapability{
			StepId: "b", Capability: dholev1.Capability_CAPABILITY_SECRETS, Remove: true,
		}))
	})

	base := basePipeline(tenantA)
	cases := map[string]struct {
		op   *dholev1.SetStepCapability
		want string
	}{
		"a step that does not exist": {
			op:   &dholev1.SetStepCapability{StepId: "nope", Capability: dholev1.Capability_CAPABILITY_NETWORK},
			want: "nope",
		},
		"declaring one the step already has": {
			op:   &dholev1.SetStepCapability{StepId: "b", Capability: dholev1.Capability_CAPABILITY_NETWORK},
			want: "already declares CAPABILITY_NETWORK",
		},
		"withdrawing one the step does not have": {
			op: &dholev1.SetStepCapability{
				StepId: "b", Capability: dholev1.Capability_CAPABILITY_PRIVILEGED, Remove: true,
			},
			want: "does not declare CAPABILITY_PRIVILEGED",
		},
		"the unspecified capability": {
			op:   &dholev1.SetStepCapability{StepId: "b"},
			want: "not a capability",
		},
		"an undeclared enum value": {
			op:   &dholev1.SetStepCapability{StepId: "b", Capability: dholev1.Capability(99)},
			want: "not a capability",
		},
		"an index past the end": {
			op: &dholev1.SetStepCapability{
				StepId: "b", Capability: dholev1.Capability_CAPABILITY_PRIVILEGED, Index: index(3),
			},
			want: "index 3",
		},
	}
	for name, tc := range cases {
		t.Run("Refuses/"+name, func(t *testing.T) {
			refused(t, proto.Clone(base).(*dholev1.Pipeline), stepCapabilityOp(tc.op), tc.want)
		})
	}

	t.Run("Refuses/a capability the step lists twice", func(t *testing.T) {
		p := basePipeline(tenantA)
		stepOf(t, p, "b").Capabilities = append(stepOf(t, p, "b").Capabilities,
			dholev1.Capability_CAPABILITY_NETWORK)
		refused(t, p, stepCapabilityOp(&dholev1.SetStepCapability{
			StepId: "b", Capability: dholev1.Capability_CAPABILITY_NETWORK, Remove: true,
		}), "more than once")
	})
}

// TestSetPropertyReachesEveryStepScalar: the image a step runs in, the engine
// kind it must land on and its timeout are each one set_property with an exact
// inverse, and a timeout that is not a uint32 is refused rather than truncated.
func TestSetPropertyReachesEveryStepScalar(t *testing.T) {
	set := func(property, value string) *dholev1.Operation {
		return &dholev1.Operation{Kind: &dholev1.Operation_SetProperty{SetProperty: &dholev1.SetProperty{
			StepId: "c", Property: property, Value: value,
		}}}
	}

	t.Run("image", func(t *testing.T) {
		got, change := roundTripOp(t, set("image", "registry.example/build@sha256:abc"))
		require.Equal(t, "registry.example/build@sha256:abc", stepOf(t, got, "c").GetImage())
		require.Contains(t, change.GetSummary(), "image")
	})
	t.Run("engine_type", func(t *testing.T) {
		got, _ := roundTripOp(t, set("engine_type", "kubernetes"))
		require.Equal(t, "kubernetes", stepOf(t, got, "c").GetEngineType())
	})
	t.Run("timeout_seconds", func(t *testing.T) {
		got, change := roundTripOp(t, set("timeout_seconds", "900"))
		require.Equal(t, uint32(900), stepOf(t, got, "c").GetTimeoutSeconds())
		require.Contains(t, change.GetSummary(), `from "0" to "900"`)
	})
	t.Run("the inverse restores a value that was already set", func(t *testing.T) {
		// Step "c" carries neither an image nor an engine type, so a round trip
		// from it cannot tell "restore the old value" from "clear it".
		p := basePipeline(tenantA)
		stepOf(t, p, "c").Image = "registry.example/old@sha256:def"
		stepOf(t, p, "c").EngineType = "process"
		_, _, inverse, err := api.Apply(p, set("image", "registry.example/new@sha256:abc"))
		require.NoError(t, err)
		require.Equal(t, "registry.example/old@sha256:def", inverse.GetSetProperty().GetValue())
		_, _, inverse, err = api.Apply(p, set("engine_type", "kubernetes"))
		require.NoError(t, err)
		require.Equal(t, "process", inverse.GetSetProperty().GetValue())
	})
	t.Run("timeout_seconds back to unbounded", func(t *testing.T) {
		p := basePipeline(tenantA)
		stepOf(t, p, "c").TimeoutSeconds = 60
		next, _, inverse, err := api.Apply(p, set("timeout_seconds", "0"))
		require.NoError(t, err)
		require.Zero(t, stepOf(t, next, "c").GetTimeoutSeconds())
		require.Equal(t, "60", inverse.GetSetProperty().GetValue())
	})

	for _, bad := range []string{"", "abc", "-1", "1.5", "4294967296"} {
		t.Run("Refuses timeout "+bad, func(t *testing.T) {
			refused(t, basePipeline(tenantA), set("timeout_seconds", bad), "timeout_seconds")
		})
	}
}

// TestADeclarationEditMergesWithAConcurrentEditToAnotherStep: a secret edit
// claims its step and nothing else, so a colleague editing a different step
// from the same base still lands — the rebase does not treat an unclassified
// operation as touching the whole pipeline.
func TestADeclarationEditMergesWithAConcurrentEditToAnotherStep(t *testing.T) {
	for name, op := range map[string]*dholev1.Operation{
		"set_step_secret": stepSecretOp(&dholev1.SetStepSecret{StepId: "b", Env: "NEXUS", Name: "nexus-push"}),
		"set_step_capability": stepCapabilityOp(&dholev1.SetStepCapability{
			StepId: "b", Capability: dholev1.Capability_CAPABILITY_PRIVILEGED,
		}),
	} {
		t.Run(name, func(t *testing.T) {
			h := newRealHarness(t)
			original, base := seed(t, h, tenantA)
			ctx := context.Background()

			_, err := h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
				PipelineId: original.GetId(), BaseRevision: base.ID, Operation: setProperty("a", "oci://example/fetch:v2"),
			}, tokenCarol))
			require.NoError(t, err)

			_, err = h.client.ApplyOperation(ctx, authed(&dholev1.ApplyOperationRequest{
				PipelineId: original.GetId(), BaseRevision: base.ID, Operation: op,
			}, tokenAlice))
			require.NoError(t, err, "an edit to step b conflicted with an edit to step a")
		})
	}
}

// TestValidateReportsASecretWithoutItsCapability: the ADR 0027 rules are a
// property of a definition, so Validate reports them — as errors positioned on
// the step, in exactly the words the scheduler refuses to dispatch the same
// step in. The operations do not refuse them (ADR 0028), so this is where an
// author is told before a run fails.
func TestValidateReportsASecretWithoutItsCapability(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine(
		dholev1.Capability_CAPABILITY_SECRETS, dholev1.Capability_CAPABILITY_NETWORK,
	)})
	redeem := dholev1.Capability_CAPABILITY_SECRETS
	p := &dholev1.Pipeline{
		Id:     "pipe-secrets",
		Tenant: &dholev1.Tenant{Id: tenantA},
		Steps: []*dholev1.Step{
			{Id: "nocap", Secrets: []*dholev1.StepSecret{{Name: "nexus-push", Env: "NEXUS_PASSWORD"}}},
			{
				Id: "badenv", Capabilities: []dholev1.Capability{redeem},
				Secrets: []*dholev1.StepSecret{{Name: "nexus-push", Env: "NEXUS-PASSWORD"}},
			},
			{
				Id: "twice", Capabilities: []dholev1.Capability{redeem},
				Secrets: []*dholev1.StepSecret{{Name: "a", Env: "TOKEN"}, {Name: "b", Env: "TOKEN"}},
			},
			{
				Id: "unnamed", Capabilities: []dholev1.Capability{redeem},
				Secrets: []*dholev1.StepSecret{{Env: "TOKEN"}},
			},
			{
				Id: "fine", Capabilities: []dholev1.Capability{redeem},
				Secrets: []*dholev1.StepSecret{{Name: "nexus-push", Env: "NEXUS_PASSWORD"}},
			},
		},
	}

	got, err := h.client.Validate(context.Background(), authed(&dholev1.ValidateRequest{Pipeline: p}, tokenAlice))
	require.NoError(t, err)

	byStep := map[string][]*dholev1.Diagnostic{}
	for _, d := range got.Msg.GetDiagnostics() {
		byStep[d.GetStepId()] = append(byStep[d.GetStepId()], d)
	}
	for _, id := range []string{"nocap", "badenv", "twice", "unnamed"} {
		want := secrets.ValidateDeclarations(stepOf(t, p, id))
		require.Error(t, want, "the fixture step %q is supposed to be refused by the scheduler", id)
		found := false
		for _, d := range byStep[id] {
			if d.GetMessage() == want.Error() {
				found = true
				require.Equal(t, "error", d.GetSeverity(), "a step the scheduler will refuse is an error, not advice")
			}
		}
		require.Truef(t, found, "no diagnostic on step %q saying %q: got %v", id, want, byStep[id])
	}
	require.Empty(t, byStep["fine"], "a well-formed declaration was reported")
}
