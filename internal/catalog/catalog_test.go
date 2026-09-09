package catalog_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/catalog"
)

const tenant = "acme"

// openCatalog opens a catalog over a fresh SQLite file in the test's own
// directory, applying the embedded migrations, and closes it at the end.
func openCatalog(t *testing.T) catalog.Store {
	t.Helper()
	return openCatalogAt(t, filepath.Join(t.TempDir(), "runs.db"))
}

// openCatalogAt opens a catalog over a named file, so a test can reopen the
// same one.
func openCatalogAt(t *testing.T, path string) catalog.Store {
	t.Helper()
	c, err := catalog.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// notifyPlugin is a manifest for a plugin that sends a message: the archetypal
// at-most-once step, and the one whose effect class must never be widened.
func notifyPlugin() catalog.Manifest {
	return catalog.Manifest{
		Namespace:    "acme",
		Name:         "notify",
		Version:      "1.4.2",
		Digest:       &dholev1.Digest{Algo: "sha256", Hex: "abc123"},
		Kind:         catalog.KindStep,
		EffectClass:  dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK, dholev1.Capability_CAPABILITY_SECRETS},
		InputSchema:  []byte(`{"type":"object","properties":{"body":{"type":"string"}}}`),
		OutputSchema: []byte(`{"type":"object","properties":{"messageId":{"type":"string"}}}`),
		EngineTypes:  []string{"container", "process"},
	}
}

// TestManifestDeclaresEffectClassAndCapabilities is the catalog's whole
// reason to exist: what a type declares about itself is what comes back out.
// ADR 0002 makes the effect class the input to both cache policy and retry
// policy, so a catalog that quietly normalises, defaults or drops it would
// turn an unrepeatable action into a retryable one.
func TestManifestDeclaresEffectClassAndCapabilities(t *testing.T) {
	ctx := context.Background()
	c := openCatalog(t)

	m := notifyPlugin()
	require.NoError(t, c.Publish(ctx, tenant, m))

	entry, err := c.Resolve(ctx, tenant, "acme/notify@1.4.2")
	require.NoError(t, err)

	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, entry.EffectClass)
	require.Equal(t, m.Capabilities, entry.Capabilities)
	require.Equal(t, catalog.KindStep, entry.Kind)
	require.Equal(t, "acme", entry.Namespace)
	require.Equal(t, "notify", entry.Name)
	require.Equal(t, "1.4.2", entry.Version)
	require.Equal(t, "sha256", entry.Digest.GetAlgo())
	require.Equal(t, "abc123", entry.Digest.GetHex())
	require.JSONEq(t, string(m.InputSchema), string(entry.InputSchema))
	require.JSONEq(t, string(m.OutputSchema), string(entry.OutputSchema))
	require.Equal(t, []string{"container", "process"}, entry.EngineTypes)
	require.Empty(t, entry.OverrideWarnings)

	listed, err := c.List(ctx, tenant)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, entry.Ref(), listed[0].Ref())
}

// TestStepInheritsEffectClassFromManifest pins ADR 0002's defaulting rule and,
// more importantly, its asymmetry. A step that says nothing takes the plugin's
// declaration, so the dangerous value is never typed by hand. A step that
// narrows — claiming a stricter class than the plugin — is accepted silently,
// because being more careful than the author asked is always safe. A step that
// widens — claiming a weaker class, here PURE over the plugin's AT_MOST_ONCE —
// would make an unrepeatable action cacheable and freely retryable, so it comes
// back flagged.
func TestStepInheritsEffectClassFromManifest(t *testing.T) {
	ctx := context.Background()
	c := openCatalog(t)
	require.NoError(t, c.Publish(ctx, tenant, notifyPlugin()))

	// No declaration at all: the plugin's class is inherited, unflagged.
	inherited, err := c.ResolveStep(ctx, tenant, &dholev1.Step{
		Id:        "notify-oncall",
		PluginRef: "acme/notify@1.4.2",
	})
	require.NoError(t, err)
	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, inherited.EffectClass)
	require.Empty(t, inherited.OverrideWarnings)

	// Widening: the plugin says the action must run at most once, the step
	// claims it is pure. Honoured — the step author may know something — but
	// flagged, because this is the direction that silently enables caching and
	// automatic retry of a side effect.
	widened, err := c.ResolveStep(ctx, tenant, &dholev1.Step{
		Id:          "notify-oncall",
		PluginRef:   "acme/notify@1.4.2",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
	})
	require.NoError(t, err)
	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_PURE, widened.EffectClass)
	require.Len(t, widened.OverrideWarnings, 1)
	require.Contains(t, widened.OverrideWarnings[0], "EFFECT_CLASS_PURE")
	require.Contains(t, widened.OverrideWarnings[0], "EFFECT_CLASS_AT_MOST_ONCE")
	require.Contains(t, widened.OverrideWarnings[0], "notify-oncall")

	// Narrowing is the safe direction and stays silent: a pure plugin used by
	// a step that insists on at-most-once merely forgoes the cache.
	require.NoError(t, c.Publish(ctx, tenant, pureManifest()))
	narrowed, err := c.ResolveStep(ctx, tenant, &dholev1.Step{
		Id:          "build",
		PluginRef:   "acme/build@2.0.0",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
	})
	require.NoError(t, err)
	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, narrowed.EffectClass)
	require.Empty(t, narrowed.OverrideWarnings)
}

// pureManifest is a cacheable build plugin — the other population ADR 0002
// has to host.
func pureManifest() catalog.Manifest {
	return catalog.Manifest{
		Namespace:    "acme",
		Name:         "build",
		Version:      "2.0.0",
		Digest:       &dholev1.Digest{Algo: "sha256", Hex: "def456"},
		Kind:         catalog.KindStep,
		EffectClass:  dholev1.EffectClass_EFFECT_CLASS_PURE,
		InputSchema:  []byte(`{"type":"object"}`),
		OutputSchema: []byte(`{"type":"object"}`),
		EngineTypes:  []string{"container"},
	}
}

// TestCatalogSurvivesControlPlaneRestart is the line ADR 0010 draws. The
// runtime engine registry is ephemeral on purpose: an instance that stops
// heartbeating must age out, because a claim that a dead process is alive is
// worse than no claim. The catalog is the opposite — what a step type is, what
// it may do, and what it hashes to must outlive every restart, or a reboot
// would forget the meaning of every pipeline already saved.
func TestCatalogSurvivesControlPlaneRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runs.db")

	first := openCatalogAt(t, path)
	require.NoError(t, first.Publish(ctx, tenant, notifyPlugin()))
	require.NoError(t, first.Close())

	// The control plane restarts: a brand-new process, a brand-new handle.
	second := openCatalogAt(t, path)
	entry, err := second.Resolve(ctx, tenant, "acme/notify@1.4.2")
	require.NoError(t, err)
	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, entry.EffectClass)
	require.Equal(t, "abc123", entry.Digest.GetHex())
	require.Equal(t, []string{"container", "process"}, entry.EngineTypes)

	listed, err := second.List(ctx, tenant)
	require.NoError(t, err)
	require.Len(t, listed, 1)
}

// TestManifestWithInvalidJSONSchemaIsRejected keeps a broken contract out of
// the durable store, where every later run would inherit it. The error has to
// say *which* schema: a publisher holding two documents must not have to guess.
func TestManifestWithInvalidJSONSchemaIsRejected(t *testing.T) {
	ctx := context.Background()
	c := openCatalog(t)

	badInput := notifyPlugin()
	badInput.InputSchema = []byte(`{"type":"not-a-type"}`)
	err := c.Publish(ctx, tenant, badInput)
	require.Error(t, err)
	require.Contains(t, err.Error(), "input schema")
	require.NotContains(t, err.Error(), "output schema")

	badOutput := notifyPlugin()
	badOutput.OutputSchema = []byte(`{"properties":`)
	err = c.Publish(ctx, tenant, badOutput)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output schema")
	require.NotContains(t, err.Error(), "input schema")

	// Nothing was stored: a rejected publish must not leave half a type behind.
	_, err = c.Resolve(ctx, tenant, "acme/notify@1.4.2")
	require.ErrorIs(t, err, catalog.ErrNotFound)
}

// TestEveryMethodIsTenantScoped: the spec allows no unscoped query, even while
// one tenant exists, and entries published by one tenant are invisible to
// another. An empty tenant is a caller bug, never a wildcard.
func TestEveryMethodIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	c := openCatalog(t)

	require.EqualError(t, c.Publish(ctx, "", notifyPlugin()), "tenant scope required")
	_, err := c.Resolve(ctx, "", "acme/notify@1.4.2")
	require.EqualError(t, err, "tenant scope required")
	_, err = c.List(ctx, "")
	require.EqualError(t, err, "tenant scope required")
	_, err = c.ResolveStep(ctx, "", &dholev1.Step{PluginRef: "acme/notify@1.4.2"})
	require.EqualError(t, err, "tenant scope required")

	// One tenant's catalog is not another's.
	require.NoError(t, c.Publish(ctx, tenant, notifyPlugin()))
	_, err = c.Resolve(ctx, "other", "acme/notify@1.4.2")
	require.ErrorIs(t, err, catalog.ErrNotFound)
	listed, err := c.List(ctx, "other")
	require.NoError(t, err)
	require.Empty(t, listed)
}

// TestResolveUnknownRefIsDistinguishableFromZeroEntry: a miss must be an error
// the caller can test for, not an Entry whose zero EffectClass would read as
// "unspecified" and quietly default downstream.
func TestResolveUnknownRefIsDistinguishableFromZeroEntry(t *testing.T) {
	ctx := context.Background()
	c := openCatalog(t)

	_, err := c.Resolve(ctx, tenant, "acme/nothing@9.9.9")
	require.ErrorIs(t, err, catalog.ErrNotFound)
	require.Contains(t, err.Error(), "acme/nothing@9.9.9")

	_, err = c.ResolveStep(ctx, tenant, &dholev1.Step{Id: "s", PluginRef: "acme/nothing@9.9.9"})
	require.ErrorIs(t, err, catalog.ErrNotFound)
}

// TestMalformedRefIsRejectedWithTheExpectedForm: the ref grammar is a user
// contract, so a bad one is answered with the shape that would have worked
// rather than a bare parse failure or a not-found.
func TestMalformedRefIsRejectedWithTheExpectedForm(t *testing.T) {
	ctx := context.Background()
	c := openCatalog(t)

	for _, ref := range []string{"", "acme/notify", "acme@1.0.0", "acme/notify@", "a/b/c@1.0.0", "/notify@1.0.0"} {
		_, err := c.Resolve(ctx, tenant, ref)
		require.ErrorIs(t, err, catalog.ErrMalformedRef, "ref %q", ref)
		require.Contains(t, err.Error(), "namespace/name@version", "ref %q", ref)
		require.NotErrorIs(t, err, catalog.ErrNotFound, "ref %q", ref)
	}
}

// TestRepublishingTheSameVersionIsRefusedUnlessIdentical fixes a version as
// immutable. Task 35 resolves plugin versions into a per-revision lockfile and
// dispatches on the recorded digest; if acme/notify@1.4.2 could be replaced,
// every lockfile already written would silently point at different code and
// every cache key folded over it would be a lie (ADR 0010). A byte-identical
// republish is still a no-op, because publishing is retried and re-running an
// idempotent deploy must not fail.
func TestRepublishingTheSameVersionIsRefusedUnlessIdentical(t *testing.T) {
	ctx := context.Background()
	c := openCatalog(t)

	require.NoError(t, c.Publish(ctx, tenant, notifyPlugin()))
	require.NoError(t, c.Publish(ctx, tenant, notifyPlugin()), "identical republish is a no-op")

	changed := notifyPlugin()
	changed.EffectClass = dholev1.EffectClass_EFFECT_CLASS_PURE
	err := c.Publish(ctx, tenant, changed)
	require.ErrorIs(t, err, catalog.ErrVersionExists)
	require.Contains(t, err.Error(), "acme/notify@1.4.2")

	// The published version is untouched by the refused republish.
	entry, err := c.Resolve(ctx, tenant, "acme/notify@1.4.2")
	require.NoError(t, err)
	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, entry.EffectClass)

	// A new version alongside it is ordinary.
	next := notifyPlugin()
	next.Version = "1.4.3"
	next.Digest = &dholev1.Digest{Algo: "sha256", Hex: "beef01"}
	require.NoError(t, c.Publish(ctx, tenant, next))
	listed, err := c.List(ctx, tenant)
	require.NoError(t, err)
	require.Len(t, listed, 2)
}

// TestIncompleteManifestIsRejected: the durable store is the last place a
// half-declared type can be caught before a scheduler has to guess.
func TestIncompleteManifestIsRejected(t *testing.T) {
	ctx := context.Background()
	c := openCatalog(t)

	missingClass := notifyPlugin()
	missingClass.EffectClass = dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED
	require.ErrorContains(t, c.Publish(ctx, tenant, missingClass), "effect class")

	badKind := notifyPlugin()
	badKind.Kind = catalog.Kind("plugin")
	require.ErrorContains(t, c.Publish(ctx, tenant, badKind), "kind")

	noName := notifyPlugin()
	noName.Name = ""
	require.ErrorContains(t, c.Publish(ctx, tenant, noName), "name")

	noDigest := notifyPlugin()
	noDigest.Digest = nil
	require.ErrorContains(t, c.Publish(ctx, tenant, noDigest), "digest")
}
