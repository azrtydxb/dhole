package trigger_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/trigger"
)

const storeTenant = "acme"

// openStore is a trigger store over a fresh SQLite file with the embedded
// migrations applied — the same database the run log and the catalog live in,
// because a trigger table of its own would be a second thing to back up and a
// second thing to restore consistently.
func openStore(t *testing.T) (trigger.Store, *sql.DB) {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return trigger.NewStore(db, runstore.DialectSQLite), db
}

func webhookSpec() trigger.Spec {
	return trigger.Spec{
		ID:           "deploy-hook",
		Kind:         "http",
		PipelineID:   "deploy",
		InputMapping: map[string]string{"event": "ref"},
		Untrusted:    true,
		CreatedBy:    "alice",
	}
}

// TestAStoredTriggerComesBackExactlyAsItWasCreated is what the table is for:
// a trigger an operator created through the contract has to survive the plane
// that accepted it, or it is configuration in a process's memory again.
func TestAStoredTriggerComesBackExactlyAsItWasCreated(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	require.NoError(t, store.Create(ctx, storeTenant, webhookSpec()))

	stored, err := store.List(ctx, storeTenant)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, "deploy-hook", stored[0].ID)
	require.Equal(t, "http", stored[0].Kind)
	require.Equal(t, "deploy", stored[0].PipelineID)
	require.Equal(t, map[string]string{"event": "ref"}, stored[0].InputMapping)
	require.True(t, stored[0].Untrusted)
	require.Equal(t, "alice", stored[0].CreatedBy)
}

// TestATriggerIdIsTakenOnce keeps the id the identity it is claimed to be: a
// schedule's id is the key of its row in trigger_schedules, so two triggers
// sharing one would be two pipelines driven by one due time.
func TestATriggerIdIsTakenOnce(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	require.NoError(t, store.Create(ctx, storeTenant, webhookSpec()))
	err := store.Create(ctx, storeTenant, webhookSpec())
	require.ErrorIs(t, err, trigger.ErrExists)
}

// TestAnotherTenantsTriggerIsNeitherListedNorDeletable: every stored record
// carries a tenant scope, and a trigger starts runs — so a leak here is one
// tenant firing another's pipeline.
func TestAnotherTenantsTriggerIsNeitherListedNorDeletable(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	require.NoError(t, store.Create(ctx, storeTenant, webhookSpec()))

	stored, err := store.List(ctx, "somebody-else")
	require.NoError(t, err)
	require.Empty(t, stored)

	require.ErrorIs(t, store.Delete(ctx, "somebody-else", "deploy-hook"), trigger.ErrNoSuchTrigger)

	// And the owner's is untouched by the attempt.
	stored, err = store.List(ctx, storeTenant)
	require.NoError(t, err)
	require.Len(t, stored, 1)
}

// TestAnUnscopedTriggerCallIsRefused: there is no unscoped query in this
// system, even while only one tenant exists.
func TestAnUnscopedTriggerCallIsRefused(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	require.ErrorIs(t, store.Create(ctx, "", webhookSpec()), runstore.ErrTenantRequired)
	_, err := store.List(ctx, "")
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.ErrorIs(t, store.Delete(ctx, "", "deploy-hook"), runstore.ErrTenantRequired)
}

// TestADeletedTriggerIsGoneAndSayingSoTwiceIsAnError. A delete that reported
// success for a trigger that was never there would let a typo read as a
// trigger removed, which is the one mistake an operator cannot see.
func TestADeletedTriggerIsGoneAndSayingSoTwiceIsAnError(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	require.NoError(t, store.Create(ctx, storeTenant, webhookSpec()))
	require.NoError(t, store.Delete(ctx, storeTenant, "deploy-hook"))

	stored, err := store.List(ctx, storeTenant)
	require.NoError(t, err)
	require.Empty(t, stored)

	require.ErrorIs(t, store.Delete(ctx, storeTenant, "deploy-hook"), trigger.ErrNoSuchTrigger)
}

// TestAGitTriggersSecretIsStoredAndReadableOnlyByThePlane: the forge signs
// with it and the plane verifies with it, so the plane must hold it — but it
// travels back to nobody. The contract's own listing drops it; this is the
// layer that still has it.
func TestAGitTriggersSecretIsStoredAndReadableOnlyByThePlane(t *testing.T) {
	ctx := context.Background()
	store, _ := openStore(t)

	spec := webhookSpec()
	spec.ID, spec.Kind, spec.Secret = "forge", "git", "s3cr3t"
	require.NoError(t, store.Create(ctx, storeTenant, spec))

	stored, err := store.List(ctx, storeTenant)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.Equal(t, "s3cr3t", stored[0].Secret,
		"the plane cannot verify a forge's signature without the secret it signed with")
}
