package server_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/secrets"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/tenant"
)

// accountSecretValue is what tenant acme's engine must receive in its own
// account, and nothing else.
const accountSecretValue = "account-correcthorsebatterystaple"

// TestATenantInItsOwnAccountRedeemsThroughTheEmbeddedServer: a tenant given its
// own NATS account (ADR 0014) has engines whose credentials place them in that
// account, and an account is its own subject namespace. The plane served
// redemption only on its own connection, in its own account, so such an engine
// asked where nothing answered at all (ADR 0031).
func TestATenantInItsOwnAccountRedeemsThroughTheEmbeddedServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// A bus with a plane identity of its own, and then the tenant's account
	// and an engine credential inside it, provisioned the way a deployment
	// provisions them.
	busSrv, err := bus.StartEmbeddedWithTiers(t.TempDir(), tenantID, []string{"trusted"})
	require.NoError(t, err)
	t.Cleanup(busSrv.Close)
	accountCreds, err := tenant.ProvisionAccount(ctx, busSrv, "acme")
	require.NoError(t, err)
	engineCreds, err := tenant.ProvisionTierUser(ctx, busSrv, "acme", "trusted")
	require.NoError(t, err)

	src := secrets.NewMapSource()
	src.Set("acme", "harbor-robot", accountSecretValue)
	src.Set("globex", "harbor-robot", "globex-"+accountSecretValue)
	dir := t.TempDir()
	srv, err := server.New(server.Config{
		APIAddr: "127.0.0.1:0",

		Mode:           server.ModeDistributed,
		BusURL:         busSrv.PlaneURL(),
		StoreDSN:       filepath.Join(dir, "dhole.db"),
		BlobRoot:       filepath.Join(dir, "state"),
		StepSecrets:    src,
		SecretAccounts: map[string]string{"acme": accountCreds},
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() { stopPlane(t, srv) })

	harbor := secrets.Reference{Source: secrets.SourceStep, Secret: "harbor-robot", Binding: "REGISTRY_PASSWORD"}
	engine, err := bus.Connect(ctx, engineCreds)
	require.NoError(t, err)
	t.Cleanup(engine.Close)
	redeemer := secrets.NewBusRedeemer(engine, bus.SubjectSecretRedeem())

	mine, err := srv.Secrets().Issue(ctx, secrets.Scope{TenantID: "acme"}, harbor, time.Minute)
	require.NoError(t, err)
	value, err := redeemer.Redeem(ctx, "acme", mine)
	require.NoError(t, err, "an engine in its tenant's own account could not redeem its handle")
	require.Equal(t, accountSecretValue, value)

	// A handle of another tenant, presented from inside acme's account, is
	// refused and names nothing.
	theirs, err := srv.Secrets().Issue(ctx, secrets.Scope{TenantID: "globex"}, harbor, time.Minute)
	require.NoError(t, err)
	_, err = redeemer.Redeem(ctx, "acme", theirs)
	require.Error(t, err, "an engine in acme's account redeemed a handle issued for globex")
	require.NotContains(t, err.Error(), accountSecretValue)

	// Inside the account the account is the tenant: the account credential
	// may spell another tenant's subject, and is still acme.
	account, err := bus.Connect(ctx, accountCreds)
	require.NoError(t, err)
	t.Cleanup(account.Close)
	spelled, err := srv.Secrets().Issue(ctx, secrets.Scope{TenantID: "globex"}, harbor, time.Minute)
	require.NoError(t, err)
	reply, err := account.RequestRaw(ctx, bus.SubjectSecretRedeemFor("globex"), []byte(spelled.GetHandle()))
	require.NoError(t, err, "nothing answered in acme's account")
	require.Contains(t, string(reply), secrets.RefusalPrefix,
		"a request in acme's account spelling globex's subject redeemed globex's handle")

	// And the legacy subject, inside the account, for an engine written
	// before the tenant was in the subject.
	legacy, err := srv.Secrets().Issue(ctx, secrets.Scope{TenantID: "acme"}, harbor, time.Minute)
	require.NoError(t, err)
	reply, err = engine.RequestRaw(ctx, bus.SubjectSecretRedeem(), []byte(legacy.GetHandle()))
	require.NoError(t, err, "nothing answered the legacy subject in acme's account")
	require.Equal(t, accountSecretValue, string(reply))
}

// TestAPlaneSweepsTheHandlesOfARunNoLongerOpen: an approval denied and a
// halting model step close a run without the scheduler, so nothing on those
// paths revokes its handles. The plane's sweep revokes every handle whose run
// is not in its tenant's open-run index (ADR 0031).
func TestAPlaneSweepsTheHandlesOfARunNoLongerOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	src := secrets.NewMapSource()
	src.Set(tenantID, "harbor-robot", stepSecretValue)
	srv := startSecretPlane(ctx, t, t.TempDir(), src)

	open, err := srv.Submit(ctx, tenantID, sleeper("sweep-keeps-my-handles"))
	require.NoError(t, err)
	harbor := secrets.Reference{Source: secrets.SourceStep, Secret: "harbor-robot", Binding: "REGISTRY_PASSWORD"}
	closed, err := srv.Secrets().Issue(ctx, secrets.Scope{
		TenantID: tenantID, RunID: "run-closed-by-an-approval", StepID: "push", Attempt: 1,
	}, harbor, 10*time.Minute)
	require.NoError(t, err)
	live, err := srv.Secrets().Issue(ctx, secrets.Scope{
		TenantID: tenantID, RunID: open, StepID: "push", Attempt: 1,
	}, harbor, 10*time.Minute)
	require.NoError(t, err)

	// Two sweep ticks, so one has certainly run after the handles existed.
	time.Sleep(11 * time.Second)

	_, err = srv.Secrets().Redeem(ctx, closed.GetHandle())
	require.Error(t, err, "a handle of a run that is no longer open survived the plane's sweep")
	value, err := srv.Secrets().Redeem(ctx, live.GetHandle())
	require.NoError(t, err, "the sweep revoked a handle of a run that is still open")
	require.Equal(t, stepSecretValue, value)
}

// TestTwoPlanesShareTheirSecretHandles is `controlPlane.replicas: 2` through
// the server itself: two planes over one bus, each with its own broker. A
// handle one issues is redeemed through the other and revoked from either,
// and an engine's request is answered by exactly one of them (ADR 0031).
func TestTwoPlanesShareTheirSecretHandles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	busSrv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(busSrv.Close)
	src := secrets.NewMapSource()
	src.Set(tenantID, "harbor-robot", stepSecretValue)
	start := func(name string) *server.Server {
		dir := t.TempDir()
		srv, err := server.New(server.Config{
			APIAddr: "127.0.0.1:0",

			Mode:         server.ModeDistributed,
			BusURL:       busSrv.URL(),
			StoreDSN:     filepath.Join(dir, "dhole.db"),
			BlobRoot:     filepath.Join(dir, "state"),
			DeploymentID: name,
			StepSecrets:  src,
		})
		require.NoError(t, err)
		require.NoError(t, srv.Start(ctx))
		t.Cleanup(func() { stopPlane(t, srv) })
		return srv
	}
	a, b := start("plane-a"), start("plane-b")
	harbor := secrets.Reference{Source: secrets.SourceStep, Secret: "harbor-robot", Binding: "REGISTRY_PASSWORD"}

	ref, err := a.Secrets().Issue(ctx, secrets.Scope{TenantID: tenantID}, harbor, time.Minute)
	require.NoError(t, err)
	value, err := b.Secrets().RedeemFor(ctx, tenantID, ref.GetHandle())
	require.NoError(t, err, "plane B refused a handle plane A issued")
	require.Equal(t, stepSecretValue, value)
	_, err = a.Secrets().RedeemFor(ctx, tenantID, ref.GetHandle())
	require.Error(t, err, "plane A redeemed a handle plane B had already spent")

	// No run, so neither plane's sweep can be what revokes it.
	revoked, err := a.Secrets().Issue(ctx, secrets.Scope{TenantID: tenantID}, harbor, time.Minute)
	require.NoError(t, err)
	n, err := b.Secrets().RevokeHandles(ctx, revoked.GetHandle())
	require.NoError(t, err)
	require.Equal(t, 1, n, "plane B could not revoke a handle plane A issued")
	_, err = a.Secrets().RedeemFor(ctx, tenantID, revoked.GetHandle())
	require.Error(t, err, "a handle revoked on plane B was redeemed on plane A")

	engine, err := bus.Connect(ctx, busSrv.URL())
	require.NoError(t, err)
	t.Cleanup(engine.Close)
	redeemer := secrets.NewBusRedeemer(engine, bus.SubjectSecretRedeem())
	for range 10 {
		ref, err := a.Secrets().Issue(ctx, secrets.Scope{TenantID: tenantID}, harbor, time.Minute)
		require.NoError(t, err)
		value, err := redeemer.Redeem(ctx, tenantID, ref)
		require.NoError(t, err, "an engine's redemption failed with two planes serving")
		require.Equal(t, stepSecretValue, value)
	}
}
