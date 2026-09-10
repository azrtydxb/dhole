package server_test

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/tenancy"
)

// This file is about the half of the quota that had no caller.
//
// `Enforcer.AdmitStep` was wired into the scheduler and `max_concurrent_steps`
// became real; `Enforcer.AdmitRun` was not, so `max_runs_per_day` was a column
// nobody read and the usage ledger held no RUN_STARTED row to bill a run from.
// Both run-creating paths are exercised here: StartRun on the contract, and
// Server.Submit, which is what every trigger fires into.

// admissionTenant is the tenant a plane that was not told otherwise runs
// under, and the one both paths below are scoped to. There is no unscoped run.
const admissionTenant = server.DefaultTenant

// capRunsPerDay narrows the tenant's daily run limit to n, in the plane's own
// database, exactly as an operator provisioning a tenant would.
//
// The limit is set rather than the default relied on: DefaultQuota allows a
// thousand runs a day, and a test that started a thousand runs to reach it
// would be a load test rather than an admission test.
func capRunsPerDay(ctx context.Context, t *testing.T, plane planeStore, n int) {
	t.Helper()
	tenants, err := tenancy.NewStore(plane.db, runstore.DialectSQLite)
	require.NoError(t, err)
	require.NoError(t, tenants.SetQuota(ctx, admissionTenant, tenancy.Quota{
		MaxConcurrentSteps: 64,
		MaxRunsPerDay:      n,
		MaxCASBytes:        1 << 30,
	}))
}

// runsStartedToday is what the daily limit is measured against, read back out
// of the usage ledger the way the enforcer reads it.
func runsStartedToday(ctx context.Context, t *testing.T, plane planeStore) int {
	t.Helper()
	tenants, err := tenancy.NewStore(plane.db, runstore.DialectSQLite)
	require.NoError(t, err)
	used, err := tenants.RunsStartedToday(ctx, admissionTenant, time.Now().UTC())
	require.NoError(t, err)
	return used
}

// TestStartRunRefusesARunPastTheTenantsDailyLimitAndNamesTheQuota is the
// contract half of the gap: a tenant at max_runs_per_day is refused by the
// RPC that creates runs, and told which limit stopped it.
//
// Through the served binary and a real client, because a check wired into an
// api.Server a test constructed itself proves nothing about whether `dhole
// serve` passes it an enforcer — which is the mistake this repository has
// made repeatedly.
func TestStartRunRefusesARunPastTheTenantsDailyLimitAndNamesTheQuota(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv, dir := startWithAPIIn(ctx, t)
	plane := openPlane(t, dir)
	capRunsPerDay(ctx, t, plane, 1)

	client := apiClient(t, srv)
	bootstrap := srv.BootstrapToken()
	approver := plane.issueTokenTheSupportedWay(ctx, t, releaseManager)

	// The one run the tenant is allowed today. gatedPipeline's step matches no
	// engine, so the run stays open and undispatched and records no usage of
	// its own beyond its admission.
	runID := startGatedRun(ctx, t, client, bootstrap, approver, "admission-first")

	// The second is refused, as exhausted rather than as anything a caller
	// would retry blindly, and the reason names the limit.
	create := connect.NewRequest(&dholev1.CreatePipelineRequest{
		PipelineId: "admission-second", Pipeline: gatedPipeline("admission-second"),
	})
	create.Header().Set("Authorization", "Bearer "+bootstrap)
	created, err := client.CreatePipeline(ctx, create)
	require.NoError(t, err)
	approve := connect.NewRequest(&dholev1.ApproveRevisionRequest{
		RevisionId: created.Msg.GetRevision().GetId(),
	})
	approve.Header().Set("Authorization", "Bearer "+approver)
	_, err = client.ApproveRevision(ctx, approve)
	require.NoError(t, err)

	start := connect.NewRequest(&dholev1.StartRunRequest{PipelineId: "admission-second"})
	start.Header().Set("Authorization", "Bearer "+bootstrap)
	refused, err := client.StartRun(ctx, start)
	require.Error(t, err, "a run past the tenant's daily limit was started")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	require.Contains(t, err.Error(), string(tenancy.QuotaMaxRunsPerDay),
		"the refusal does not say which limit stopped the run")
	require.Nil(t, refused)

	// A refused run leaves nothing behind: it is not counted against the
	// limit it was refused by, and no RUN_CREATED exists for anyone to watch.
	// The order is the property — admission before the first event — and a
	// check made after the append would pass every assertion above while
	// leaving a run in the log that never started.
	require.Equal(t, 1, runsStartedToday(ctx, t, plane),
		"the refused run was charged against the quota that refused it")
	open, err := srv.OpenRuns(ctx, admissionTenant)
	require.NoError(t, err)
	require.Equal(t, []string{runID}, open, "the refused run left a run behind in the log")

	// The admitted one is in the ledger, which is what a bill is made of.
	usage, err := tenancyStore(t, plane).UsageForRun(ctx, admissionTenant, runID)
	require.NoError(t, err)
	var started []tenancy.Usage
	for _, u := range usage {
		if u.Kind == tenancy.KindRunStarted {
			started = append(started, u)
		}
	}
	require.Len(t, started, 1, "an admitted run left no %s row to bill from", tenancy.KindRunStarted)
	require.Equal(t, int64(1), started[0].Quantity)
}

// TestSubmitRefusesATriggeredRunPastTheTenantsDailyLimit is the other half:
// every trigger — schedule, http and git — fires into Server.Submit, so a
// quota enforced only in internal/api would be a limit any webhook could walk
// past.
func TestSubmitRefusesATriggeredRunPastTheTenantsDailyLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv, dir := startWithAPIIn(ctx, t)
	plane := openPlane(t, dir)
	capRunsPerDay(ctx, t, plane, 1)

	_, err := srv.Submit(ctx, admissionTenant, gatedPipeline("submit-first"))
	require.NoError(t, err)

	runID, err := srv.Submit(ctx, admissionTenant, gatedPipeline("submit-second"))
	require.Error(t, err, "a triggered run past the tenant's daily limit was started")
	require.ErrorIs(t, err, tenancy.ErrQuotaExceeded)
	require.Contains(t, err.Error(), string(tenancy.QuotaMaxRunsPerDay),
		"the refusal does not say which limit stopped the run")
	require.Empty(t, runID, "a refused run was still given an id")
	require.Equal(t, 1, runsStartedToday(ctx, t, plane),
		"the refused run was charged against the quota that refused it")
	open, err := srv.OpenRuns(ctx, admissionTenant)
	require.NoError(t, err)
	require.Len(t, open, 1, "the refused run left a run behind in the log")

	// And no revision either: Submit saves one before it appends, so a check
	// placed after the save would leave a definition nothing will ever run.
	revisions, err := defstore.NewWithDialect(plane.db, runstore.DialectSQLite).
		Revisions(ctx, admissionTenant, "submit-second")
	require.NoError(t, err)
	require.Empty(t, revisions, "the refused run left a revision behind")
}

// tenancyStore is the usage ledger, opened on the plane's own database.
func tenancyStore(t *testing.T, plane planeStore) *tenancy.Store {
	t.Helper()
	tenants, err := tenancy.NewStore(plane.db, runstore.DialectSQLite)
	require.NoError(t, err)
	return tenants
}
