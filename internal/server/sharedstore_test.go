package server_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/server"
)

// The bug this pins, seen on a real cluster: the engine wrote a step's log and
// its outputs to its own pod's disk, and the control plane looked for them on
// its own. Every step succeeded and nothing it produced could be read.
//
// TestSingleBinaryAndDistributedParity did not catch it because it hands the
// external engine `srv.CAS()` — the very same Go object the plane uses, which
// no two processes ever share. Here the plane and the engine build their own
// stores, as two processes must, and only the storage underneath is common.
// That is the whole difference between the deployment that works and the one
// that does not.
func TestAPlaneAndAnEngineWithSeparateStoresOverOneBackingStoreExchangeArtifacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// The one thing they share. In production this is a bucket; here it is a
	// directory, because what is under test is that neither process holds the
	// other's store object, not which backend it is.
	backing := t.TempDir()

	// Distributed, so the plane runs NO engine of its own. An embedded engine
	// would share the plane's process and its store, and would quietly do the
	// work this test means to give to the separate one.
	planeBlobs := blobstore.NewFilesystem(backing)
	dir := t.TempDir()
	busURL := startExternalNATS(t)
	srv, err := server.New(server.Config{
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeDistributed,
		BusURL:   busURL,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
		Blobs:    planeBlobs,
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})

	// The engine's own stores. Separate objects, same bytes underneath —
	// exactly what DHOLE_OBJECT_STORE pointing two processes at one bucket
	// produces.
	engineBlobs := blobstore.NewFilesystem(backing)
	stopEngine := startExternalEngine(ctx, t, busURL, engineBlobs, cas.NewOverBlobs(engineBlobs))
	t.Cleanup(stopEngine)

	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "a")
	requireStepSucceeded(t, events, "b")

	// b's input is a's output, so b succeeding at all proves an artifact
	// crossed from the engine's store to where the plane could plan against
	// it. Reading the bytes through the PLANE's store proves the other
	// direction too.
	out := outputBytes(ctx, t, srv, events, "b", "out")
	require.Contains(t, string(out), "hello from a",
		"the plane could not read what the engine stored")

	// And the authoritative log, which is the half a person actually looks at.
	logKey := logKeyOf(t, events, "b")
	require.NotEmpty(t, logKey, "the step reported no authoritative log")
	rc, err := planeBlobs.Read(ctx, tenantID, logKey)
	require.NoError(t, err, "the plane cannot read the log the engine wrote")
	require.NoError(t, rc.Close())
}
