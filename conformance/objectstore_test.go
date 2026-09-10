package conformance

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/blobstore"
)

// TestTheHarnessNamesTheObjectStoreBackendItHandsTheEngine holds the suite to
// the contract it tests engines against.
//
// The suite used to hand an engine a bare DHOLE_BLOB_DIR and nothing else,
// which is a harness convention rather than the store protocol: an engine
// written against docs/wire-contract.md selects its backend from
// DHOLE_OBJECT_STORE and would either have to guess, or default to a store the
// deployment did not choose. That is the same drift that once left
// `dhole-engine` unrunnable through the suite built to test engines — the two
// spelling the same configuration differently — and it is worth an assertion
// on the environment itself rather than only an end-to-end pass that a
// permissive engine would give anyway.
func TestTheHarnessNamesTheObjectStoreBackendItHandsTheEngine(t *testing.T) {
	h := &harness{blobDir: "/tmp/conformance-blobs"}
	env := dholeEnv(h.engineEnv("nats://127.0.0.1:4222"))

	require.Contains(t, env, "DHOLE_OBJECT_STORE="+blobstore.KindFilesystem,
		"the suite must name the backend it is handing the engine, because "+
			"DHOLE_BLOB_DIR alone is not the store protocol")
	require.Contains(t, env, "DHOLE_BLOB_DIR="+h.blobDir)
}

// TestTheHarnessSetsNoS3ConfigurationAlongsideItsFilesystemStore keeps the
// filesystem case honest: an engine that read a bucket name here would be
// exercising a path the suite has no server for.
func TestTheHarnessSetsNoS3ConfigurationAlongsideItsFilesystemStore(t *testing.T) {
	h := &harness{blobDir: "/tmp/conformance-blobs"}
	for _, entry := range dholeEnv(h.engineEnv("nats://127.0.0.1:4222")) {
		require.False(t, strings.HasPrefix(entry, "DHOLE_S3_"),
			"the suite runs a filesystem store; %q would tell an engine otherwise", entry)
	}
}

// dholeEnv keeps a failure readable: engineEnv inherits the whole process
// environment, and an assertion that dumps it buries the one line that matters.
func dholeEnv(env []string) []string {
	var ours []string
	for _, entry := range env {
		if strings.HasPrefix(entry, "DHOLE_") {
			ours = append(ours, entry)
		}
	}
	return ours
}
