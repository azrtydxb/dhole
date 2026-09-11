package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/server"
)

// dockerfile is the kind of file ADR 0023 exists for: the bytes a build step
// needs, which a pipeline could not name before because "the definition's
// repository" is something this system does not have.
var dockerfile = []byte("FROM busybox:1.36\nENTRYPOINT [\"/bin/sh\"]\n")

// authedFor stamps the plane's bootstrap credential on a request. There is no
// unauthenticated call on this contract.
func authedFor[T any](msg *T, token string) *connect.Request[T] {
	req := connect.NewRequest(msg)
	req.Header().Set("Authorization", "Bearer "+token)
	return req
}

// TestServeCarriesADefinitionFileFromUploadToARevisionThatPinsIt is ADR 0023
// through the SERVED contract: a real Connect client over real HTTP to a plane
// from server.New/Start, because constructing an api.Server in-process proves
// nothing about whether `dhole serve` mounts what it built — a gap this
// repository has had before.
//
// It follows the whole path in one test on purpose. The upload, the edit that
// declares the file and the revision that comes back are three calls that are
// worthless separately: bytes nobody declared are dead storage, and a
// declaration whose bytes are absent is a run that fails on an engine.
func TestServeCarriesADefinitionFileFromUploadToARevisionThatPinsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := apiClient(t, srv)
	token := srv.BootstrapToken()

	put, err := client.PutDefinitionFile(ctx, authedFor(&dholev1.PutDefinitionFileRequest{
		Content: dockerfile, Path: "Dockerfile", MediaType: "text/plain",
	}, token))
	require.NoError(t, err, "the plane serves no upload path, so a definition cannot carry a file")

	sum := sha256.Sum256(dockerfile)
	require.Equal(t, hex.EncodeToString(sum[:]), put.Msg.GetFile().GetDigest().GetHex(),
		"the bytes were stored under a digest that is not their content hash, so nothing pins them")
	require.Equal(t, uint64(len(dockerfile)), put.Msg.GetFile().GetSizeBytes())

	created, err := client.CreatePipeline(ctx, authedFor(&dholev1.CreatePipelineRequest{
		PipelineId: "carries-a-file",
	}, token))
	require.NoError(t, err)

	// A step that reads the file on an input port with no edge into it. This
	// is the shape ADR 0001 requires: the file is DECLARED, not ambient.
	withStep, err := client.ApplyOperation(ctx, authedFor(&dholev1.ApplyOperationRequest{
		PipelineId:   "carries-a-file",
		BaseRevision: created.Msg.GetRevision().GetId(),
		Operation: &dholev1.Operation{Kind: &dholev1.Operation_SetFile{
			SetFile: &dholev1.SetFile{Path: "Dockerfile", File: put.Msg.GetFile()},
		}},
	}, token))
	require.NoError(t, err)
	require.NotEmpty(t, withStep.Msg.GetDiff().GetChanges())
	require.Equal(t, "Dockerfile", withStep.Msg.GetDiff().GetChanges()[0].GetFilePath(),
		"a file change that names no file is indistinguishable from one that did nothing")

	bound, err := client.ApplyOperation(ctx, authedFor(&dholev1.ApplyOperationRequest{
		PipelineId:   "carries-a-file",
		BaseRevision: withStep.Msg.GetRevision().GetId(),
		Operation: &dholev1.Operation{Kind: &dholev1.Operation_AddStep{
			AddStep: &dholev1.AddStep{Step: &dholev1.Step{
				Id:          "build",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				Inputs:      []*dholev1.Port{{Name: "context", Type: blobType()}},
				Outputs:     []*dholev1.Port{{Name: "image", Type: blobType()}},
				FileInputs:  []*dholev1.FileInput{{Port: "context", Path: "Dockerfile"}},
			}},
		}},
	}, token))
	require.NoError(t, err)

	// The revision is what a run pins, so the file has to come back out of it.
	got, err := client.GetPipeline(ctx, authedFor(&dholev1.GetPipelineRequest{
		PipelineId: "carries-a-file",
		RevisionId: bound.Msg.GetRevision().GetId(),
	}, token))
	require.NoError(t, err)
	require.Len(t, got.Msg.GetPipeline().GetFiles(), 1,
		"the stored revision lost the file it was told to carry")
	require.Equal(t, put.Msg.GetFile().GetDigest().GetHex(),
		got.Msg.GetPipeline().GetFiles()[0].GetDigest().GetHex())

	// And the bytes really are in the plane's own store, under that digest —
	// which is what an engine will fetch when the step runs.
	rc, err := srv.CAS().Get(ctx, server.DefaultTenant, got.Msg.GetPipeline().GetFiles()[0].GetDigest())
	require.NoError(t, err, "the revision pins bytes the plane does not hold")
	require.NoError(t, rc.Close())
}

// TestTheServedAPIRefusesADefinitionFileWhoseBytesNobodyUploaded keeps the
// declaration and the bytes together. A revision naming a digest the tenant
// does not hold is a run that fails on an engine fetching an input, minutes
// later and a long way from the edit that caused it.
func TestTheServedAPIRefusesADefinitionFileWhoseBytesNobodyUploaded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := apiClient(t, srv)
	token := srv.BootstrapToken()

	created, err := client.CreatePipeline(ctx, authedFor(&dholev1.CreatePipelineRequest{
		PipelineId: "names-nothing",
	}, token))
	require.NoError(t, err)

	_, err = client.ApplyOperation(ctx, authedFor(&dholev1.ApplyOperationRequest{
		PipelineId:   "names-nothing",
		BaseRevision: created.Msg.GetRevision().GetId(),
		Operation: &dholev1.Operation{Kind: &dholev1.Operation_SetFile{
			SetFile: &dholev1.SetFile{Path: "Dockerfile", File: &dholev1.File{
				Digest: &dholev1.Digest{
					Algo: "sha256",
					Hex:  "0000000000000000000000000000000000000000000000000000000000000000",
				},
			}},
		}},
	}, token))
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.Contains(t, err.Error(), "not stored")
}

// blobType is the port type the file-reading step declares. A port with no
// type at all is refused by the type checker, which would fail this test for a
// reason that has nothing to do with files.
func blobType() *dholev1.PortType {
	return &dholev1.PortType{
		Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{MediaType: "text/plain"}},
	}
}

// TestTheServedUploadRecordsTheFileForTheCollector is the wiring half of the
// leak ADR 0023 promised was not there. The collector's candidates come from
// tables somebody writes: a step output is Referenced, and until this was
// wired a definition file was only ever Put. The bytes were then invisible to
// every sweep and retained for ever against max_cas_bytes — the quota ADR 0023
// makes the only ceiling on a definition's size.
//
// It reads the plane's own database rather than constructing a collector,
// because what is in doubt here is whether `dhole serve` mounts the upload
// path that records, not whether the collector works: internal/cas proves
// that on both dialects.
func TestTheServedUploadRecordsTheFileForTheCollector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv, dir := startWithAPIIn(ctx, t)
	client := apiClient(t, srv)

	put, err := client.PutDefinitionFile(ctx, authedFor(&dholev1.PutDefinitionFileRequest{
		Content: dockerfile, Path: "Dockerfile", MediaType: "text/plain",
	}, srv.BootstrapToken()))
	require.NoError(t, err)

	db, err := runstore.OpenSQLite(filepath.Join(dir, "dhole.db"))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var rows int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM definition_blobs WHERE tenant_id = ? AND digest = ?`,
		server.DefaultTenant, "sha256:"+put.Msg.GetFile().GetDigest().GetHex()).Scan(&rows))
	require.Equal(t, 1, rows,
		"the plane stored the bytes without recording them, so no sweep will ever see them again")
}
