package cas_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/defstore"
)

// uploadFile puts bytes through the upload path a definition's files take and
// returns the File a revision would carry.
func uploadFile(t *testing.T, gc *cas.GC, tenantID, path, content string) *dholev1.File {
	t.Helper()
	files := cas.NewDefinitionFiles(gc.Store, gc.DB, gc.Dialect)
	digest, err := files.Put(context.Background(), tenantID, strings.NewReader(content))
	require.NoError(t, err)
	return &dholev1.File{Path: path, Digest: digest, SizeBytes: uint64(len(content))}
}

// saveRevision stores a revision of pipelineID carrying exactly these files,
// the way an edit that attaches or replaces one does.
func saveRevision(
	t *testing.T, gc *cas.GC, tenantID, pipelineID string, files ...*dholev1.File,
) defstore.Revision {
	t.Helper()
	defs := defstore.NewWithDialect(gc.DB, gc.Dialect)
	rev, err := defs.Save(context.Background(), tenantID,
		&dholev1.Pipeline{Id: pipelineID, Files: files}, "author")
	require.NoError(t, err)
	return rev
}

// collectingAll is a retain window wide open on both ends: every run and every
// upload is older than it. Tests use it so that what survives a collection
// survives because something PINS it, never because it was merely young.
const collectingAll = -time.Second

// TestADefinitionFileNoRevisionCarriesIsCollected is the defect ADR 0023's
// Consequences already promised was not there: "a file removed by an operation
// is not deleted from the CAS: it is unreferenced, and the collector reclaims
// it when nothing points at it". The collector enumerated blob_refs, whose
// only writer is a step OUTPUT, so a definition file was never even examined
// and a tenant replacing a large file on every edit grew without bound against
// the quota those files are charged to.
func TestADefinitionFileNoRevisionCarriesIsCollected(t *testing.T) {
	ctx := context.Background()
	gc, store, _, _ := newGC(t)

	orphan := uploadFile(t, gc, "acme", "Dockerfile", "FROM scratch\n")

	freed, err := gc.Collect(ctx, "acme", collectingAll)
	require.NoError(t, err)
	require.Equal(t, 1, freed, "the upload no revision carries is the one blob to reclaim")
	require.False(t, has(t, store, "acme", orphan.GetDigest()),
		"a definition file nothing carries must be collected, which is what ADR 0023 promises")
}

// TestAPinnedRevisionStillResolvesEveryFileItCarriesAfterTheHeadMovedOn is the
// constraint the whole feature is subordinate to. A run pins a revision, and
// pinning the revision pins its bytes: the file the pipeline's head replaced
// three edits ago is still the file that revision runs. Collecting it would
// break a run that is required to be reproducible, which is a far worse defect
// than the leak this change is fixing.
func TestAPinnedRevisionStillResolvesEveryFileItCarriesAfterTheHeadMovedOn(t *testing.T) {
	ctx := context.Background()
	gc, store, _, _ := newGC(t)

	pinned := uploadFile(t, gc, "acme", "Dockerfile", "FROM alpine:3.20\n")
	saveRevision(t, gc, "acme", "ci", pinned)

	// The head moves on: the same path, different bytes, a new revision.
	current := uploadFile(t, gc, "acme", "Dockerfile", "FROM alpine:3.21\n")
	saveRevision(t, gc, "acme", "ci", current)

	freed, err := gc.Collect(ctx, "acme", collectingAll)
	require.NoError(t, err)
	require.Equal(t, 0, freed, "both files are carried by a revision; neither is collectable")

	require.True(t, has(t, store, "acme", pinned.GetDigest()),
		"the file the superseded revision carries must survive: a run pinned to it must still resolve it")
	require.True(t, has(t, store, "acme", current.GetDigest()),
		"the file the head carries must survive")

	rc, err := store.Get(ctx, "acme", pinned.GetDigest())
	require.NoError(t, err, "the pinned revision's bytes must still be readable, not merely present")
	defer func() { _ = rc.Close() }()
	body := make([]byte, 64)
	n, _ := rc.Read(body)
	require.Equal(t, "FROM alpine:3.20\n", string(body[:n]))
}

// TestADetachedDefinitionFileSurvivesWhileAnyRevisionStillCarriesIt states the
// retention rule at the level an editor sees it. Detaching a file makes a NEW
// revision without it; the revision that had it is immutable and still names
// the bytes, so the detachment alone must not reclaim anything.
func TestADetachedDefinitionFileSurvivesWhileAnyRevisionStillCarriesIt(t *testing.T) {
	ctx := context.Background()
	gc, store, _, _ := newGC(t)

	attached := uploadFile(t, gc, "acme", "Dockerfile", "FROM debian:bookworm\n")
	saveRevision(t, gc, "acme", "ci", attached)
	saveRevision(t, gc, "acme", "ci") // the detachment: same pipeline, no files

	freed, err := gc.Collect(ctx, "acme", collectingAll)
	require.NoError(t, err)
	require.Equal(t, 0, freed)
	require.True(t, has(t, store, "acme", attached.GetDigest()),
		"the earlier revision still carries these bytes, and a run may be pinned to it")
}

// TestAFreshlyUploadedDefinitionFileIsNotCollectedBeforeItsRevisionIsSaved
// closes the race the upload path opens. PutDefinitionFile is a separate call
// from the edit that declares the file, so between the two the bytes are
// carried by no revision at all — which is indistinguishable from an orphan
// unless age is part of the rule.
func TestAFreshlyUploadedDefinitionFileIsNotCollectedBeforeItsRevisionIsSaved(t *testing.T) {
	ctx := context.Background()
	gc, store, _, _ := newGC(t)

	uploading := uploadFile(t, gc, "acme", "Dockerfile", "FROM scratch\n")

	freed, err := gc.Collect(ctx, "acme", time.Hour)
	require.NoError(t, err)
	require.Equal(t, 0, freed)
	require.True(t, has(t, store, "acme", uploading.GetDigest()),
		"an upload younger than the retain window is somebody mid-edit, not an orphan")
}

// TestADefinitionFileSharedWithARetainedRunIsNotCollected is refcounting over
// CONTENT rather than ownership: identical bytes are one object, so a file no
// revision carries may still be the output a live run is entitled to read.
func TestADefinitionFileSharedWithARetainedRunIsNotCollected(t *testing.T) {
	ctx := context.Background()
	gc, store, runs, _ := newGC(t)

	shared := uploadFile(t, gc, "acme", "shared", "identical bytes")
	require.NoError(t, gc.Reference(ctx, "acme", shared.GetDigest(), "run-live"))
	finish(t, runs, "acme", "run-live", time.Minute)

	freed, err := gc.Collect(ctx, "acme", time.Hour)
	require.NoError(t, err)
	require.Equal(t, 0, freed)
	require.True(t, has(t, store, "acme", shared.GetDigest()),
		"a live run references these bytes, whatever the definition store thinks of them")
}

// TestCollectingADefinitionFileCreditsItBackToTheTenant keeps the accounting
// symmetric. These bytes were charged against max_cas_bytes on upload (ADR
// 0023 makes the quota the only ceiling on a definition's size), so reclaiming
// them without a credit would leave the tenant paying for storage that is
// empty — the exact bug the Collected hook was added for.
func TestCollectingADefinitionFileCreditsItBackToTheTenant(t *testing.T) {
	ctx := context.Background()
	gc, _, _, _ := newGC(t)

	credited := &recordingCollector{}
	gc.Collected = credited

	orphan := uploadFile(t, gc, "acme", "Dockerfile", "FROM scratch\n")

	_, err := gc.Collect(ctx, "acme", collectingAll)
	require.NoError(t, err)
	require.Equal(t, []string{orphan.GetDigest().GetHex()}, credited.hexes,
		"the collected definition file must reach the ledger that charged for it")
}

// TestASweepExaminesOnlyTheBytesItsOwnTenantUploaded holds the rule that has
// no exceptions anywhere in this system: there is no unscoped query. Two
// tenants uploading the same Dockerfile hold the same digest, and a candidate
// list read without its tenant would have one tenant's sweep judging bytes the
// other recorded — and deleting its own copy of them on that basis, since the
// store is content-addressed and the digests match.
//
// The blob acme holds here was never recorded as a definition file, which is
// the state of every plugin artifact and of every step output whose reference
// has not been written yet. Those must survive a sweep untouched.
func TestASweepExaminesOnlyTheBytesItsOwnTenantUploaded(t *testing.T) {
	ctx := context.Background()
	gc, store, _, _ := newGC(t)

	theirs := uploadFile(t, gc, "globex", "Dockerfile", "FROM scratch\n")
	unrecorded, err := store.Put(ctx, "acme", strings.NewReader("FROM scratch\n"))
	require.NoError(t, err)

	freed, err := gc.Collect(ctx, "acme", collectingAll)
	require.NoError(t, err)
	require.Equal(t, 0, freed, "acme recorded nothing, so acme's sweep has nothing to judge")
	require.True(t, has(t, store, "acme", unrecorded),
		"these bytes are not a candidate of acme's: globex recorded them, and acme never did")
	require.True(t, has(t, store, "globex", theirs.GetDigest()),
		"globex uploaded these bytes and acme's sweep must not touch them")
}

// recordingCollector stands in for the tenancy ledger: it records what it was
// told to credit and reads nothing, because a credit that read the store from
// inside the collector's transaction would deadlock SQLite's single connection.
type recordingCollector struct{ hexes []string }

func (r *recordingCollector) Collected(_ context.Context, _ string, d *dholev1.Digest) error {
	r.hexes = append(r.hexes, d.GetHex())
	return nil
}

// definitionRows counts the definition-file rows this tenant has, which is the
// only way to state what the collector leaves behind.
func definitionRows(t *testing.T, gc *cas.GC, tenantID string) int {
	t.Helper()
	var n int
	require.NoError(t, gc.DB.QueryRowContext(context.Background(),
		gc.Dialect.Rebind(`SELECT COUNT(*) FROM definition_blobs WHERE tenant_id = ?`),
		tenantID).Scan(&n))
	return n
}

// TestTheRecordOfACollectedDefinitionFileGoesWithTheBytes keeps the index from
// outliving the store it describes: a row whose blob is gone makes every later
// sweep re-examine a digest that will never come back, and the row is the only
// thing that would keep the tenant's candidate list growing after the bytes
// stopped doing so.
func TestTheRecordOfACollectedDefinitionFileGoesWithTheBytes(t *testing.T) {
	ctx := context.Background()
	gc, _, _, _ := newGC(t)

	uploadFile(t, gc, "acme", "Dockerfile", "FROM scratch\n")
	require.Equal(t, 1, definitionRows(t, gc, "acme"), "the upload is recorded")

	_, err := gc.Collect(ctx, "acme", collectingAll)
	require.NoError(t, err)
	require.Equal(t, 0, definitionRows(t, gc, "acme"),
		"the row describes bytes that are gone and must go with them")
}

// TestTheRecordOfASurvivingDefinitionFileIsKept is the other half, and the one
// that matters: dropping the row of a blob that survived would un-enumerate it
// permanently, which is the defect this whole change closes, reintroduced by a
// tidy-up.
func TestTheRecordOfASurvivingDefinitionFileIsKept(t *testing.T) {
	ctx := context.Background()
	gc, _, _, _ := newGC(t)

	carried := uploadFile(t, gc, "acme", "Dockerfile", "FROM alpine:3.21\n")
	saveRevision(t, gc, "acme", "ci", carried)

	_, err := gc.Collect(ctx, "acme", collectingAll)
	require.NoError(t, err)
	require.Equal(t, 1, definitionRows(t, gc, "acme"),
		"the bytes are still there, so the row that makes them examinable must be too")
}

// TestReuploadingADefinitionFileRestartsItsRetainWindow states why the
// recorded time is the LAST upload rather than the first. Somebody attaching a
// file that was uploaded and abandoned weeks ago is mid-edit exactly like
// somebody uploading new bytes, and a sweep between their upload and their
// save must not take the file out from under them.
func TestReuploadingADefinitionFileRestartsItsRetainWindow(t *testing.T) {
	ctx := context.Background()
	gc, store, _, _ := newGC(t)

	abandoned := uploadFile(t, gc, "acme", "Dockerfile", "FROM scratch\n")
	_, err := gc.DB.ExecContext(ctx, gc.Dialect.Rebind(
		`UPDATE definition_blobs SET uploaded_at = ? WHERE tenant_id = ? AND digest = ?`),
		time.Now().UTC().Add(-30*24*time.Hour).Format(time.RFC3339Nano),
		"acme", "sha256:"+abandoned.GetDigest().GetHex())
	require.NoError(t, err)

	// The same bytes, uploaded again by whoever is editing now.
	uploadFile(t, gc, "acme", "Dockerfile", "FROM scratch\n")

	freed, err := gc.Collect(ctx, "acme", time.Hour)
	require.NoError(t, err)
	require.Equal(t, 0, freed)
	require.True(t, has(t, store, "acme", abandoned.GetDigest()),
		"these bytes were uploaded a second ago; the edit that names them has not been saved yet")
}

// refusingDeleter is a store whose Delete fails, which is the state a
// filesystem in trouble presents: the row still describes bytes that are
// there.
type refusingDeleter struct{ cas.Store }

func (refusingDeleter) Delete(_ context.Context, _ string, _ *dholev1.Digest) error {
	return errors.New("the filesystem refused")
}

// TestADefinitionFileTheStoreRefusedToDeleteKeepsItsRecord is the failure
// direction that has no second chance. A sweep that drops the row while the
// bytes survive makes the blob invisible to every later sweep — exactly the
// defect this table was added to close, reintroduced as a tidy-up that ran
// whether or not the delete it was tidying up after had worked.
func TestADefinitionFileTheStoreRefusedToDeleteKeepsItsRecord(t *testing.T) {
	ctx := context.Background()
	gc, store, _, _ := newGC(t)

	uploadFile(t, gc, "acme", "Dockerfile", "FROM scratch\n")
	require.Equal(t, 1, definitionRows(t, gc, "acme"))

	gc.Store = refusingDeleter{Store: store}
	_, err := gc.Collect(ctx, "acme", collectingAll)
	require.Error(t, err, "a store that cannot delete must say so rather than report a clean sweep")
	require.Equal(t, 1, definitionRows(t, gc, "acme"),
		"the bytes are still there, so the record that makes them collectable next time must be too")
}
