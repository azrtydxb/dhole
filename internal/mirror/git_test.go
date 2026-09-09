package mirror_test

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	_ "modernc.org/sqlite"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/mirror"
)

// migrationPath is the definition schema the store under test needs. The test
// applies the real migration rather than a hand-written copy.
const migrationPath = "../runstore/migrations/0005_definitions.sql"

const (
	tenant      = "acme"
	otherTenant = "globex"
)

// newStore opens a throwaway SQLite definition store.
func newStore(t *testing.T) defstore.Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "definitions.db")
	dsn := "file:" + url.PathEscape(path) + "?_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	schema, err := os.ReadFile(migrationPath)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), string(schema))
	require.NoError(t, err)

	return defstore.New(db)
}

// newBareRepo initialises an empty bare repository to mirror into, with HEAD
// on the branch the mirror writes, and returns its URL.
func newBareRepo(t *testing.T) string {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "remote.git")
	repo, err := git.PlainInit(dir, true)
	require.NoError(t, err)
	head := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(mirror.DefaultBranch))
	require.NoError(t, repo.Storer.SetReference(head))
	return dir
}

// newMirror returns a Git mirror writing tenant repositories to remotes, with
// the retry wound tight so a failing test fails fast.
func newMirror(t *testing.T, remotes map[string]string) *mirror.Git {
	t.Helper()

	m, err := mirror.NewGit(mirror.Config{
		Remotes:   remotes,
		WorkDir:   t.TempDir(),
		Attempts:  2,
		BaseDelay: time.Millisecond,
		MaxDelay:  2 * time.Millisecond,
	})
	require.NoError(t, err)
	return m
}

// readMirrored clones remote fresh and returns the file at path, so an
// assertion reads what the remote actually holds rather than a local cache.
func readMirrored(t *testing.T, remote, path string) (string, bool) {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "readback")
	_, err := git.PlainClone(dir, false, &git.CloneOptions{URL: remote})
	if errors.Is(err, transport.ErrEmptyRemoteRepository) {
		// Never mirrored to at all, which is the strongest form of "this
		// file is not there".
		return "", false
	}
	require.NoError(t, err)

	b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(path)))
	if os.IsNotExist(err) {
		return "", false
	}
	require.NoError(t, err)
	return string(b), true
}

// A mirror is an export, not a second source of truth (ADR 0008). Somebody
// will edit the mirrored YAML in a clone and push it; when they do, the next
// authoritative push must overwrite that edit, and nothing they did may have
// changed what the database says is active. The moment an edit in git can
// change behaviour, approval state and run history stop meaning anything.
func TestGitMirrorEditsAreNotAuthoritative(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	remote := newBareRepo(t)
	m := newMirror(t, map[string]string{tenant: remote})

	first := smallPipeline("p1", "oci://dhole/build:1")
	rev1, err := store.Save(ctx, tenant, first, "ada")
	require.NoError(t, err)
	require.NoError(t, store.Approve(ctx, tenant, rev1.ID, "grace"))
	require.NoError(t, m.Push(ctx, tenant, rev1, first))

	// Somebody clones the mirror, edits the exported definition by hand and
	// adds an unrelated file, then pushes.
	clone := filepath.Join(t.TempDir(), "clone")
	repo, err := git.PlainClone(clone, false, &git.CloneOptions{URL: remote})
	require.NoError(t, err)
	wt, err := repo.Worktree()
	require.NoError(t, err)

	defPath := filepath.Join(clone, "pipelines", "p1.yaml")
	edited, err := os.ReadFile(defPath)
	require.NoError(t, err)
	require.Contains(t, string(edited), "oci://dhole/build:1")
	tampered := strings.ReplaceAll(string(edited), "oci://dhole/build:1", "oci://evil/backdoor:1")
	require.NoError(t, os.WriteFile(defPath, []byte(tampered), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(clone, "NOTES.md"), []byte("hand written\n"), 0o600))
	_, err = wt.Add(".")
	require.NoError(t, err)
	_, err = wt.Commit("hand edit in the clone", &git.CommitOptions{
		Author: &object.Signature{Name: "mallory", Email: "mallory@example.com", When: time.Now()},
	})
	require.NoError(t, err)
	require.NoError(t, repo.Push(&git.PushOptions{RemoteName: "origin"}))

	// The next authoritative push must overwrite the hand edit.
	second := smallPipeline("p1", "oci://dhole/build:2")
	rev2, err := store.Save(ctx, tenant, second, "ada")
	require.NoError(t, err)
	require.NoError(t, m.Push(ctx, tenant, rev2, second))

	got, ok := readMirrored(t, remote, "pipelines/p1.yaml")
	require.True(t, ok, "the mirrored definition must exist")
	require.NotContains(t, got, "oci://evil/backdoor:1",
		"the hand edit must be overwritten, not merged")
	mirrored, err := mirror.FromYAML([]byte(got))
	require.NoError(t, err)
	require.True(t, proto.Equal(second, mirrored),
		"the mirror must hold exactly the authoritative definition")

	// Nothing the hand edit did may have touched what runs. rev2 is a draft:
	// the active revision is still the one that was approved in the database.
	active, err := store.Active(ctx, tenant, "p1")
	require.NoError(t, err)
	require.Equal(t, rev1.ID, active.ID)
	stored, err := store.Get(ctx, tenant, "p1", active.ID)
	require.NoError(t, err)
	require.True(t, proto.Equal(first, stored),
		"a git edit must never alter the definition a run reads")

	// The mirror overwrites the files it exports and leaves the rest of the
	// repository alone; it is not a force-wipe.
	_, ok = readMirrored(t, remote, "NOTES.md")
	require.True(t, ok, "unrelated files in the mirror must survive a push")
}

// Export and import between backends must stay lossless (ADR 0008), so the
// round trip has to survive every field of the schema — not the handful a
// hand-written fixture happens to set.
func TestYAMLRoundTripsLosslessly(t *testing.T) {
	p := fullPipeline()
	requireEveryFieldSet(t, p)
	requireEveryEnumValueUsed(t, p)

	encoded, err := mirror.ToYAML(p)
	require.NoError(t, err)

	back, err := mirror.FromYAML(encoded)
	require.NoError(t, err)
	require.True(t, proto.Equal(p, back), "round trip lost data:\n%s", encoded)
}

// What is exported is the definition, not the control plane's bookkeeping.
// Layout, approval state and approver live outside the document so that
// import and export between backends cannot carry one backend's private data
// into another.
func TestYAMLContainsNoBackendSpecificFields(t *testing.T) {
	encoded, err := mirror.ToYAML(fullPipeline())
	require.NoError(t, err)

	for _, key := range []string{"layout", "approver", "state"} {
		require.NotContains(t, string(encoded), key+":",
			"%q is control-plane bookkeeping and must not be exported", key)
	}
}

// The mirror must never be load-bearing. A dead remote is a mirror problem,
// reported as drift; it is not a reason a definition cannot be saved.
func TestMirrorPushFailureDoesNotBlockSave(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	unreachable := filepath.Join(t.TempDir(), "does-not-exist.git")
	m := newMirror(t, map[string]string{tenant: unreachable})

	p := smallPipeline("p1", "oci://dhole/build:1")
	rev, err := store.Save(ctx, tenant, p, "ada")
	require.NoError(t, err, "a dead mirror must not fail a save")
	require.NotEmpty(t, rev.ID)

	require.Error(t, m.Push(ctx, tenant, rev, p))

	state, err := m.Status(ctx, tenant)
	require.NoError(t, err)
	require.True(t, state.Drift, "an unreachable remote must be reported as drift")
	require.Equal(t, rev.ID, state.Pending)
	require.NotEmpty(t, state.Err)

	// The definition is still there to be read, and still readable exactly.
	stored, err := store.Get(ctx, tenant, "p1", rev.ID)
	require.NoError(t, err)
	require.True(t, proto.Equal(p, stored))
}

// Every record is tenant-scoped, and a mirror is a record. One tenant's
// definitions appearing in another's repository would be a cross-tenant leak
// through the export path.
func TestMirrorIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	ours, theirs := newBareRepo(t), newBareRepo(t)
	m := newMirror(t, map[string]string{tenant: ours, otherTenant: theirs})

	p := smallPipeline("secret", "oci://dhole/build:1")
	rev := defstore.Revision{ID: "rev_1", PipelineID: p.GetId(), ContentHash: "1"}
	require.NoError(t, m.Push(ctx, tenant, rev, p))

	_, ok := readMirrored(t, ours, "pipelines/secret.yaml")
	require.True(t, ok)
	_, ok = readMirrored(t, theirs, "pipelines/secret.yaml")
	require.False(t, ok, "a tenant's definition must never reach another tenant's mirror")

	// An empty tenant is a bug in the caller, never a wildcard.
	err := m.Push(ctx, "", rev, p)
	require.ErrorContains(t, err, "tenant scope required")
	_, err = m.Status(ctx, "")
	require.ErrorContains(t, err, "tenant scope required")
}

// A pipeline id is caller data. One that would escape the repository is
// rejected rather than sanitised: sanitising invents a path nobody asked for
// and quietly writes the wrong file.
func TestMirrorRejectsPipelineIDThatEscapesTheRepository(t *testing.T) {
	ctx := context.Background()
	remote := newBareRepo(t)
	m := newMirror(t, map[string]string{tenant: remote})

	for _, id := range []string{"..", "../escape", "a/b", `a\b`, "/etc/passwd", ".", ""} {
		p := smallPipeline(id, "oci://dhole/build:1")
		rev := defstore.Revision{ID: "rev_1", PipelineID: id, ContentHash: "1"}
		require.Error(t, m.Push(ctx, tenant, rev, p), "pipeline id %q must be rejected", id)
	}
}

// The retry must stop when the caller gives up, rather than working its way
// through a backoff schedule nobody is waiting for.
func TestMirrorRetryStopsOnContextCancellation(t *testing.T) {
	unreachable := filepath.Join(t.TempDir(), "does-not-exist.git")
	m, err := mirror.NewGit(mirror.Config{
		Remotes:   map[string]string{tenant: unreachable},
		WorkDir:   t.TempDir(),
		Attempts:  100,
		BaseDelay: time.Hour,
		MaxDelay:  time.Hour,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p := smallPipeline("p1", "oci://dhole/build:1")
	rev := defstore.Revision{ID: "rev_1", PipelineID: p.GetId(), ContentHash: "1"}

	done := make(chan error, 1)
	go func() { done <- m.Push(ctx, tenant, rev, p) }()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Push ignored context cancellation and slept through its backoff")
	}
}

// smallPipeline is the definition most git assertions push around.
func smallPipeline(id, pluginRef string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenant},
		Steps: []*dholev1.Step{{
			Id:          "build",
			Name:        "Build",
			PluginRef:   pluginRef,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			Outputs: []*dholev1.Port{{
				Name: "artifact",
				Type: &dholev1.PortType{
					Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{MediaType: "application/gzip"}},
				},
			}},
		}},
	}
}

// fullPipeline exercises every field of the schema and every value of every
// enum it reaches. requireEveryFieldSet and requireEveryEnumValueUsed hold it
// to that against the descriptor, so a field added to the proto fails this
// test until the fixture covers it — a round-trip test that silently skips a
// field certifies a losslessness it never checked.
func fullPipeline() *dholev1.Pipeline {
	blob := func(mediaType string) *dholev1.PortType {
		return &dholev1.PortType{
			Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{MediaType: mediaType}},
		}
	}
	structured := func(id, schema string) *dholev1.PortType {
		return &dholev1.PortType{
			Kind: &dholev1.PortType_Structured{Structured: &dholev1.StructType{
				SchemaId: id,
				Schema:   schema,
			}},
		}
	}
	return &dholev1.Pipeline{
		Id:     "every-field",
		Tenant: &dholev1.Tenant{Id: tenant},
		Steps: []*dholev1.Step{
			{
				Id:           "build",
				Name:         "Build",
				PluginRef:    "oci://dhole/build:1",
				EffectClass:  dholev1.EffectClass_EFFECT_CLASS_PURE,
				LeaseScope:   dholev1.LeaseScope_LEASE_SCOPE_STEP,
				Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
				Inputs: []*dholev1.Port{
					{Name: "source", Type: blob("application/gzip")},
					{Name: "config", Type: structured("https://dhole.dev/build.json", `{"type":"object"}`)},
				},
				Outputs: []*dholev1.Port{{Name: "artifact", Type: blob("application/octet-stream")}},
			},
			{
				Id:           "publish",
				Name:         "Publish",
				PluginRef:    "oci://dhole/publish:1",
				EffectClass:  dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
				LeaseScope:   dholev1.LeaseScope_LEASE_SCOPE_JOB,
				Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS},
				Inputs:       []*dholev1.Port{{Name: "artifact", Type: blob("application/octet-stream")}},
				Outputs: []*dholev1.Port{
					{Name: "receipt", Type: structured("https://dhole.dev/receipt.json", `{"type":"object"}`)},
				},
			},
			{
				Id:           "announce",
				Name:         "Announce",
				PluginRef:    "oci://dhole/announce:1",
				EffectClass:  dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
				LeaseScope:   dholev1.LeaseScope_LEASE_SCOPE_PIPELINE,
				Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
				Inputs: []*dholev1.Port{
					{Name: "receipt", Type: structured("https://dhole.dev/receipt.json", `{"type":"object"}`)},
				},
				Outputs: []*dholev1.Port{{Name: "log", Type: blob("text/plain")}},
			},
			{
				Id:           "pooled",
				Name:         "Pooled",
				PluginRef:    "oci://dhole/pooled:1",
				EffectClass:  dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED,
				LeaseScope:   dholev1.LeaseScope_LEASE_SCOPE_POOL,
				Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_HOST_MOUNT},
				Inputs:       []*dholev1.Port{{Name: "log", Type: blob("text/plain")}},
				Outputs:      []*dholev1.Port{{Name: "report", Type: blob("text/plain")}},
			},
			{
				Id:           "service",
				Name:         "Service",
				PluginRef:    "oci://dhole/service:1",
				EffectClass:  dholev1.EffectClass_EFFECT_CLASS_PURE,
				LeaseScope:   dholev1.LeaseScope_LEASE_SCOPE_SERVICE,
				Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_UNSPECIFIED},
				Inputs:       []*dholev1.Port{{Name: "report", Type: blob("text/plain")}},
				Outputs:      []*dholev1.Port{{Name: "status", Type: blob("text/plain")}},
			},
			{
				Id:           "unscoped",
				Name:         "Unscoped",
				PluginRef:    "oci://dhole/unscoped:1",
				EffectClass:  dholev1.EffectClass_EFFECT_CLASS_PURE,
				LeaseScope:   dholev1.LeaseScope_LEASE_SCOPE_UNSPECIFIED,
				Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
				Inputs:       []*dholev1.Port{{Name: "status", Type: blob("text/plain")}},
				Outputs:      []*dholev1.Port{{Name: "done", Type: blob("text/plain")}},
			},
		},
		Edges: []*dholev1.Edge{
			{FromStep: "build", FromPort: "artifact", ToStep: "publish", ToPort: "artifact"},
			{FromStep: "publish", FromPort: "receipt", ToStep: "announce", ToPort: "receipt"},
			{FromStep: "announce", FromPort: "log", ToStep: "pooled", ToPort: "log"},
			{FromStep: "pooled", FromPort: "report", ToStep: "service", ToPort: "report"},
			{FromStep: "service", FromPort: "status", ToStep: "unscoped", ToPort: "status"},
		},
	}
}

// requireEveryFieldSet walks the Pipeline descriptor and fails for any field
// path the fixture leaves unset, including every arm of every oneof.
func requireEveryFieldSet(t *testing.T, p *dholev1.Pipeline) {
	t.Helper()

	want := declaredPaths(p.ProtoReflect().Descriptor(), "", map[protoreflect.FullName]bool{})
	got := map[string]bool{}
	populatedPaths(p.ProtoReflect(), "", got)

	var missing []string
	for _, path := range want {
		if !got[path] {
			missing = append(missing, path)
		}
	}
	require.Empty(t, missing,
		"the round-trip fixture leaves these fields unset, so it cannot prove they survive")
}

// declaredPaths is every field path reachable from md, recursion stopped at a
// type already on the path so a recursive schema terminates.
func declaredPaths(md protoreflect.MessageDescriptor, prefix string, onPath map[protoreflect.FullName]bool) []string {
	if onPath[md.FullName()] {
		return nil
	}
	onPath[md.FullName()] = true
	defer delete(onPath, md.FullName())

	var paths []string
	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		path := prefix + string(fd.Name())
		paths = append(paths, path)
		if msg := messageOf(fd); msg != nil {
			paths = append(paths, declaredPaths(msg, path+".", onPath)...)
		}
	}
	return paths
}

// populatedPaths records every field path actually set in m.
func populatedPaths(m protoreflect.Message, prefix string, out map[string]bool) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		path := prefix + string(fd.Name())
		out[path] = true
		switch {
		case fd.IsMap():
			if fd.MapValue().Message() != nil {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					populatedPaths(mv.Message(), path+".", out)
					return true
				})
			}
		case fd.IsList():
			if fd.Message() != nil {
				list := v.List()
				for i := range list.Len() {
					populatedPaths(list.Get(i).Message(), path+".", out)
				}
			}
		case fd.Message() != nil:
			populatedPaths(v.Message(), path+".", out)
		}
		return true
	})
}

// requireEveryEnumValueUsed fails for any value of any enum reachable from the
// schema that the fixture never sets, so a new enum arm cannot slip through
// the round trip unproven.
func requireEveryEnumValueUsed(t *testing.T, p *dholev1.Pipeline) {
	t.Helper()

	want := map[string]bool{}
	declaredEnumValues(p.ProtoReflect().Descriptor(), map[protoreflect.FullName]bool{}, want)
	got := map[string]bool{}
	usedEnumValues(p.ProtoReflect(), got)

	var missing []string
	for value := range want {
		if !got[value] {
			missing = append(missing, value)
		}
	}
	require.Empty(t, missing, "the round-trip fixture never uses these enum values")
}

// declaredEnumValues collects every enum value reachable from md.
func declaredEnumValues(md protoreflect.MessageDescriptor, onPath map[protoreflect.FullName]bool, out map[string]bool) {
	if onPath[md.FullName()] {
		return
	}
	onPath[md.FullName()] = true
	defer delete(onPath, md.FullName())

	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		if ed := fd.Enum(); ed != nil {
			values := ed.Values()
			for j := range values.Len() {
				out[string(ed.FullName())+"."+string(values.Get(j).Name())] = true
			}
		}
		if msg := messageOf(fd); msg != nil {
			declaredEnumValues(msg, onPath, out)
		}
	}
}

// usedEnumValues collects every enum value m actually carries.
func usedEnumValues(m protoreflect.Message, out map[string]bool) {
	md := m.Descriptor()
	fields := md.Fields()
	for i := range fields.Len() {
		fd := fields.Get(i)
		switch {
		case fd.Enum() != nil && fd.IsList():
			list := m.Get(fd).List()
			for j := range list.Len() {
				recordEnum(fd, list.Get(j).Enum(), out)
			}
		case fd.Enum() != nil:
			recordEnum(fd, m.Get(fd).Enum(), out)
		case messageOf(fd) != nil && fd.IsList():
			list := m.Get(fd).List()
			for j := range list.Len() {
				usedEnumValues(list.Get(j).Message(), out)
			}
		case messageOf(fd) != nil && fd.IsMap():
			m.Get(fd).Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
				usedEnumValues(mv.Message(), out)
				return true
			})
		case fd.Message() != nil:
			if m.Has(fd) {
				usedEnumValues(m.Get(fd).Message(), out)
			}
		}
	}
}

// recordEnum notes one enum value by its full name.
func recordEnum(fd protoreflect.FieldDescriptor, n protoreflect.EnumNumber, out map[string]bool) {
	ed := fd.Enum()
	if v := ed.Values().ByNumber(n); v != nil {
		out[string(ed.FullName())+"."+string(v.Name())] = true
	}
}

// messageOf is the message type a field carries, for singular, repeated and
// map-valued fields alike, and nil for a scalar.
func messageOf(fd protoreflect.FieldDescriptor) protoreflect.MessageDescriptor {
	if fd.IsMap() {
		return fd.MapValue().Message()
	}
	return fd.Message()
}
