// Package mirror is the one-way git export of the canonical definition store.
//
// The server database holds the definition; git, where a deployment has one,
// is a mirror of it and never a second source of truth (ADR 0008). The
// direction is the whole point. Somebody will clone the mirror, edit the YAML
// and push it — and when they do, the next authoritative push overwrites their
// edit and nothing about what runs has changed. A mirror that quietly became
// bidirectional is the failure this design exists to prevent: the moment an
// edit in git can change behaviour, the approval state and the run history
// stop meaning anything.
//
// The same argument makes the mirror non-load-bearing in the other direction.
// A push that cannot reach its remote is reported as drift and nothing more;
// it never fails a save. A mirror that can block a save has become part of the
// critical path, which is exactly what it must not be.
//
// Every repository is scoped to one tenant. There is no shared repository with
// per-tenant directories, because the moment two tenants share a remote, read
// access to one is read access to both.
package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/transport"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
)

// The errors this package returns.
var (
	// ErrTenantRequired is returned by every method given an empty tenant.
	// An empty tenant is a bug in the caller, never a wildcard.
	ErrTenantRequired = errors.New("tenant scope required")

	// ErrNoRemote means this tenant has no repository configured. Git is
	// optional: a deployment with no repo is a supported deployment, and a
	// caller that mirrors anyway should hear so rather than guess a URL.
	ErrNoRemote = errors.New("no git mirror configured for tenant")

	// ErrUnsafePipelineID refuses an id that would write outside the
	// directory the mirror owns. It is refused rather than sanitised:
	// sanitising invents a path nobody asked for and quietly exports the
	// definition under a name that is not its identity.
	ErrUnsafePipelineID = errors.New("pipeline id is not a safe file name")
)

// DefaultBranch is the branch a mirror writes when none is configured.
const DefaultBranch = "main"

// definitionDir is the directory in the repository the mirror owns. Files
// outside it are none of the mirror's business and are left untouched.
const definitionDir = "pipelines"

// Default retry shape. A remote that is down stays down for a while, so the
// delay grows and is capped: the point of the ceiling is that a mirror can
// never spin hot against a dead remote and turn an outage into load.
const (
	defaultAttempts  = 3
	defaultBaseDelay = 200 * time.Millisecond
	defaultMaxDelay  = 5 * time.Second
)

// State is what the mirror can say about one tenant's repository.
type State struct {
	// Drift reports the mirror not holding what the database holds: the
	// last push failed, and the remote is behind by at least that
	// revision. It is an observation about the mirror, never about the
	// definition, which was saved regardless.
	Drift bool

	// Revision is the id of the last revision successfully mirrored, empty
	// if none ever was.
	Revision string

	// Pending is the id of the revision the most recent failed push was
	// carrying, empty when there is no such failure.
	Pending string

	// Err is the text of the failure behind Drift, empty when there is
	// none. It is text rather than an error so State stays a value a
	// handler can serialise.
	Err string
}

// Config configures a Git mirror.
type Config struct {
	// Remotes maps a tenant id to the git URL that tenant's definitions
	// mirror to. One repository per tenant: sharing a remote between
	// tenants would make read access to one read access to both.
	Remotes map[string]string

	// WorkDir is where the per-tenant working clones live. It is a cache,
	// not state: deleting it costs one clone.
	WorkDir string

	// Branch is the branch written in every repository, DefaultBranch when
	// empty.
	Branch string

	// Attempts is how many times one push is tried before it is reported as
	// drift, defaultAttempts when zero.
	Attempts int

	// BaseDelay and MaxDelay shape the backoff between attempts.
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// Git mirrors definitions into one git repository per tenant.
type Git struct {
	cfg Config

	// mu guards both the per-tenant working clones and the states below: a
	// tenant's clone is a directory on disk that two concurrent pushes
	// would corrupt, and its state is read by handlers while pushes write.
	mu     sync.Mutex
	states map[string]State
}

// NewGit returns a mirror writing to the configured remotes.
func NewGit(cfg Config) (*Git, error) {
	if cfg.WorkDir == "" {
		return nil, errors.New("mirror: work directory required")
	}
	if cfg.Branch == "" {
		cfg.Branch = DefaultBranch
	}
	if cfg.Attempts <= 0 {
		cfg.Attempts = defaultAttempts
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = defaultBaseDelay
	}
	if cfg.MaxDelay < cfg.BaseDelay {
		cfg.MaxDelay = max(defaultMaxDelay, cfg.BaseDelay)
	}
	if err := os.MkdirAll(cfg.WorkDir, 0o700); err != nil {
		return nil, fmt.Errorf("mirror: work directory: %w", err)
	}
	return &Git{cfg: cfg, states: map[string]State{}}, nil
}

// Push exports one revision of a definition into the tenant's repository,
// overwriting whatever that file held. It is one way: nothing in the
// repository is ever read back into the definition store, so an edit made in
// a clone survives only until the next push and never reaches a run.
//
// A failure is returned for the caller to log, and recorded as drift. It is
// not a reason to fail whatever the caller was doing.
func (g *Git) Push(ctx context.Context, tenantID string, rev defstore.Revision, p *dholev1.Pipeline) error {
	if tenantID == "" {
		return ErrTenantRequired
	}
	if p == nil {
		return errors.New("mirror: push: nil pipeline")
	}
	path, err := definitionPath(p.GetId())
	if err != nil {
		return err
	}
	remote, ok := g.cfg.Remotes[tenantID]
	if !ok || remote == "" {
		return fmt.Errorf("%w: %s", ErrNoRemote, tenantID)
	}
	content, err := ToYAML(p)
	if err != nil {
		return err
	}

	err = g.retry(ctx, func() error {
		return g.pushOnce(tenantID, remote, path, content, rev)
	})
	g.record(tenantID, rev, err)
	if err != nil {
		return fmt.Errorf("mirror: push revision %s for tenant %s: %w", rev.ID, tenantID, err)
	}
	return nil
}

// Status reports what the mirror knows about one tenant's repository.
func (g *Git) Status(_ context.Context, tenantID string) (State, error) {
	if tenantID == "" {
		return State{}, ErrTenantRequired
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.states[tenantID], nil
}

// record updates the tenant's state after an attempted push.
func (g *Git) record(tenantID string, rev defstore.Revision, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	state := g.states[tenantID]
	if err != nil {
		state.Drift = true
		state.Pending = rev.ID
		state.Err = err.Error()
	} else {
		state.Drift = false
		state.Revision = rev.ID
		state.Pending = ""
		state.Err = ""
	}
	g.states[tenantID] = state
}

// retry runs attempt until it succeeds or the attempts run out, backing off
// between tries and stopping the moment the caller's context is done — a
// backoff nobody is waiting for is just a goroutine holding a lock.
func (g *Git) retry(ctx context.Context, attempt func() error) error {
	delay := g.cfg.BaseDelay
	var err error
	for i := range g.cfg.Attempts {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if err == nil {
				return ctxErr
			}
			return fmt.Errorf("%w (last attempt: %w)", ctxErr, err)
		}
		if err = attempt(); err == nil {
			return nil
		}
		if i == g.cfg.Attempts-1 {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w (last attempt: %w)", ctx.Err(), err)
		case <-timer.C:
		}
		if delay = 2 * delay; delay > g.cfg.MaxDelay {
			delay = g.cfg.MaxDelay
		}
	}
	return err
}

// pushOnce is one attempt: resynchronise the working clone with the remote,
// write the definition, commit and push.
//
// Resynchronising first is what makes the export authoritative without a force
// push. The working clone is hard-reset to the remote branch, so the commit is
// built on whatever anybody else pushed and lands as a fast-forward — and
// because the file is written wholesale rather than merged, a hand edit to it
// is simply gone. Everything else in the repository is left exactly as it was.
func (g *Git) pushOnce(tenantID, remote, path string, content []byte, rev defstore.Revision) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	dir := filepath.Join(g.cfg.WorkDir, tenantDir(tenantID))
	repo, err := g.open(dir, remote)
	if err != nil {
		return err
	}
	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("worktree: %w", err)
	}
	if err := g.sync(repo, wt); err != nil {
		return err
	}

	full := filepath.Join(dir, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	if err := os.WriteFile(full, content, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if _, err := wt.Add(path); err != nil {
		return fmt.Errorf("stage %s: %w", path, err)
	}

	status, err := wt.Status()
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	if !status.IsClean() {
		// An unchanged definition produces no commit, so a repeated push
		// of the same revision does not fill the history with noise.
		msg := fmt.Sprintf("mirror %s revision %s\n\nExported from the definition store; edits here are not authoritative.",
			rev.PipelineID, rev.ID)
		if _, err := wt.Commit(msg, &git.CommitOptions{
			Author: &object.Signature{Name: "dhole", Email: "dhole@localhost", When: time.Now()},
		}); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}

	branch := plumbing.NewBranchReferenceName(g.cfg.Branch)
	err = repo.Push(&git.PushOptions{
		RemoteName: git.DefaultRemoteName,
		RefSpecs:   []config.RefSpec{config.RefSpec(branch + ":" + branch)},
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("push: %w", err)
	}
	return nil
}

// open returns the tenant's working clone, creating it if it is not there.
// A clone that has gone missing or been corrupted is rebuilt rather than
// reported: it holds no state the store does not already hold.
func (g *Git) open(dir, remote string) (*git.Repository, error) {
	repo, err := git.PlainOpen(dir)
	switch {
	case err == nil:
		return repo, g.setRemote(repo, remote)
	case !errors.Is(err, git.ErrRepositoryNotExists):
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			return nil, fmt.Errorf("open working clone: %w", err)
		}
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create working clone: %w", err)
	}
	repo, err = git.PlainInit(dir, false)
	if err != nil {
		return nil, fmt.Errorf("init working clone: %w", err)
	}
	head := plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(g.cfg.Branch))
	if err := repo.Storer.SetReference(head); err != nil {
		return nil, fmt.Errorf("set head: %w", err)
	}
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: git.DefaultRemoteName,
		URLs: []string{remote},
	}); err != nil {
		return nil, fmt.Errorf("configure remote: %w", err)
	}
	return repo, nil
}

// setRemote points an existing working clone at the configured URL, so a
// reconfigured remote does not keep mirroring to the old one.
func (g *Git) setRemote(repo *git.Repository, remote string) error {
	existing, err := repo.Remote(git.DefaultRemoteName)
	if err == nil && len(existing.Config().URLs) == 1 && existing.Config().URLs[0] == remote {
		return nil
	}
	if err == nil {
		if err := repo.DeleteRemote(git.DefaultRemoteName); err != nil {
			return fmt.Errorf("configure remote: %w", err)
		}
	}
	if _, err := repo.CreateRemote(&config.RemoteConfig{
		Name: git.DefaultRemoteName,
		URLs: []string{remote},
	}); err != nil {
		return fmt.Errorf("configure remote: %w", err)
	}
	return nil
}

// sync fetches the remote and hard-resets the working clone onto it, so the
// commit about to be made lands on top of whatever else was pushed.
func (g *Git) sync(repo *git.Repository, wt *git.Worktree) error {
	branch := plumbing.NewBranchReferenceName(g.cfg.Branch)
	spec := config.RefSpec("+" + branch + ":" + plumbing.NewRemoteReferenceName(git.DefaultRemoteName, g.cfg.Branch))
	err := repo.Fetch(&git.FetchOptions{
		RemoteName: git.DefaultRemoteName,
		RefSpecs:   []config.RefSpec{spec},
		Force:      true,
	})
	// A remote with nothing on this branch yet is the normal state of a
	// repository the mirror has never written to, not a failure; anything
	// else is a real transport problem and is worth another attempt.
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) && !isEmptyRemoteBranch(err) {
		return fmt.Errorf("fetch: %w", err)
	}

	remoteRef, err := repo.Reference(plumbing.NewRemoteReferenceName(git.DefaultRemoteName, g.cfg.Branch), true)
	if err != nil {
		// Nothing on the remote branch yet: the first push creates it.
		return nil
	}
	local := plumbing.NewHashReference(branch, remoteRef.Hash())
	if err := repo.Storer.SetReference(local); err != nil {
		return fmt.Errorf("update branch: %w", err)
	}
	if err := wt.Reset(&git.ResetOptions{Commit: remoteRef.Hash(), Mode: git.HardReset}); err != nil {
		return fmt.Errorf("reset to remote: %w", err)
	}
	return nil
}

// isEmptyRemoteBranch reports a fetch that failed only because the remote has
// nothing on this branch yet, which is the normal state of a repository the
// mirror has never written to.
func isEmptyRemoteBranch(err error) bool {
	var noMatch git.NoMatchingRefSpecError
	return errors.As(err, &noMatch) ||
		errors.Is(err, transport.ErrEmptyRemoteRepository) ||
		errors.Is(err, plumbing.ErrReferenceNotFound)
}

// definitionPath is where a pipeline's definition lives in the repository.
//
// The id is caller data, so it is checked rather than cleaned: anything that
// could name a file outside the directory the mirror owns — a separator, a
// parent reference, an absolute path — is refused. Sanitising here would
// silently export the definition under a name that is not its identity, which
// is worse than refusing to export it at all.
func definitionPath(id string) (string, error) {
	switch {
	case id == "":
		return "", fmt.Errorf("%w: empty", ErrUnsafePipelineID)
	case id == "." || id == "..":
		return "", fmt.Errorf("%w: %q", ErrUnsafePipelineID, id)
	case strings.ContainsAny(id, `/\`), strings.ContainsRune(id, 0):
		return "", fmt.Errorf("%w: %q", ErrUnsafePipelineID, id)
	case filepath.IsAbs(id), filepath.VolumeName(id) != "":
		return "", fmt.Errorf("%w: %q", ErrUnsafePipelineID, id)
	}
	return definitionDir + "/" + id + ".yaml", nil
}

// tenantDir is the working-clone directory for one tenant, named so that a
// tenant id containing path characters cannot reach another tenant's clone.
func tenantDir(tenantID string) string {
	sum := sha256.Sum256([]byte(tenantID))
	return hex.EncodeToString(sum[:])
}
