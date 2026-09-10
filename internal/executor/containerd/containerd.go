// Package containerd runs steps as OCI containers on a containerd daemon.
//
// It talks to containerd directly, with no Docker daemon anywhere in the
// picture: ADR 0006 ships containerd/OCI precisely so that the container
// backend is a client of the runtime rather than of another daemon that owns
// it. A sandbox is one container holding an idle init process, and commands
// are exec processes inside it. That shape is what makes the lease scopes mean
// something here — a step-scoped sandbox is a container deleted when the step
// ends, a pipeline-scoped one is the same container entered again for every
// step, carrying state no cache key can describe, which is why
// internal/cache refuses to cache such steps.
//
// Nothing here asks the executor interface to grow a container shape. The
// container is an implementation detail behind Acquire, and everything this
// backend cannot honestly promise — privilege, a real network, secret
// delivery — is a capability it declines to advertise rather than a special
// case in the core.
//
// Two things the container gets by default, and both are deliberate: an
// unprivileged uid, because a step that gets root in its sandbox gets root
// over every host resource that sandbox is given; and a lazily mounted image
// where the daemon can do it, decided by SelectPullMode in lazypull.go.
package containerd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	client "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/remotes/docker"
	"github.com/containerd/containerd/v2/defaults"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	"github.com/distribution/reference"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	specs "github.com/opencontainers/runtime-spec/specs-go"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
)

// Kind is this backend's stable identifier in configuration.
const Kind = "containerd"

const (
	// defaultImage is the sandbox image when nothing names one. It is written
	// as a tag and never used as one: Acquire pulls the digest it resolves to.
	defaultImage = "docker.io/library/busybox:1.36"
	// defaultNamespace is the containerd namespace sandboxes live in. It is
	// deliberately not "default" or "k8s.io": those hold somebody else's
	// containers, and Release deletes what it finds by name.
	defaultNamespace = "dhole"
	// sandboxRoot is the sandbox filesystem root inside the container. Put and
	// Get resolve names under it and never escape it.
	sandboxRoot = "/dhole"
	// sandboxUID and sandboxGID are the unprivileged user steps run as. 65534
	// is nobody/nogroup on every Linux image in practice, and the id is what
	// matters here rather than the name: the kernel checks the number.
	sandboxUID uint32 = 65534
	sandboxGID uint32 = 65534
	// runningTimeout bounds waiting for a started task to report Running.
	runningTimeout = 30 * time.Second
	// killTreeTimeout bounds the sweep that terminates a cancelled step's
	// processes. It is short: it runs on the cancellation path, where the
	// caller is already gone.
	killTreeTimeout = 30 * time.Second
	// releaseTimeout bounds tearing one sandbox down.
	releaseTimeout = 60 * time.Second
	// processTimeout bounds killing and deleting one exec process.
	processTimeout = 15 * time.Second
	// ioDrainGrace is how long output is allowed to finish arriving after the
	// process exited. It is bounded because a grandchild that outlived the
	// step still holds the stream open, and an unbounded wait there would hang
	// every cancelled step instead of returning within the contract's window.
	ioDrainGrace = 500 * time.Millisecond
)

// Config is how a containerd executor is pointed at a daemon.
type Config struct {
	// Address is the containerd socket. Empty means containerd's own default
	// (/run/containerd/containerd.sock); a k3s node's is at
	// /run/k3s/containerd/containerd.sock.
	Address string
	// Namespace is the containerd namespace sandboxes are created in. Empty
	// means "dhole".
	Namespace string
	// Image is the sandbox image. Empty means defaultImage. A Spec may name
	// its own, which then applies to that sandbox only.
	Image string
	// Snapshotter overrides the snapshotter to pull and run with. Empty lets
	// SelectPullMode choose, which prefers lazy pull where the daemon has it.
	Snapshotter string
	// Privileged makes sandboxes run privileged and as root, and makes this
	// executor advertise CAPABILITY_PRIVILEGED. It is off by default, and a
	// spec requesting privilege against an executor without it is refused
	// rather than quietly run unprivileged.
	Privileged bool
	// HostNetwork puts sandboxes in the host's network namespace, and makes
	// this executor advertise CAPABILITY_NETWORK. Without it a container gets
	// its own empty namespace — loopback and nothing else — so advertising
	// network there would be a promise the scheduler would rely on.
	HostNetwork bool
	// InsecureRegistries are registry hosts ("host" or "host:port") reachable
	// over plain HTTP. Anything not named here is contacted over HTTPS.
	InsecureRegistries []string
	// Logger receives the pull-mode decisions. Nil means slog.Default.
	Logger *slog.Logger
}

// Executor hands out sandboxes that are containerd containers.
type Executor struct {
	cfg      Config
	client   *client.Client
	ns       string
	log      *slog.Logger
	insecure map[string]struct{}

	// mu guards the pull mode, which is decided on the first Acquire and can
	// be demoted later by a daemon that turns out not to be able to use the
	// snapshotter it advertises.
	mu       sync.Mutex
	pullMode PullMode

	identityOnce sync.Once
	identity     string
	identityErr  error
}

// New returns a containerd executor talking to the daemon cfg names.
//
// It does not dial: containerd's client connects lazily, and that is worth
// keeping — a control plane that cannot construct its executor at startup
// because a node's daemon is briefly down is a worse failure than a step that
// fails when it runs.
func New(cfg Config) (executor.Executor, error) {
	address := cfg.Address
	if address == "" {
		address = defaults.DefaultAddress
	}
	ns := cfg.Namespace
	if ns == "" {
		ns = defaultNamespace
	}
	c, err := client.New(address, client.WithDefaultNamespace(ns))
	if err != nil {
		return nil, fmt.Errorf("containerd executor: client for %q: %w", address, err)
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	insecure := make(map[string]struct{}, len(cfg.InsecureRegistries))
	for _, h := range cfg.InsecureRegistries {
		if h != "" {
			insecure[h] = struct{}{}
		}
	}
	return &Executor{cfg: cfg, client: c, ns: ns, log: log, insecure: insecure}, nil
}

// Kind implements executor.Executor.
func (*Executor) Kind() string { return Kind }

// Capabilities implements executor.Executor. Only what the configuration
// genuinely provides is advertised: a container with its own network namespace
// has loopback and nothing else, and an unprivileged container is not
// privileged however much a step would like it to be. Secret delivery is not
// implemented here at all.
func (e *Executor) Capabilities() []dholev1.Capability {
	var caps []dholev1.Capability
	if e.cfg.HostNetwork {
		caps = append(caps, dholev1.Capability_CAPABILITY_NETWORK)
	}
	if e.cfg.Privileged {
		caps = append(caps, dholev1.Capability_CAPABILITY_PRIVILEGED)
	}
	return caps
}

// requireCapabilities refuses a spec asking for something this backend does not
// advertise. Running it anyway would be the worse failure: the step would get
// weaker isolation than it asked for and never learn, which is how a build that
// needs privilege silently produces a wrong artifact instead of an error.
func (e *Executor) requireCapabilities(want []dholev1.Capability) error {
	have := e.Capabilities()
	for _, w := range want {
		if w == dholev1.Capability_CAPABILITY_UNSPECIFIED {
			continue
		}
		found := false
		for _, h := range have {
			if h == w {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("containerd executor: %s: capability not advertised by this backend (it advertises %v)",
				w, have)
		}
	}
	return nil
}

// EnvironmentIdentity implements executor.Executor. It is the digest the
// sandbox image resolves to, never its tag: the same tag serves different
// content over time, so a cache keyed on one returns yesterday's answer for
// today's image (ADR 0021). An image that cannot be resolved yields
// ErrNoStableIdentity, and its steps stay uncached.
//
// The identity describes the executor's configured image. A Spec naming its
// own image gets that image for its container; a caller mixing images on one
// executor needs a per-sandbox identity the interface does not yet carry.
func (e *Executor) EnvironmentIdentity() (string, error) {
	e.identityOnce.Do(func() {
		e.identity, e.identityErr = resolveDigest(e.image(""), e.insecure)
	})
	return e.identity, e.identityErr
}

// image is the image a sandbox runs: the spec's if it named one, else the
// configured one, else the default.
func (e *Executor) image(specImage string) string {
	switch {
	case specImage != "":
		return specImage
	case e.cfg.Image != "":
		return e.cfg.Image
	default:
		return defaultImage
	}
}

// scoped puts the executor's containerd namespace on a context. Every store in
// containerd is namespaced, and a call without one addresses nothing.
func (e *Executor) scoped(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, e.ns)
}

// snapshotter is the snapshotter to pull and run with. The decision needs a
// daemon to ask, so it happens on the first Acquire rather than in New — and a
// daemon that cannot be asked falls back to a full pull with a logged warning
// rather than to a snapshotter it may not have.
func (e *Executor) snapshotter(ctx context.Context) string {
	if e.cfg.Snapshotter != "" {
		return e.cfg.Snapshotter
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pullMode.Snapshotter == "" {
		e.pullMode = SelectPullMode(ctx, e.listSnapshotters, e.log)
	}
	return e.pullMode.Snapshotter
}

// demoteSnapshotter gives up on the lazy snapshotter for the rest of this
// executor's life and reports whether the caller should try again with the
// ordinary one.
//
// It exists because "the daemon lists it" turned out not to mean "the daemon
// can use it". A k3s node reports a stargz snapshotter with no init error, and
// creating a container against it fails with `open
// .../io.containerd.snapshotter.v1.stargz/snapshotter/snapshots/2/fs: no such
// file or directory`. Trusting the plugin list there makes every step on that
// node fail for a reason that has nothing to do with the step. The lazy pull
// is an optimisation (ADR 0006), so the honest response is the same one
// SelectPullMode already makes for a daemon it cannot ask: fall back to a full
// pull, and say so loudly enough that an operator can fix the snapshotter.
//
// A snapshotter the operator named explicitly is never demoted: that is a
// configuration decision, and silently running somewhere else would hide it.
func (e *Executor) demoteSnapshotter(cause error) bool {
	if e.cfg.Snapshotter != "" {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.pullMode.Lazy {
		return false
	}
	e.log.Warn("lazy image pull unavailable, falling back to a full image pull",
		"want_snapshotter", e.pullMode.Snapshotter,
		"reason", "containerd advertises the snapshotter but could not create a container with it",
		"error", cause.Error())
	e.pullMode = PullMode{Snapshotter: DefaultSnapshotter, Lazy: false}
	return true
}

// listSnapshotters reports the snapshotter plugins the daemon has loaded. A
// plugin that failed to initialise is not reported: containerd lists it, and
// pulling with it fails.
func (e *Executor) listSnapshotters(ctx context.Context) ([]string, error) {
	resp, err := e.client.IntrospectionService().Plugins(e.scoped(ctx), "type==io.containerd.snapshotter.v1")
	if err != nil {
		return nil, fmt.Errorf("list containerd snapshotters: %w", err)
	}
	names := make([]string, 0, len(resp.GetPlugins()))
	for _, p := range resp.GetPlugins() {
		if p.GetInitErr() == nil {
			names = append(names, p.GetID())
		}
	}
	return names, nil
}

// resolver is containerd's registry client, configured to speak plain HTTP to
// the hosts the operator named — plus localhost, which is containerd's own
// default and where a test or airgapped registry usually sits — and HTTPS to
// everything else.
//
// The authorizer is passed explicitly, and that is not decoration: containerd
// builds one only when it is building the whole host configuration itself, so
// supplying Hosts silently drops it and every pull from Docker Hub comes back
// 401 Unauthorized with nothing saying why. That is what happened the first
// time this ran against a real daemon.
func (e *Executor) resolver() docker.ResolverOptions {
	return docker.ResolverOptions{
		Hosts: docker.ConfigureDefaultRegistries(
			docker.WithAuthorizer(docker.NewDockerAuthorizer()),
			docker.WithPlainHTTP(func(host string) (bool, error) {
				if _, ok := e.insecure[host]; ok {
					return true, nil
				}
				return docker.MatchLocalhost(host)
			}),
		),
	}
}

// prepare resolves the image to a digest and makes sure containerd has it
// unpacked for the snapshotter sandboxes will use.
//
// The pull is BY DIGEST, always. Pulling a tag would leave the container
// running whatever that tag points at by the time the pull happened, which is
// not necessarily what EnvironmentIdentity reported a moment earlier — and a
// cache key that describes a different image than the one that ran is worse
// than no cache at all.
func (e *Executor) prepare(ctx context.Context, specImage string) (client.Image, string, error) {
	ref := e.image(specImage)
	identity, err := resolveDigest(ref, e.insecure)
	if err != nil {
		return nil, "", err
	}
	pull, err := pullRef(ref, identity)
	if err != nil {
		return nil, "", err
	}
	snapshotter := e.snapshotter(ctx)

	img, err := e.client.GetImage(ctx, pull)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return nil, "", fmt.Errorf("containerd executor: look up image %s: %w", pull, err)
		}
		img, err = e.client.Pull(ctx, pull,
			client.WithPullUnpack,
			client.WithPullSnapshotter(snapshotter),
			client.WithResolver(docker.NewResolver(e.resolver())),
		)
		if err != nil {
			return nil, "", fmt.Errorf("containerd executor: pull %s: %w", pull, err)
		}
	}
	// An image already in the store may have been pulled for a different
	// snapshotter — by another tool on the same node, or by this executor
	// before the stargz plugin appeared — and creating a container from a
	// snapshot that was never written fails at run time, far from the cause.
	unpacked, err := img.IsUnpacked(ctx, snapshotter)
	if err != nil {
		return nil, "", fmt.Errorf("containerd executor: check %s is unpacked for %s: %w", pull, snapshotter, err)
	}
	if !unpacked {
		if err := img.Unpack(ctx, snapshotter); err != nil {
			return nil, "", fmt.Errorf("containerd executor: unpack %s for %s: %w", pull, snapshotter, err)
		}
	}
	return img, snapshotter, nil
}

// pullRef is the caller's image pinned to the digest it resolved to. The
// digest is taken from identity rather than from the reference, and the
// repository from the reference rather than from identity: identity is
// normalised for cache keys ("index.docker.io/library/busybox@..."), and
// containerd wants its own normalisation of the host.
func pullRef(image, identity string) (string, error) {
	named, err := reference.ParseDockerRef(image)
	if err != nil {
		return "", fmt.Errorf("containerd executor: parse image %q: %w", image, err)
	}
	at := strings.LastIndex(identity, "@")
	if at < 0 {
		return "", fmt.Errorf("containerd executor: %q is not a digest reference", identity)
	}
	return named.Name() + identity[at:], nil
}

// imageEnv is the environment an image declares, read straight out of its
// config blob.
//
// oci.WithImageConfig would supply this and more, and it is deliberately not
// used: it resolves the image's user by TEMP-MOUNTING the rootfs on whatever
// machine the client runs on, so it fails with `open
// .../snapshots/25009/fs: no such file or directory` for any client that is
// not sharing the daemon's filesystem — an engine in a sidecar container next
// to the node's containerd, for one. The user and working directory are set
// explicitly here anyway; the environment is the only thing worth taking from
// the image, and a content-store read is an API call like any other.
func imageEnv(ctx context.Context, img client.Image) ([]string, error) {
	desc, err := img.Config(ctx)
	if err != nil {
		return nil, fmt.Errorf("containerd executor: config of %s: %w", img.Name(), err)
	}
	blob, err := content.ReadBlob(ctx, img.ContentStore(), desc)
	if err != nil {
		return nil, fmt.Errorf("containerd executor: read the config of %s: %w", img.Name(), err)
	}
	var cfg ocispec.Image
	if err := json.Unmarshal(blob, &cfg); err != nil {
		return nil, fmt.Errorf("containerd executor: parse the config of %s: %w", img.Name(), err)
	}
	return cfg.Config.Env, nil
}

// specOpts builds the OCI runtime spec for one sandbox.
func (e *Executor) specOpts(env []string) []oci.SpecOpts {
	opts := []oci.SpecOpts{
		// The image's environment first, then everything this backend insists
		// on, which must win.
		oci.WithEnv(env),
		// The container exists to be entered, not to run one program: its init
		// process idles so exec processes have somewhere to run.
		oci.WithProcessArgs("sh", "-c", "while :; do sleep 3600; done"),
		oci.WithProcessCwd("/"),
		// The sandbox root is a tmpfs rather than a directory in the image:
		// the image's filesystem is owned by root and the step is not root, so
		// a plain directory would be unwritable. 1777 is /tmp's mode, and the
		// container is the only thing that can see this mount.
		oci.WithMounts([]specs.Mount{{
			Destination: sandboxRoot,
			Type:        "tmpfs",
			Source:      "tmpfs",
			Options:     []string{"nosuid", "nodev", "mode=1777"},
		}}),
	}
	if e.cfg.Privileged {
		opts = append(opts, oci.WithPrivileged, oci.WithUIDGID(0, 0))
	} else {
		// Rootless by default, and no path back up: WithNoNewPrivileges stops
		// a setuid binary in the image from handing root back.
		opts = append(opts, oci.WithUIDGID(sandboxUID, sandboxGID), oci.WithNoNewPrivileges)
	}
	if e.cfg.HostNetwork {
		opts = append(opts,
			oci.WithHostNamespace(specs.NetworkNamespace),
			oci.WithHostHostsFile,
			oci.WithHostResolvconf,
		)
	}
	return opts
}

// createContainer prepares the image and creates the container from it,
// retrying once with the ordinary snapshotter when the lazy one turns out not
// to work on this daemon. See demoteSnapshotter for the failure that motivates
// the retry; the loop runs at most twice, because the second pass is by then
// on DefaultSnapshotter and nothing demotes that.
func (e *Executor) createContainer(
	ctx context.Context, id, specImage string, lease executor.LeaseScope,
) (client.Container, error) {
	for {
		img, snapshotter, err := e.prepare(ctx, specImage)
		if err != nil {
			return nil, err
		}
		env, err := imageEnv(ctx, img)
		if err != nil {
			return nil, err
		}
		container, err := e.client.NewContainer(ctx, id,
			client.WithImage(img),
			client.WithSnapshotter(snapshotter),
			client.WithNewSnapshot(id+"-rootfs", img),
			client.WithNewSpec(e.specOpts(env)...),
			client.WithContainerLabels(map[string]string{
				"dhole.dev/managed": "true",
				"dhole.dev/lease":   string(lease),
			}),
		)
		if err == nil {
			return container, nil
		}
		if !e.demoteSnapshotter(err) {
			return nil, fmt.Errorf("containerd executor: create sandbox container: %w", err)
		}
	}
}

// Acquire creates a container and starts its idle init process.
func (e *Executor) Acquire(ctx context.Context, spec executor.Spec) (executor.Sandbox, error) {
	// Before anything is created: a spec asking for what this backend does not
	// advertise is refused, not quietly downgraded.
	if err := e.requireCapabilities(spec.Requirements.Capabilities); err != nil {
		return nil, err
	}
	lease := spec.Lease
	if lease == "" {
		lease = executor.LeaseStep
	}
	ctx = e.scoped(ctx)

	id := "dhole-sandbox-" + randomID()
	container, err := e.createContainer(ctx, id, spec.Image, lease)
	if err != nil {
		return nil, err
	}
	sb := &sandbox{exec: e, id: id, container: container, env: spec.Env, workDir: sandboxRoot}

	// The init process's own IO goes nowhere: it prints nothing, and a FIFO
	// nobody reads fills up and blocks the container it belongs to.
	task, err := container.NewTask(ctx, cio.NullIO)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("containerd executor: create task for %s: %w", id, err),
			sb.Release(context.WithoutCancel(ctx)))
	}
	sb.task = task
	if err := task.Start(ctx); err != nil {
		return nil, errors.Join(
			fmt.Errorf("containerd executor: start task for %s: %w", id, err),
			sb.Release(context.WithoutCancel(ctx)))
	}
	if err := waitRunning(ctx, task); err != nil {
		return nil, errors.Join(err, sb.Release(context.WithoutCancel(ctx)))
	}

	if spec.WorkDir != "" {
		if sb.workDir, err = resolve(sandboxRoot, spec.WorkDir); err != nil {
			return nil, errors.Join(err, sb.Release(context.WithoutCancel(ctx)))
		}
		if _, err := sb.run(ctx, "", []string{"mkdir", "-p", sb.workDir}, nil, nil, nil); err != nil {
			return nil, errors.Join(err, sb.Release(context.WithoutCancel(ctx)))
		}
	}
	return sb, nil
}

// waitRunning blocks until the task is actually running. Start returns as soon
// as the shim accepted it, and an exec against a task still in Created fails
// with an error that says nothing about the race that caused it.
func waitRunning(ctx context.Context, task client.Task) error {
	deadline := time.Now().Add(runningTimeout)
	for {
		status, err := task.Status(ctx)
		if err != nil {
			return fmt.Errorf("containerd executor: task status: %w", err)
		}
		switch status.Status {
		case client.Running:
			return nil
		case client.Stopped:
			return fmt.Errorf("containerd executor: sandbox task stopped before it was ready (exit %d)",
				status.ExitStatus)
		case client.Created, client.Pausing, client.Paused, client.Unknown:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("containerd executor: sandbox task was still %s after %s", status.Status, runningTimeout)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("containerd executor: wait for sandbox task: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}

type sandbox struct {
	exec      *Executor
	id        string
	container client.Container
	task      client.Task
	env       map[string]string
	workDir   string

	mu       sync.Mutex
	released bool
}

// Exec runs a command inside the sandbox container as an exec process. A
// non-zero exit is an exit code, not an error; err is reserved for the sandbox
// failing to run the command at all.
func (s *sandbox) Exec(ctx context.Context, cmd executor.Cmd) (int32, error) {
	if len(cmd.Args) == 0 {
		return 0, errors.New("containerd executor: exec with no command")
	}
	dir := s.workDir
	if cmd.Dir != "" {
		var err error
		if dir, err = resolve(s.workDir, cmd.Dir); err != nil {
			return 0, err
		}
	}
	// Every process of this command carries a unique marker in its
	// environment. Cancellation needs to find them again, and it cannot do it
	// by parentage: a step whose direct child exits leaves its grandchildren
	// reparented to the container's init, with nothing left connecting them to
	// this command. An inherited environment variable survives that, and
	// survives a new process group too.
	tree := treeMarker + "=" + randomID()
	script := fmt.Sprintf("cd %s || exit 127\nexec env %s %s %s",
		shellQuote(dir), shellQuote(tree), envArgs(s.env, cmd.Env), shellArgs(cmd.Args))
	// The marker travels with the command: run sweeps the tree by it if the
	// caller's context ends. Under a pipeline or pool lease the container
	// outlives the step, so anything the step spawned and abandoned would
	// otherwise go on burning the node's CPU for the rest of the run.
	return s.run(ctx, tree, []string{"sh", "-c", script}, cmd.Stdin, cmd.Stdout, cmd.Stderr)
}

// treeMarker is the environment variable naming which command a process
// belongs to. It is read back out of /proc/<pid>/environ, which the kernel
// only shows to the same user — the same user every process in the container
// runs as.
const treeMarker = "DHOLE_EXEC_TREE"

// killTree kills every process in the container carrying marker, which is the
// command and everything it spawned, however deep and whoever has since
// adopted it. The sweeping shell excludes itself; it does not carry the
// marker, but saying so costs nothing and a future caller might.
func (s *sandbox) killTree(ctx context.Context, marker string) error {
	// Detached from the caller's context on purpose: this runs BECAUSE that
	// context is done, and a cancelled step still has to leave nothing behind.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), killTreeTimeout)
	defer cancel()
	script := fmt.Sprintf(`for p in /proc/[0-9]*; do
  pid=${p#/proc/}
  [ "$pid" = "$$" ] && continue
  if grep -qa %s "$p/environ" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null; fi
done
exit 0`, shellQuote(marker))
	if _, err := s.run(ctx, "", []string{"sh", "-c", script}, nil, nil, nil); err != nil {
		return fmt.Errorf("containerd executor: terminate the cancelled step's processes: %w", err)
	}
	return nil
}

// Signal delivers sig to every process the sandbox is running, which is every
// process in the container except its idle init and the signalling shell
// itself. Signalling an idle sandbox is a no-op, not an error.
//
// containerd can signal a task's whole cgroup, and this deliberately does not
// use that: it would take the init process with it, and the sandbox has to
// survive being signalled — a pipeline-leased container is entered again by
// the next step.
func (s *sandbox) Signal(ctx context.Context, sig executor.Signal) error {
	name, err := signalName(sig)
	if err != nil {
		return err
	}
	script := fmt.Sprintf(`for p in /proc/[0-9]*; do
  pid=${p#/proc/}
  [ "$pid" = 1 ] && continue
  [ "$pid" = "$$" ] && continue
  kill -%s "$pid" 2>/dev/null
done
exit 0`, name)
	if _, err := s.run(ctx, "", []string{"sh", "-c", script}, nil, nil, nil); err != nil {
		return fmt.Errorf("containerd executor: signal %s: %w", sig, err)
	}
	return nil
}

func signalName(sig executor.Signal) (string, error) {
	switch sig {
	case executor.SIGTERM:
		return "TERM", nil
	case executor.SIGINT:
		return "INT", nil
	case executor.SIGKILL:
		return "KILL", nil
	default:
		return "", fmt.Errorf("containerd executor: unknown signal %q", sig)
	}
}

// run starts one exec process in the sandbox container and waits for it.
func (s *sandbox) run(
	ctx context.Context, tree string, argv []string, stdin io.Reader, stdout, stderr io.Writer,
) (int32, error) {
	s.mu.Lock()
	released, task := s.released, s.task
	s.mu.Unlock()
	if released || task == nil {
		return 0, fmt.Errorf("containerd executor: sandbox %s is released", s.id)
	}
	ctx = s.exec.scoped(ctx)

	pspec, err := s.processSpec(ctx, argv)
	if err != nil {
		return 0, err
	}
	// All three streams are always wired up. A nil stream leaves cio without a
	// FIFO path for it, and the shim then fails the whole exec with
	// `containerd-shim: opening file "" failed` — every command with no stderr
	// writer, which is most of them. An empty stdin reader is EOF, which is
	// what the interface promises a nil Stdin means.
	if stdin == nil {
		stdin = strings.NewReader("")
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	// A cancellation has to reach the step wherever this function happens to
	// be, so it is watched for here rather than handled at one point in the
	// flow. The step's direct child usually exits at once — that is what
	// OrphanScript in the contract does, and what a build script that
	// backgrounds a server does — so the exit status arrives long before the
	// cancel, and by the time it comes this function is already blocked
	// tearing the exec down: containerd cannot delete an exec process whose
	// stdio is still held open by something the step left running, and waits
	// fifteen seconds before giving up. Sweeping the tree from here unblocks
	// that teardown; sweeping it after the status instead never runs at all.
	if tree != "" {
		returned := make(chan struct{})
		defer close(returned)
		go func() {
			select {
			case <-ctx.Done():
				if err := s.killTree(ctx, tree); err != nil {
					s.exec.log.Warn("containerd executor: could not sweep the cancelled step's processes",
						"sandbox", s.id, "error", err)
				}
			case <-returned:
			}
		}()
	}

	// containerd keeps the process's stdin open until it is told not to, so a
	// command reading stdin to EOF — `cat > file`, which is how Put writes —
	// waits forever on input that has already all arrived. The reader below
	// reports when the last byte has been read, and CloseIO is what actually
	// delivers the EOF.
	eof := make(chan struct{})
	execID := "dhole-exec-" + randomID()
	proc, err := task.Exec(ctx, execID, pspec,
		cio.NewCreator(cio.WithStreams(&stdinCloser{r: stdin, eof: eof}, stdout, stderr)))
	if err != nil {
		return 0, fmt.Errorf("containerd executor: exec %q in sandbox %s: %w", argv[0], s.id, err)
	}
	defer func() {
		// Deleting the process is what frees the shim's bookkeeping and the
		// FIFOs; WithProcessKill covers the paths where it is still alive.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), processTimeout)
		defer cancel()
		if _, err := proc.Delete(dctx, client.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
			s.exec.log.Warn("containerd executor: could not delete exec process",
				"sandbox", s.id, "exec", execID, "error", err)
		}
	}()

	// Wait is armed before Start, and on a context that outlives the caller's:
	// a process that exits between Start and Wait would otherwise never be
	// reaped, and a cancelled step still has an exit status worth reading.
	statusC, err := proc.Wait(context.WithoutCancel(ctx))
	if err != nil {
		return 0, fmt.Errorf("containerd executor: wait for %q in sandbox %s: %w", argv[0], s.id, err)
	}
	if err := proc.Start(ctx); err != nil {
		return 0, fmt.Errorf("containerd executor: start %q in sandbox %s: %w", argv[0], s.id, err)
	}
	// Only after Start: closing the input of a process that does not exist yet
	// is an error, and the eof signal may already have fired for the empty
	// stdin every command without one is given.
	go func() {
		<-eof
		if err := proc.CloseIO(context.WithoutCancel(ctx), client.WithStdinCloser); err != nil &&
			!errdefs.IsNotFound(err) {
			s.exec.log.Warn("containerd executor: could not close the command's stdin",
				"sandbox", s.id, "exec", execID, "error", err)
		}
	}()

	var status client.ExitStatus
	select {
	case status = <-statusC:
		drainIO(proc)
	case <-ctx.Done():
		// The sweep above is already running; this is the command itself,
		// which carries the marker too but need not wait for a shell to find
		// it. Both are needed: a command with no marker (Signal's own sweep,
		// Put's writer) has only this one.
		kctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), processTimeout)
		defer cancel()
		if err := proc.Kill(kctx, syscall.SIGKILL); err != nil && !errdefs.IsNotFound(err) {
			return 0, fmt.Errorf("containerd executor: kill the cancelled %q in sandbox %s: %w",
				argv[0], s.id, err)
		}
		select {
		case status = <-statusC:
		case <-kctx.Done():
			return 0, fmt.Errorf("containerd executor: %q in sandbox %s did not exit after being killed: %w",
				argv[0], s.id, ctx.Err())
		}
	}
	code, _, err := status.Result()
	if err != nil {
		return 0, fmt.Errorf("containerd executor: exit status of %q in sandbox %s: %w", argv[0], s.id, err)
	}
	return narrowExit(code), nil
}

// stdinCloser reports, once, that everything the caller wanted to write has
// been read. It is the reader containerd's own ctr uses for the same purpose:
// the shim holds the stdin FIFO open so a caller can attach to it later, so
// nothing but an explicit CloseIO ever gives the process an EOF.
type stdinCloser struct {
	r    io.Reader
	eof  chan struct{}
	once sync.Once
}

func (s *stdinCloser) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if errors.Is(err, io.EOF) {
		s.once.Do(func() { close(s.eof) })
	}
	return n, err
}

// processSpec is the exec process's OCI spec, taken from the container's own
// so that the user, capabilities and environment of an exec are the sandbox's
// and not a fresh set of defaults — a step running as a different uid from the
// sandbox it was given is not the sandbox anybody asked for.
func (s *sandbox) processSpec(ctx context.Context, argv []string) (*specs.Process, error) {
	spec, err := s.container.Spec(ctx)
	if err != nil {
		return nil, fmt.Errorf("containerd executor: read the spec of sandbox %s: %w", s.id, err)
	}
	if spec.Process == nil {
		return nil, fmt.Errorf("containerd executor: sandbox %s has no process spec", s.id)
	}
	p := *spec.Process
	p.Args = argv
	p.Cwd = "/"
	p.Terminal = false
	return &p, nil
}

// drainIO lets output that is still in flight arrive, but not indefinitely. A
// grandchild that outlived the step holds the inherited stream open, and
// waiting for it to let go would hang a step that has already exited — the
// contract's cancellation window is two seconds, and this sits inside it.
func drainIO(proc client.Process) {
	streams := proc.IO()
	if streams == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		streams.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(ioDrainGrace):
	}
}

// Put writes a file into the sandbox under a path relative to its root.
func (s *sandbox) Put(ctx context.Context, name string, r io.Reader) error {
	target, err := resolve(sandboxRoot, name)
	if err != nil {
		return err
	}
	if r == nil {
		r = strings.NewReader("")
	}
	script := fmt.Sprintf("mkdir -p %s && cat > %s", shellQuote(path.Dir(target)), shellQuote(target))
	code, err := s.run(ctx, "", []string{"sh", "-c", script}, r, nil, nil)
	if err != nil {
		return fmt.Errorf("containerd executor: put %q: %w", name, err)
	}
	if code != 0 {
		return fmt.Errorf("containerd executor: put %q: writer exited %d", name, code)
	}
	return nil
}

// Get reads a file back out of the sandbox. The caller closes the reader; a
// file that is missing or unreadable surfaces as a read error rather than as
// silently empty content.
func (s *sandbox) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	target, err := resolve(sandboxRoot, name)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	go func() {
		code, err := s.run(context.WithoutCancel(ctx), "", []string{"cat", "--", target}, nil, pw, nil)
		switch {
		case err != nil:
			_ = pw.CloseWithError(fmt.Errorf("containerd executor: get %q: %w", name, err))
		case code != 0:
			_ = pw.CloseWithError(fmt.Errorf("containerd executor: get %q: reader exited %d", name, code))
		default:
			_ = pw.Close()
		}
	}()
	return pr, nil
}

// Release deletes the sandbox container and its snapshot. It is idempotent,
// and anything already gone is a success: cleanup runs unconditionally, often
// after something else has already removed the container, and failing there
// would turn a finished run into a failed one.
func (s *sandbox) Release(ctx context.Context) error {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	s.released = true
	task, container := s.task, s.container
	s.mu.Unlock()

	// Detached from the caller's context, and bounded instead. Release runs on
	// the path where the caller has already given up — a cancelled step, a
	// failed Acquire — and a Release that declines to do anything because the
	// context is done leaves a container and its snapshot on the node for as
	// long as the node lives.
	ctx, cancel := context.WithTimeout(s.exec.scoped(context.WithoutCancel(ctx)), releaseTimeout)
	defer cancel()
	var errs []error
	if task != nil {
		// The whole cgroup, init included: this is the one place the sandbox
		// is meant to stop existing, so nothing it spawned may outlive it.
		if err := task.Kill(ctx, syscall.SIGKILL, client.WithKillAll); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("containerd executor: kill sandbox task %s: %w", s.id, err))
		}
		if _, err := task.Delete(ctx, client.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("containerd executor: delete sandbox task %s: %w", s.id, err))
		}
	}
	if err := container.Delete(ctx, client.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
		errs = append(errs, fmt.Errorf("containerd executor: delete sandbox container %s: %w", s.id, err))
	}
	return errors.Join(errs...)
}

// randomID is a short unique suffix for a container or exec id. It is not a
// secret, but crypto/rand costs nothing here and math/rand in a server that
// forgot to seed it produces the same ids on every start.
func randomID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on any platform Dhole runs on; if it ever
		// does, a timestamp is still unique enough for a container id.
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// envArgs renders the sandbox environment and a command's overrides as
// arguments to env(1). The host environment is deliberately not inherited: a
// step that depends on an ambient variable is a step whose inputs are not
// declared.
func envArgs(base, overrides map[string]string) string {
	merged := make(map[string]string, len(base)+len(overrides))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range overrides {
		merged[k] = v
	}
	parts := make([]string, 0, len(merged))
	for k, v := range merged {
		parts = append(parts, shellQuote(k+"="+v))
	}
	return strings.Join(parts, " ")
}

func shellArgs(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, a := range args {
		quoted = append(quoted, shellQuote(a))
	}
	return strings.Join(quoted, " ")
}

// shellQuote makes one string a single shell word. The argument vector reaches
// a shell inside the container rather than execve directly, so nothing may be
// left for the shell to interpret.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// resolve joins name onto root and refuses anything that leaves it, so a
// crafted artifact name cannot read or write the container's filesystem
// outside the sandbox.
func resolve(root, name string) (string, error) {
	if name == "" {
		return "", errors.New("containerd executor: empty path")
	}
	if path.IsAbs(name) {
		return "", fmt.Errorf("containerd executor: %q is absolute, sandbox paths are relative", name)
	}
	target := path.Join(root, path.Clean(name))
	if target != root && !strings.HasPrefix(target, root+"/") {
		return "", fmt.Errorf("containerd executor: %q escapes the sandbox", name)
	}
	return target, nil
}

// narrowExit converts containerd's exit status to the wire's int32 without
// wrapping. containerd reports a signalled process as 128+signal, which is
// where the 137 of an OOM kill comes from.
func narrowExit(code uint32) int32 {
	if code > 255 {
		return 255
	}
	return int32(code)
}
