// Package kubernetes runs steps as pods in a Kubernetes cluster.
//
// A sandbox is one pod holding an idle container; commands run inside it
// through the API server's exec subresource. That shape is what lets the
// lease scopes of ADR 0006 mean something here: a step-scoped sandbox is a pod
// deleted when the step ends, while a pipeline-scoped one is the same pod
// entered again for every step — carrying state no cache key can describe,
// which is exactly why internal/cache refuses to cache such steps.
//
// Nothing in this backend needs the executor interface to grow a Kubernetes
// shape: the pod is an implementation detail behind Acquire, and everything
// this backend cannot honestly promise — privilege, host mounts — is a
// capability it declines to advertise rather than a special case in the core.
package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"io"
	neturl "net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/client-go/util/exec"
	"k8s.io/streaming/pkg/httpstream"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
)

// Kind is this backend's stable identifier in configuration.
const Kind = "kubernetes"

const (
	// defaultImage is the sandbox image when no pod template names one. It is
	// pinned to a tag here and resolved to a digest for cache identity; see
	// EnvironmentIdentity.
	defaultImage = "busybox:1.36"
	// containerName is the container commands run in. A pod template may add
	// sidecars; the first container is the step's.
	containerName = "step"
	// sandboxRoot is the sandbox filesystem root inside the container. Put and
	// Get resolve names under it and never escape it.
	sandboxRoot = "/dhole"
	// readyTimeout bounds waiting for a pod to schedule and pull its image.
	readyTimeout = 3 * time.Minute
	// evictionExitCode is what a step reports when its pod disappeared under
	// it. 137 is the conventional SIGKILL status, and it is deliberately not
	// zero: an evicted step is a failed step, never a silent pass.
	evictionExitCode = 137
)

// Config is how a Kubernetes executor is pointed at a cluster.
type Config struct {
	// Kubeconfig is the path to a kubeconfig file. Empty means the in-cluster
	// service account, so the same binary works as a pod.
	Kubeconfig string
	// Namespace is where sandbox pods are created. Empty means "default".
	Namespace string
	// ServiceAccount is the identity sandbox pods run as. Empty leaves the
	// namespace default.
	ServiceAccount string
	// PodTemplate is the pod every sandbox is made from. Its first container
	// supplies the image and the security context; its command is replaced,
	// because the pod exists to be entered rather than to run one program.
	PodTemplate *corev1.PodSpec
}

// Executor hands out sandboxes that are pods.
type Executor struct {
	cfg       Config
	rest      *rest.Config
	client    kubernetes.Interface
	namespace string

	identityOnce sync.Once
	identity     string
	identityErr  error
}

// New returns a Kubernetes executor talking to the cluster cfg names.
func New(cfg Config) (executor.Executor, error) {
	restCfg, err := restConfig(cfg.Kubeconfig)
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes executor: build client: %w", err)
	}
	ns := cfg.Namespace
	if ns == "" {
		ns = metav1.NamespaceDefault
	}
	return &Executor{cfg: cfg, rest: restCfg, client: client, namespace: ns}, nil
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("kubernetes executor: no kubeconfig and not in a cluster: %w", err)
		}
		return cfg, nil
	}
	if _, err := os.Stat(kubeconfig); err != nil {
		return nil, fmt.Errorf("kubernetes executor: kubeconfig: %w", err)
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("kubernetes executor: load kubeconfig %q: %w", kubeconfig, err)
	}
	return cfg, nil
}

// Kind implements executor.Executor.
func (*Executor) Kind() string { return Kind }

// Capabilities implements executor.Executor. Only what the configured pod
// template genuinely provides is advertised: a pod always has a network
// namespace, but privilege and host mounts exist only if the template asked
// for them, and secret delivery is not implemented here at all — advertising
// any of those would be a promise the scheduler would then rely on.
func (e *Executor) Capabilities() []dholev1.Capability {
	caps := []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK}
	spec := e.podSpec("")
	for i := range spec.Containers {
		sc := spec.Containers[i].SecurityContext
		if sc != nil && sc.Privileged != nil && *sc.Privileged {
			caps = append(caps, dholev1.Capability_CAPABILITY_PRIVILEGED)
			break
		}
	}
	for i := range spec.Volumes {
		if spec.Volumes[i].HostPath != nil {
			caps = append(caps, dholev1.Capability_CAPABILITY_HOST_MOUNT)
			break
		}
	}
	return caps
}

// EnvironmentIdentity implements executor.Executor. It is the digest of the
// image sandboxes run, never its tag: a tag is a moving target, so caching
// against one serves results produced in an environment that no longer exists.
// An image that cannot be resolved to a digest yields ErrNoStableIdentity, and
// its steps stay uncached, which is the safe direction to be wrong in.
//
// The identity describes the executor's configured template. A Spec that names
// its own image gets that image for its pod, and a caller that mixes images on
// one executor needs a per-sandbox identity the interface does not yet carry.
func (e *Executor) EnvironmentIdentity() (string, error) {
	e.identityOnce.Do(func() {
		e.identity, e.identityErr = resolveDigest(e.image(""))
	})
	return e.identity, e.identityErr
}

func resolveDigest(image string) (string, error) {
	noIdentity := func(err error) (string, error) {
		return "", fmt.Errorf("kubernetes executor: %q has no resolvable digest: %w: %w",
			image, err, executor.ErrNoStableIdentity)
	}
	ref, err := name.ParseReference(image)
	if err != nil {
		return noIdentity(err)
	}
	if digest, ok := ref.(name.Digest); ok {
		return digest.Name(), nil
	}
	desc, err := remote.Head(ref)
	if err != nil {
		return noIdentity(err)
	}
	return ref.Context().Digest(desc.Digest.String()).Name(), nil
}

// image is the image a sandbox runs: the spec's if it named one, else the pod
// template's, else the default.
func (e *Executor) image(specImage string) string {
	if specImage != "" {
		return specImage
	}
	if t := e.cfg.PodTemplate; t != nil && len(t.Containers) > 0 && t.Containers[0].Image != "" {
		return t.Containers[0].Image
	}
	return defaultImage
}

// podSpec builds the pod spec for one sandbox from the template. The first
// container's command is always replaced: the pod's job is to stay alive so
// commands can be run inside it, not to run one program and exit.
func (e *Executor) podSpec(specImage string) *corev1.PodSpec {
	var spec *corev1.PodSpec
	if e.cfg.PodTemplate != nil {
		spec = e.cfg.PodTemplate.DeepCopy()
	} else {
		spec = &corev1.PodSpec{}
	}
	if len(spec.Containers) == 0 {
		spec.Containers = []corev1.Container{{Name: containerName}}
	}
	spec.Containers[0].Image = e.image(specImage)
	spec.Containers[0].Command = []string{"sh", "-c",
		fmt.Sprintf("mkdir -p %s; while :; do sleep 3600; done", sandboxRoot)}
	spec.Containers[0].Args = nil
	spec.RestartPolicy = corev1.RestartPolicyNever
	if e.cfg.ServiceAccount != "" {
		spec.ServiceAccountName = e.cfg.ServiceAccount
	}
	// Sandboxes are disposable: a pod that lingers in Terminating holds a name
	// and a slot in the namespace for no benefit.
	grace := int64(0)
	spec.TerminationGracePeriodSeconds = &grace
	return spec
}

// Acquire creates a pod and waits for it to be ready to run commands.
func (e *Executor) Acquire(ctx context.Context, spec executor.Spec) (executor.Sandbox, error) {
	lease := spec.Lease
	if lease == "" {
		lease = executor.LeaseStep
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dhole-sandbox-" + rand.String(8),
			Namespace: e.namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "dhole",
				"dhole.dev/lease":              string(lease),
			},
		},
		Spec: *e.podSpec(spec.Image),
	}
	created, err := e.client.CoreV1().Pods(e.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("kubernetes executor: create sandbox pod: %w", err)
	}

	sb := &sandbox{
		exec:    e,
		pod:     created.Name,
		env:     spec.Env,
		workDir: sandboxRoot,
	}
	if spec.WorkDir != "" {
		if sb.workDir, err = resolve(sandboxRoot, spec.WorkDir); err != nil {
			return nil, errors.Join(err, sb.Release(context.WithoutCancel(ctx)))
		}
	}
	if err := e.waitReady(ctx, created.Name); err != nil {
		return nil, errors.Join(err, sb.Release(context.WithoutCancel(ctx)))
	}
	if spec.WorkDir != "" {
		if _, err := sb.run(ctx, []string{"mkdir", "-p", sb.workDir}, nil, nil, nil); err != nil {
			return nil, errors.Join(err, sb.Release(context.WithoutCancel(ctx)))
		}
	}
	return sb, nil
}

func (e *Executor) waitReady(ctx context.Context, podName string) error {
	deadline := time.Now().Add(readyTimeout)
	for {
		pod, err := e.client.CoreV1().Pods(e.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("kubernetes executor: wait for pod %s: %w", podName, err)
		}
		switch pod.Status.Phase {
		case corev1.PodFailed, corev1.PodSucceeded:
			return fmt.Errorf("kubernetes executor: sandbox pod %s ended in phase %s before it was ready",
				podName, pod.Status.Phase)
		case corev1.PodRunning:
			if len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].Ready {
				return nil
			}
		case corev1.PodPending, corev1.PodUnknown:
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("kubernetes executor: sandbox pod %s not ready within %s (phase %s)",
				podName, readyTimeout, pod.Status.Phase)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kubernetes executor: wait for pod %s: %w", podName, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

type sandbox struct {
	exec    *Executor
	pod     string
	env     map[string]string
	workDir string

	mu       sync.Mutex
	released bool
}

// Exec runs a command inside the sandbox pod. A non-zero exit is an exit code,
// not an error; the pod disappearing under the command is the opposite — an
// error, with a non-zero code, so an evicted step can never read as a pass.
func (s *sandbox) Exec(ctx context.Context, cmd executor.Cmd) (int32, error) {
	if len(cmd.Args) == 0 {
		return 0, errors.New("kubernetes executor: exec with no command")
	}
	dir := s.workDir
	if cmd.Dir != "" {
		var err error
		if dir, err = resolve(s.workDir, cmd.Dir); err != nil {
			return 0, err
		}
	}
	script := fmt.Sprintf("cd %s || exit 127\nexec env %s %s",
		shellQuote(dir), envArgs(s.env, cmd.Env), shellArgs(cmd.Args))
	return s.run(ctx, []string{"sh", "-c", script}, cmd.Stdin, cmd.Stdout, cmd.Stderr)
}

// run streams one command through the exec subresource.
func (s *sandbox) run(
	ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer,
) (int32, error) {
	req := s.exec.client.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(s.exec.namespace).Name(s.pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   argv,
			Stdin:     stdin != nil,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	streamer, err := newStreamer(s.exec.rest, req.URL())
	if err != nil {
		return 0, fmt.Errorf("kubernetes executor: attach to pod %s: %w", s.pod, err)
	}
	// A nil writer is discarded rather than left unset: the API server opens
	// the output streams either way, and a client with nowhere to put them
	// holds the session open long after the command has exited.
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	err = streamer.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin: stdin, Stdout: stdout, Stderr: stderr,
	})
	if err == nil {
		return 0, nil
	}

	var exitErr utilexec.CodeExitError
	if errors.As(err, &exitErr) {
		code := narrowExit(exitErr.Code)
		// A container killed out from under the command exits by signal. Only
		// then is it worth asking the API server whether the pod is still
		// there, so the common non-zero exit stays one round trip.
		if code == evictionExitCode || code == 143 {
			if gone, why := s.podGone(ctx); gone {
				return evictionExitCode, why
			}
		}
		return code, nil
	}
	if gone, why := s.podGone(ctx); gone {
		return evictionExitCode, why
	}
	return 0, fmt.Errorf("kubernetes executor: run %q in pod %s: %w", argv[0], s.pod, err)
}

// newStreamer opens the exec subresource, preferring the WebSocket protocol
// and falling back to SPDY. WebSocket is what current API servers and load
// balancers in front of them speak reliably; SPDY is the older upgrade that
// clusters before 1.29 still need, and it hangs rather than fails through some
// proxies, so it is the fallback and not the default.
func newStreamer(cfg *rest.Config, url *neturl.URL) (remotecommand.Executor, error) {
	ws, err := remotecommand.NewWebSocketExecutor(cfg, "GET", url.String())
	if err != nil {
		return nil, err
	}
	spdy, err := remotecommand.NewSPDYExecutor(cfg, "POST", url)
	if err != nil {
		return nil, err
	}
	return remotecommand.NewFallbackExecutor(ws, spdy, httpstream.IsUpgradeFailure)
}

// podGone reports whether the sandbox pod has been deleted, is on its way out,
// or has failed — the three ways a command can stop without the step having
// run. A pod that is still healthy answers on the first round trip, because
// this sits on the path of every signalled command; only an API error is worth
// a second look.
func (s *sandbox) podGone(ctx context.Context) (bool, error) {
	// Deletion has to be observable even when the caller's context died with
	// the exec it was driving.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	evicted := func(detail string) (bool, error) {
		return true, fmt.Errorf(
			"kubernetes executor: pod %s/%s was evicted or deleted while the command was running%s",
			s.exec.namespace, s.pod, detail)
	}
	for attempt := range 3 {
		pod, err := s.exec.client.CoreV1().Pods(s.exec.namespace).Get(ctx, s.pod, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			return evicted("")
		case err != nil:
			if attempt == 2 {
				return false, nil
			}
			select {
			case <-ctx.Done():
				return false, nil
			case <-time.After(200 * time.Millisecond):
			}
			continue
		case pod.DeletionTimestamp != nil:
			return evicted("")
		case pod.Status.Phase == corev1.PodFailed:
			return evicted(fmt.Sprintf(": %s %s", pod.Status.Reason, pod.Status.Message))
		default:
			// The pod is healthy: the command simply exited.
			return false, nil
		}
	}
	return false, nil
}

// Signal delivers sig to every process the sandbox is running, which is every
// process in the container except the idle process that keeps it alive and the
// signalling shell itself. Signalling an idle sandbox is a no-op, not an error.
func (s *sandbox) Signal(ctx context.Context, sig executor.Signal) error {
	name, err := signalName(sig)
	if err != nil {
		return err
	}
	// A step is a process tree, and killing only its root leaves the rest
	// running in the pod until the pod dies — which, under a pipeline lease,
	// can be the whole run.
	script := fmt.Sprintf(`for p in /proc/[0-9]*; do
  pid=${p#/proc/}
  [ "$pid" = 1 ] && continue
  [ "$pid" = "$$" ] && continue
  kill -%s "$pid" 2>/dev/null
done
exit 0`, name)
	if _, err := s.run(ctx, []string{"sh", "-c", script}, nil, nil, nil); err != nil {
		return fmt.Errorf("kubernetes executor: signal %s: %w", sig, err)
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
		return "", fmt.Errorf("kubernetes executor: unknown signal %q", sig)
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
	code, err := s.run(ctx, []string{"sh", "-c", script}, r, nil, nil)
	if err != nil {
		return fmt.Errorf("kubernetes executor: put %q: %w", name, err)
	}
	if code != 0 {
		return fmt.Errorf("kubernetes executor: put %q: writer exited %d", name, code)
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
		code, err := s.run(context.WithoutCancel(ctx),
			[]string{"cat", "--", target}, nil, pw, nil)
		switch {
		case err != nil:
			_ = pw.CloseWithError(fmt.Errorf("kubernetes executor: get %q: %w", name, err))
		case code != 0:
			_ = pw.CloseWithError(fmt.Errorf("kubernetes executor: get %q: reader exited %d", name, code))
		default:
			_ = pw.Close()
		}
	}()
	return pr, nil
}

// Release deletes the sandbox pod. It is idempotent, and a pod that is already
// gone is a success: cleanup runs unconditionally, often after an eviction has
// already removed the pod, and failing there would turn a finished run into a
// failed one. The delete sets no finalizer and no grace period, so the pod
// leaves immediately instead of lingering in Terminating.
func (s *sandbox) Release(ctx context.Context) error {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	s.released = true
	s.mu.Unlock()

	err := s.exec.client.CoreV1().Pods(s.exec.namespace).Delete(ctx, s.pod, *metav1.NewDeleteOptions(0))
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("kubernetes executor: delete sandbox pod %s: %w", s.pod, err)
	}
	return nil
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
		return "", errors.New("kubernetes executor: empty path")
	}
	if path.IsAbs(name) {
		return "", fmt.Errorf("kubernetes executor: %q is absolute, sandbox paths are relative", name)
	}
	target := path.Join(root, path.Clean(name))
	if target != root && !strings.HasPrefix(target, root+"/") {
		return "", fmt.Errorf("kubernetes executor: %q escapes the sandbox", name)
	}
	return target, nil
}

// narrowExit converts an exit status to the wire's int32 without wrapping.
func narrowExit(code int) int32 {
	switch {
	case code < 0:
		return -1
	case code > 255:
		return 255
	default:
		return int32(code)
	}
}
