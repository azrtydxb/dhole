// Package charts_test renders the Helm chart and asserts the things a
// rendered manifest can be wrong about while `helm lint` stays happy.
//
// helm lint checks that a chart is well-formed YAML with the fields Kubernetes
// wants. It cannot know that NATS reads a size differently from Kubernetes, so
// a chart that lints, templates and applies can still crash-loop on first
// boot — which is exactly what happened.
package charts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// render runs `helm template` and returns the manifest, skipping when helm is
// not installed rather than pretending the chart was checked.
func render(t *testing.T, args ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not on PATH: the chart cannot be rendered here")
	}
	out, err := exec.Command("helm", append([]string{"template", "dhole", "./dhole"}, args...)...).CombinedOutput()
	require.NoError(t, err, "helm template failed: %s", out)
	return string(out)
}

// renderErr is render for the cases where the refusal IS the behaviour under
// test, returning the error instead of failing on it.
func renderErr(t *testing.T, args ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not on PATH: the chart cannot be rendered here")
	}
	out, err := exec.Command("helm", append([]string{"template", "dhole", "./dhole"}, args...)...).CombinedOutput()
	return string(out), err
}

// renderNotes returns the chart's NOTES.txt as an operator sees it.
//
// `helm template` does not render notes, so a mistake in NOTES.txt is invisible
// to every other test in this file. A dry-run install does render them, which
// is both how the text is asserted and how the template itself is type-checked.
func renderNotes(t *testing.T, args ...string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not on PATH: the chart cannot be rendered here")
	}
	// Helm 4, because only its client-side dry run renders an install without
	// a cluster: helm 3's --dry-run still checks the API server is reachable
	// before it renders anything. On a developer's machine with helm 3 that is
	// a skip that says so; in CI, which installs helm 4, it is a failure,
	// because a skip there would be a check that silently stopped running.
	version, err := exec.Command("helm", "version", "--short").Output()
	require.NoError(t, err, "helm version")
	if !strings.HasPrefix(strings.TrimSpace(string(version)), "v4.") {
		if os.Getenv("CI") != "" {
			t.Fatalf("helm %s cannot render NOTES.txt without a cluster; CI must install helm 4", version)
		}
		t.Skipf("helm %s cannot render NOTES.txt without a cluster; install helm 4 to run this", version)
	}
	cmd := exec.Command("helm",
		append([]string{"install", "dhole", "./dhole", "--dry-run=client"}, args...)...)
	// Never a real cluster. Without this helm reads the developer's own
	// kubeconfig, so the test passed on any machine that could reach a
	// cluster — contacting it to do so — and failed on a CI runner that
	// could not, with "Kubernetes cluster unreachable".
	cmd.Env = append(os.Environ(), "KUBECONFIG="+filepath.Join(t.TempDir(), "no-cluster"))
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "helm install --dry-run failed: %s", out)
	return string(out)
}

// TestJetStreamSizeIsInNATSUnitsNotKubernetesOnes pins the translation.
//
// NATS parses only the FINAL character of a size as its unit, so the
// Kubernetes spelling "10Gi" is read as the number "10G" with unit "i" and
// refused: `max_file_store strconv.ParseInt: parsing "10G": invalid syntax`.
// The values file keeps the Kubernetes spelling because every other size in it
// is a Kubernetes quantity; the template is what translates.
func TestJetStreamSizeIsInNATSUnitsNotKubernetesOnes(t *testing.T) {
	manifest := render(t, "--set", "nats.jetstream.storageSize=10Gi")

	require.Contains(t, manifest, `max_file_store: "10G"`,
		"a Kubernetes quantity reached the NATS config, which refuses it at start-up")
	require.NotContains(t, manifest, `max_file_store: "10Gi"`)

	// The other Kubernetes suffixes translate too, or the same crash returns
	// for whoever picks a different unit.
	for value, want := range map[string]string{
		"512Mi": `max_file_store: "512M"`,
		"1Ti":   `max_file_store: "1T"`,
		"2048":  `max_file_store: "2048"`,
	} {
		require.Contains(t, render(t, "--set", "nats.jetstream.storageSize="+value), want,
			"storageSize %s did not translate", value)
	}
}

// TestEveryImageReferenceCarriesItsRegistry catches a chart that renders a
// reference the cluster cannot pull because the registry was dropped — the
// failure mode is ImagePullBackOff on a private registry, which looks like an
// infrastructure problem rather than a template one.
func TestEveryImageReferenceCarriesItsRegistry(t *testing.T) {
	manifest := render(t, "--set", "image.registry=registry.example",
		"--set", "image.repository=acme/dhole", "--set", "image.tag=v9")

	var found int
	for _, line := range strings.Split(manifest, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "image:") {
			continue
		}
		ref := strings.TrimSpace(strings.TrimPrefix(trimmed, "image:"))
		if ref == "" || strings.Contains(ref, "postgres:") || strings.Contains(ref, "nats:") {
			continue // the chart's own dependencies, pulled from the default registry
		}
		found++
		require.Contains(t, ref, "registry.example",
			"a Dhole image reference lost its registry: %s", ref)
	}
	require.Positive(t, found, "no Dhole image references were rendered at all")
}

// The chart ships a distroless engine image: no shell, no interpreter, nothing
// a command step can be. A deployment defaulting to the process backend runs
// every step straight into "fork/exec /bin/sh: no such file or directory", and
// that is exactly what a live cluster did before this default changed.
func TestEnginesRunTheirStepsInSandboxPodsByDefault(t *testing.T) {
	out := render(t)

	if !strings.Contains(out, `- name: DHOLE_EXECUTOR
              value: "kubernetes"`) {
		t.Errorf("engines do not default to the kubernetes backend; the image they run has no shell:\n%s", out)
	}
	if !strings.Contains(out, "- name: DHOLE_SANDBOX_NAMESPACE") {
		t.Error("the kubernetes backend is selected but never told which namespace to create sandboxes in")
	}
}

// A backend that may create pods and exec into them without the RBAC to do it
// fails at the first step rather than at install, which is the expensive place
// to find out.
func TestChoosingSandboxPodsGrantsTheRBACTheyNeed(t *testing.T) {
	out := render(t)

	for _, verb := range []string{"pods/exec", `resources: ["pods"]`} {
		if !strings.Contains(out, verb) {
			t.Errorf("the sandbox Role does not grant %s:\n%s", verb, out)
		}
	}
	if !strings.Contains(out, "kind: RoleBinding") {
		t.Error("the sandbox Role is never bound to the engine's service account")
	}
	if strings.Contains(out, "kind: ClusterRole") {
		t.Error("a sandbox grant escaped its namespace; it must be a Role, not a ClusterRole")
	}
}

// The other side of the default: an install that deliberately runs steps on the
// engine host must not be handed cluster credentials it never asked for.
func TestAProcessOnlyInstallGrantsNoSandboxRBAC(t *testing.T) {
	out := render(t, "--set", "engines[0].name=host", "--set", "engines[0].tier=trusted",
		"--set", "engines[0].executor=process")

	if strings.Contains(out, "-sandbox") {
		t.Errorf("a process-only install still renders the sandbox Role:\n%s", out)
	}
	if !strings.Contains(out, `value: "process"`) {
		t.Errorf("the chosen process backend did not reach the engine:\n%s", out)
	}
}

// Adding a second trust tier is the most ordinary edit this chart invites, and
// the natural way to write one — name, tier, slots, and no image block, because
// the default image is the right one — used to fail the render with a nil
// pointer rather than a message naming the missing key.
func TestAnEngineTierNeedNotRestateTheDefaultImage(t *testing.T) {
	out := render(t,
		"--set", "engines[0].name=extra",
		"--set", "engines[0].tier=untrusted",
		"--set", "engines[0].slots=4")

	if !strings.Contains(out, "azrtydxb/dhole-engine:") {
		t.Errorf("an engine without an image block did not fall back to the default image:\n%s", out)
	}
}

// The control plane and its engines must be pointed at the SAME object store,
// from one definition. Two independently-written blocks is two chances to
// point half a deployment somewhere else, and the result is invisible: every
// step succeeds and every log and artifact it produced is unreachable.
func TestThePlaneAndItsEnginesShareOneObjectStore(t *testing.T) {
	out := render(t,
		"--set", "objectStore.kind=s3",
		"--set", "objectStore.s3.bucket=dhole-artifacts",
		"--set", "objectStore.s3.endpoint=http://minio:9000")

	if got := strings.Count(out, `- name: DHOLE_S3_BUCKET`); got < 2 {
		t.Fatalf("the bucket reaches %d containers; the plane and its engines both need it:\n%s", got, out)
	}
	if got := strings.Count(out, `value: "dhole-artifacts"`); got < 2 {
		t.Errorf("the plane and its engines were given different buckets:\n%s", out)
	}
	if got := strings.Count(out, `value: "http://minio:9000"`); got < 2 {
		t.Errorf("the plane and its engines were given different endpoints:\n%s", out)
	}
}

// S3 credentials belong in a Secret, never in the rendered pod spec.
func TestS3CredentialsComeFromASecretRatherThanTheManifest(t *testing.T) {
	out := render(t,
		"--set", "objectStore.kind=s3",
		"--set", "objectStore.s3.bucket=dhole-artifacts",
		"--set", "objectStore.s3.existingSecret=dhole-s3")

	if !strings.Contains(out, "secretKeyRef") || !strings.Contains(out, "accessKeyId") {
		t.Errorf("the S3 credentials are not read from a Secret:\n%s", out)
	}
	if strings.Contains(out, "- name: DHOLE_S3_SECRET_ACCESS_KEY\n              value:") {
		t.Error("a secret access key was rendered as a literal value in the pod spec")
	}
}

// Choosing s3 without a bucket is a deployment that starts, runs, and loses
// everything it produces. It must fail at install instead.
func TestChoosingS3WithoutABucketIsRefusedAtInstall(t *testing.T) {
	if _, err := renderErr(t, "--set", "objectStore.kind=s3"); err == nil {
		t.Fatal("the chart rendered an s3 object store with no bucket")
	}
}

// A fresh install can write a pipeline and run nothing: a revision may not be
// approved by its author, and the bootstrap credential is the only principal
// there is. That is the correct rule and a dead end without instructions, so
// the chart has to say how to get out of it.
func TestTheNotesSayHowToEscapeTheBootstrapDeadlock(t *testing.T) {
	out := renderNotes(t)

	for _, want := range []string{"approved by its author", "token issue", "--subject reviewer"} {
		if !strings.Contains(out, want) {
			t.Errorf("the notes never mention %q, so an operator cannot approve anything:\n%s", want, out)
		}
	}
}

// The unshared-store warning is the one that matters most, because its symptom
// — every step succeeds, nothing it produced is readable — points nowhere near
// its cause.
func TestAFilesystemStoreWarnsThatNothingWillBeReadable(t *testing.T) {
	out := renderNotes(t)
	if !strings.Contains(out, "objectStore.kind=s3") {
		t.Errorf("a filesystem object store is not warned about:\n%s", out)
	}

	shared := renderNotes(t, "--set", "objectStore.kind=s3", "--set", "objectStore.s3.bucket=b")
	if strings.Contains(shared, "every process keeps its own object") {
		t.Errorf("an s3 deployment is warned about a problem it does not have:\n%s", shared)
	}
}

// A TCP probe cannot tell a working control plane from one whose database has
// gone: both answer a connection. The two probes ask different questions on
// purpose — readiness whether the plane can work, liveness only whether the
// process is alive — and conflating them makes a dependency outage into a
// crash-loop.
func TestTheProbesAskTheControlPlaneAQuestion(t *testing.T) {
	out := render(t)

	if strings.Contains(out, "tcpSocket") {
		t.Errorf("a probe still only checks the socket:\n%s", out)
	}
	for _, path := range []string{"path: /readyz", "path: /healthz"} {
		if !strings.Contains(out, path) {
			t.Errorf("no probe uses %s:\n%s", path, out)
		}
	}
}

// DHOLE_S3_SESSION_TOKEN is part of the object-store contract an engine author
// reads, and the chart could not set it — so a deployment on temporary
// credentials (STS, a federated role) had no way to pass one through Helm.
//
// It must be optional in the Secret as well as in the values: a session token
// is absent for a static key pair, and a required key that is missing fails
// the mount and stops the pod, which is worse than the token being unset.
func TestTheChartCanPassAnS3SessionToken(t *testing.T) {
	out := render(t,
		"--set", "objectStore.kind=s3",
		"--set", "objectStore.s3.bucket=b",
		"--set", "objectStore.s3.existingSecret=dhole-s3")

	if !strings.Contains(out, "DHOLE_S3_SESSION_TOKEN") {
		t.Fatalf("the chart cannot pass a session token:\n%s", out)
	}
	// Matched inside the session token's OWN block, not anywhere in the
	// render: there are four `optional: true` lines in a rendered chart, so
	// a bare Contains passed with the token's key still required. It did,
	// until this assertion was tightened.
	if !strings.Contains(out, "key: sessionToken") ||
		!regexp.MustCompile(`key: sessionToken\s+optional: true`).MatchString(out) {
		t.Error("the session token key is required, so a Secret without one stops the pod")
	}
	// And it reaches both the plane and its engines, like every other store
	// variable — half a deployment on temporary credentials is no deployment.
	if got := strings.Count(out, "DHOLE_S3_SESSION_TOKEN"); got < 2 {
		t.Errorf("the session token reaches %d containers, want the plane and its engines", got)
	}
}

// A model credential the control plane redeems for a `builtin:llm` step must
// reach the pod from a Secret, never from the values file: a key in a rendered
// manifest is a key in every `helm get manifest`, every GitOps repository and
// every `kubectl describe` a support ticket asks for.
func TestAModelCredentialComesFromASecretAndIsNamedOnTheCommandLine(t *testing.T) {
	out := render(t,
		"--set", "controlPlane.modelSecrets[0].name=ANTHROPIC_API_KEY",
		"--set", "controlPlane.modelSecrets[0].existingSecret=anthropic",
		"--set", "controlPlane.modelSecrets[0].key=apiKey")

	if !strings.Contains(out, "--model-secret=ANTHROPIC_API_KEY=DHOLE_MODEL_SECRET_ANTHROPIC_API_KEY") {
		t.Errorf("the plane was not told which secret to redeem:\n%s", out)
	}
	if !strings.Contains(out, "name: DHOLE_MODEL_SECRET_ANTHROPIC_API_KEY") ||
		!strings.Contains(out, "name: anthropic") {
		t.Errorf("the credential is not read from a Secret:\n%s", out)
	}
	if regexp.MustCompile(`DHOLE_MODEL_SECRET_ANTHROPIC_API_KEY\n\s+value:`).MatchString(out) {
		t.Errorf("a model credential was rendered as a literal value in the pod spec:\n%s", out)
	}
}

// A model secret that names no Kubernetes Secret to read is a plane that
// starts with a credential it cannot read and fails its first model call.
// It must fail at install instead.
func TestAModelSecretWithNoSecretToReadIsRefusedAtInstall(t *testing.T) {
	if _, err := renderErr(t, "--set", "controlPlane.modelSecrets[0].name=ANTHROPIC_API_KEY"); err == nil {
		t.Fatal("the chart rendered a model secret with nothing behind it")
	}
}

// A step secret is a plane-held secret a pipeline step may declare. It reaches
// the pod from a Secret exactly as a model credential does, and it is named on
// the command line FOR A TENANT: the flag carries the tenant, the name a step
// declares, and an environment variable the value is read from. The variable
// is positional so a secret name that is not a valid variable name (they are
// usually lower-case with dashes) still works.
func TestAStepSecretComesFromASecretAndIsNamedForATenant(t *testing.T) {
	out := render(t,
		"--set", "controlPlane.secrets[0].name=harbor-robot",
		"--set", "controlPlane.secrets[0].tenant=default",
		"--set", "controlPlane.secrets[0].existingSecret=harbor",
		"--set", "controlPlane.secrets[0].key=password")

	if !strings.Contains(out, "--secret=default/harbor-robot=DHOLE_STEP_SECRET_0") {
		t.Errorf("the plane was not told which step secret it holds, or for which tenant:\n%s", out)
	}
	if !regexp.MustCompile(`name: DHOLE_STEP_SECRET_0\n\s+valueFrom:\n\s+secretKeyRef:\n\s+name: harbor\n\s+key: password`).
		MatchString(out) {
		t.Errorf("the step secret is not read from the named Secret and key:\n%s", out)
	}
	if regexp.MustCompile(`DHOLE_STEP_SECRET_0\n\s+value:`).MatchString(out) {
		t.Errorf("a step secret was rendered as a literal value in the pod spec:\n%s", out)
	}
	// The model path is untouched by it.
	if strings.Contains(out, "--model-secret") {
		t.Errorf("a step secret was also handed to the plane as a model credential:\n%s", out)
	}
}

// A step secret that names no Secret, or no tenant, is refused at install:
// the first is a plane that starts with a value it cannot read, the second a
// secret scoped to nobody.
func TestAStepSecretWithNoSecretOrNoTenantIsRefusedAtInstall(t *testing.T) {
	if _, err := renderErr(t,
		"--set", "controlPlane.secrets[0].name=harbor-robot",
		"--set", "controlPlane.secrets[0].tenant=default",
		"--set", "controlPlane.secrets[0].key=password"); err == nil {
		t.Error("the chart rendered a step secret with no Secret behind it")
	}
	if _, err := renderErr(t,
		"--set", "controlPlane.secrets[0].name=harbor-robot",
		"--set", "controlPlane.secrets[0].existingSecret=harbor",
		"--set", "controlPlane.secrets[0].key=password"); err == nil {
		t.Error("the chart rendered a step secret scoped to no tenant")
	}
}

// A backend brings its own configuration — the vm backend needs a kernel, a
// rootfs and a hypervisor path — and a chart that named each backend's
// variables would need editing for every backend added later. A tier's own
// settings pass through instead.
func TestAnEngineTierCanCarryItsBackendsOwnSettings(t *testing.T) {
	out := render(t,
		"--set", "engines[0].name=vm",
		"--set", "engines[0].tier=trusted",
		"--set", "engines[0].executor=vm",
		"--set", "engines[0].env.DHOLE_VM_KERNEL=/vm/vmlinux",
		"--set", "engines[0].env.DHOLE_VM_ROOTFS=/vm/rootfs.cpio.gz")

	for _, want := range []string{"DHOLE_VM_KERNEL", "/vm/vmlinux", "DHOLE_VM_ROOTFS", "/vm/rootfs.cpio.gz"} {
		if !strings.Contains(out, want) {
			t.Errorf("a tier's own backend setting %q did not reach its engine:\n%s", want, out)
		}
	}
	// And selecting vm must not drag in the kubernetes backend's sandbox
	// variables, which mean nothing to it.
	if strings.Contains(out, "DHOLE_SANDBOX_NAMESPACE") {
		t.Error("a vm-backed tier was given the kubernetes backend's sandbox namespace")
	}
}

// An unsized sandbox pod takes the whole node: on kw a `go build` starved the
// engine beside it for 36 seconds, past its lease. A tier's sandbox sizing has
// to reach the engine that creates the pods, key for key.
func TestATiersSandboxResourcesReachItsEngine(t *testing.T) {
	out := render(t,
		"--set", "engines[0].name=build",
		"--set", "engines[0].tier=trusted",
		"--set", "engines[0].sandbox.resources.requests.cpu=500m",
		"--set", "engines[0].sandbox.resources.requests.memory=512Mi",
		"--set-string", "engines[0].sandbox.resources.limits.cpu=2",
		"--set", "engines[0].sandbox.resources.limits.memory=4Gi")

	for name, value := range map[string]string{
		"DHOLE_SANDBOX_CPU_REQUEST":    "500m",
		"DHOLE_SANDBOX_MEMORY_REQUEST": "512Mi",
		"DHOLE_SANDBOX_CPU_LIMIT":      "2",
		"DHOLE_SANDBOX_MEMORY_LIMIT":   "4Gi",
	} {
		want := "- name: " + name + "\n              value: \"" + value + "\""
		if !strings.Contains(out, want) {
			t.Errorf("the tier's sandbox sizing did not reach its engine as %s=%s:\n%s", name, value, out)
		}
	}
}

// Nothing configured must render nothing: an empty variable is still a
// variable, and the defaults must keep today's unsized pods rather than render
// a value the operator never chose.
func TestUnsizedSandboxesRenderNoResourceVariables(t *testing.T) {
	out := render(t, "--set", "engines[0].sandbox.resources.limits.memory=1Gi")
	require.Contains(t, out, "DHOLE_SANDBOX_MEMORY_LIMIT")
	for _, name := range []string{"DHOLE_SANDBOX_CPU_REQUEST", "DHOLE_SANDBOX_CPU_LIMIT", "DHOLE_SANDBOX_MEMORY_REQUEST"} {
		require.NotContains(t, out, name, "an unset key rendered a variable anyway")
	}

	require.NotContains(t, render(t), "DHOLE_SANDBOX_CPU_", "the default install sized sandboxes nobody asked to size")
}

// The engine's own CPU request is what keeps it renewing leases while its
// sandboxes work: the scheduler reserves it and a contended node shares CPU by
// it. A default engine with a token request is the one a build out-competes.
func TestTheDefaultEngineRequestsRealCPU(t *testing.T) {
	out := render(t)
	engine := out[strings.Index(out, "app.kubernetes.io/component: engine"):]
	m := regexp.MustCompile(`(?s)- name: engine\n.*?resources:\s*\n\s*limits:.*?\n\s*requests:\s*\n\s*cpu: (\S+)`).FindStringSubmatch(engine)
	require.NotNil(t, m, "the default engine container has no CPU request:\n%s", engine)
	require.Equal(t, "500m", m[1], "the default engine's CPU request changed; revisit the sizing rule in values.yaml")
}

// An operator's dispatch policy, written inline in the values, is rendered into
// a ConfigMap, mounted as a DIRECTORY and named on the command line (ADR 0032).
// A directory and not a subPath: Kubernetes rewrites a mounted ConfigMap in
// place only when it is mounted whole, and the plane re-reads the file.
func TestAnInlinePolicyIsRenderedIntoAConfigMapAndNamedOnTheCommandLine(t *testing.T) {
	out := render(t, "--set-string",
		"controlPlane.policy.rules=revision: ops/1\nrules:\n  - id: no-signing-key\n    expression: 'input.secret_name != \"release-signing-key\"'\n")

	if !strings.Contains(out, "--policy=/etc/dhole/policy/policy.yaml") {
		t.Errorf("the plane was not told where its policy is:\n%s", out)
	}
	if !regexp.MustCompile(`kind: ConfigMap\nmetadata:\n\s+name: dhole-policy`).MatchString(out) {
		t.Errorf("no ConfigMap holds the inline policy:\n%s", out)
	}
	if !strings.Contains(out, "id: no-signing-key") {
		t.Errorf("the ConfigMap does not carry the rules the values wrote:\n%s", out)
	}
	if !regexp.MustCompile(`mountPath: /etc/dhole/policy\n`).MatchString(out) ||
		!regexp.MustCompile(`configMap:\n\s+name: dhole-policy`).MatchString(out) {
		t.Errorf("the policy ConfigMap is not mounted whole where the flag points:\n%s", out)
	}
	if strings.Contains(out, "subPath: policy.yaml") {
		t.Errorf("the policy is mounted by subPath, which Kubernetes never updates:\n%s", out)
	}
}

// A ConfigMap the operator manages is mounted and named, under the key it
// says, and the chart renders no ConfigMap of its own.
func TestAnExistingPolicyConfigMapIsMountedAndNamedOnTheCommandLine(t *testing.T) {
	out := render(t,
		"--set", "controlPlane.policy.existingConfigMap=platform-policy",
		"--set", "controlPlane.policy.key=dispatch.yaml")

	if !strings.Contains(out, "--policy=/etc/dhole/policy/dispatch.yaml") {
		t.Errorf("the plane was not told which key holds its policy:\n%s", out)
	}
	if !regexp.MustCompile(`configMap:\n\s+name: platform-policy`).MatchString(out) {
		t.Errorf("the operator's ConfigMap is not mounted:\n%s", out)
	}
	if regexp.MustCompile(`kind: ConfigMap\nmetadata:\n\s+name: dhole-policy`).MatchString(out) {
		t.Errorf("the chart rendered its own policy ConfigMap beside the operator's:\n%s", out)
	}
	if _, err := renderErr(t,
		"--set", "controlPlane.policy.existingConfigMap=platform-policy",
		"--set-string", "controlPlane.policy.rules=revision: r\n"); err == nil {
		t.Error("the chart rendered two policies, and one of them would silently win")
	}
}

// With no policy values the plane runs the built-in default, and the chart
// says nothing about a policy at all.
func TestNoPolicyValuesRenderNoPolicyFlag(t *testing.T) {
	out := render(t)
	if strings.Contains(out, "--policy") || strings.Contains(out, "/etc/dhole/policy") {
		t.Errorf("a policy was rendered that nobody configured:\n%s", out)
	}
}
