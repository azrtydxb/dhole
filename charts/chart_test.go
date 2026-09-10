// Package charts_test renders the Helm chart and asserts the things a
// rendered manifest can be wrong about while `helm lint` stays happy.
//
// helm lint checks that a chart is well-formed YAML with the fields Kubernetes
// wants. It cannot know that NATS reads a size differently from Kubernetes, so
// a chart that lints, templates and applies can still crash-loop on first
// boot — which is exactly what happened.
package charts_test

import (
	"os/exec"
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
	out, err := exec.Command("helm",
		append([]string{"install", "dhole", "./dhole", "--dry-run"}, args...)...).CombinedOutput()
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
