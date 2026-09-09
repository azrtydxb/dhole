package git_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/trigger"
	gittrigger "github.com/azrtydxb/dhole/internal/trigger/git"
)

const (
	testPipelineID = "deploy"
	testTenant     = "acme"
	testSecret     = "s3cr3t-webhook-key"
)

// TestGitWebhookVerifiesSignatureAndRejectsForgery is the plan's case, and the
// one that decides whether this endpoint is a pipeline trigger or an
// unauthenticated remote execution primitive.
//
// The forgeries are chosen so that a check which is nearly right still fails:
// a signature of the right shape but the wrong key, a valid signature of a
// DIFFERENT body replayed onto this one, and this body's own signature
// replayed onto a tampered body.
func TestGitWebhookVerifiesSignatureAndRejectsForgery(t *testing.T) {
	sink := newRecordingSink()
	srv := serve(t, testConfig("push"), sink)

	body := githubPush
	res, out := post(t, srv.URL, body, githubHeaders(sign(testSecret, body)))
	require.Equal(t, nethttp.StatusAccepted, res, "a correctly signed payload was refused: %s", out)
	require.Equal(t, 1, sink.count())

	forgeries := map[string]struct{ body, signature string }{
		"signed with the wrong key": {body, sign("not-the-secret", body)},
		"a valid signature of another body": {
			body, sign(testSecret, `{"ref":"refs/heads/attacker"}`)},
		"this body's signature on a tampered body": {
			strings.Replace(body, "refs/heads/main", "refs/heads/evil", 1), sign(testSecret, body)},
		"a signature of the right length that is not one": {
			body, strings.Repeat("ab", sha256.Size)},
		"not hex at all": {body, "sha256=zzzz"},
	}
	for name, f := range forgeries {
		t.Run(name, func(t *testing.T) {
			before := sink.count()
			res, out := post(t, srv.URL, f.body, githubHeaders(f.signature))
			require.Equal(t, nethttp.StatusUnauthorized, res,
				"a forged payload was accepted: %s", out)
			require.Equal(t, before, sink.count(), "a forged payload started a run")
		})
	}
}

// TestGitWebhookRejectsAMissingSignature. Rejecting a WRONG signature is not
// the property; rejecting an ABSENT one is. An endpoint that verifies a
// signature when it finds one and fires when it does not has no signature
// check at all — the attacker simply omits the header.
func TestGitWebhookRejectsAMissingSignature(t *testing.T) {
	sink := newRecordingSink()
	srv := serve(t, testConfig("unsigned"), sink)

	unsigned := []struct {
		name    string
		headers map[string]string
	}{
		{"no signature header", map[string]string{"X-GitHub-Event": "push"}},
		{"an empty signature header", map[string]string{
			"X-GitHub-Event": "push", "X-Hub-Signature-256": ""}},
		{"the prefix and nothing else", map[string]string{
			"X-GitHub-Event": "push", "X-Hub-Signature-256": "sha256="}},
		{"an empty Gitea signature", map[string]string{
			"X-Gitea-Event": "push", "X-Gitea-Signature": ""}},
		{"an empty Forgejo signature", map[string]string{
			"X-Forgejo-Event": "push", "X-Forgejo-Signature": ""}},
	}
	for _, u := range unsigned {
		t.Run(u.name, func(t *testing.T) {
			res, out := post(t, srv.URL, githubPush, u.headers)
			require.Equal(t, nethttp.StatusUnauthorized, res,
				"an unsigned payload was accepted: %s", out)
			require.Equal(t, 0, sink.count(), "an unsigned payload started a run")
		})
	}
}

// TestSignatureComparisonIsConstantTime is a lint, and it is here because the
// functional tests above cannot see the difference: `==` on two byte slices
// rejects every forgery in this file and still leaks the expected MAC one byte
// at a time to anyone willing to measure.
//
// The rule is narrow and mechanical: inside the verification function, a
// signature may be compared only through crypto/hmac.Equal. Any other
// comparison of two computed values — including one made after decoding to a
// string, where Go's own string equality is not constant-time — fails here.
func TestSignatureComparisonIsConstantTime(t *testing.T) {
	const source = "git.go"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, source, nil, 0)
	require.NoError(t, err)

	var fn *ast.FuncDecl
	ast.Inspect(file, func(n ast.Node) bool {
		if d, ok := n.(*ast.FuncDecl); ok && d.Name.Name == "verifySignature" {
			fn = d
		}
		return true
	})
	require.NotNil(t, fn, "%s has no verifySignature function to check", source)

	var usesHMACEqual bool
	var offenders []string
	ast.Inspect(fn, func(n ast.Node) bool {
		switch e := n.(type) {
		case *ast.SelectorExpr:
			if id, ok := e.X.(*ast.Ident); ok && id.Name == "hmac" && e.Sel.Name == "Equal" {
				usesHMACEqual = true
			}
		case *ast.BinaryExpr:
			if e.Op != token.EQL && e.Op != token.NEQ {
				return true
			}
			// Comparing something to a literal — `sig == ""`, `err != nil` —
			// is a presence check, not a secret comparison.
			if isLiteral(e.X) || isLiteral(e.Y) {
				return true
			}
			offenders = append(offenders, fmt.Sprintf("line %d", fset.Position(e.Pos()).Line))
		}
		return true
	})

	require.True(t, usesHMACEqual,
		"verifySignature does not compare through hmac.Equal")
	require.Empty(t, offenders,
		"verifySignature compares two computed values directly (%s): "+
			"a signature must only ever be compared with hmac.Equal", strings.Join(offenders, ", "))
}

func isLiteral(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.BasicLit:
		return true
	case *ast.Ident:
		return v.Name == "nil"
	}
	return false
}

// TestGitWebhookParsesGitHubGiteaAndForgejoPayloads. Three flavours are
// claimed, so three flavours are tested, with the payload each forge actually
// sends: GitHub names the pusher `name`, Gitea and Forgejo name it `login`,
// and a single parser that reads only the fields they share would leave the
// bound input empty rather than fail loudly.
func TestGitWebhookParsesGitHubGiteaAndForgejoPayloads(t *testing.T) {
	cases := []struct {
		flavour string
		body    string
		headers func(signature string) map[string]string
		want    map[string]string
	}{
		{
			flavour: "github",
			body:    githubPush,
			headers: githubHeaders,
			want: map[string]string{
				"ref": "refs/heads/main", "branch": "main",
				"commit": "9a1b2c3d4e5f60718293a4b5c6d7e8f901234567",
				"repo":   "acme/widgets", "actor": "octocat",
			},
		},
		{
			flavour: "gitea",
			body:    giteaPush,
			headers: giteaHeaders,
			want: map[string]string{
				"ref": "refs/heads/release", "branch": "release",
				"commit": "c0ffee1122334455667788990011223344556677",
				"repo":   "acme/gadgets", "actor": "gitea-bot",
			},
		},
		{
			flavour: "forgejo",
			body:    forgejoPush,
			headers: forgejoHeaders,
			want: map[string]string{
				"ref": "refs/heads/trunk", "branch": "trunk",
				"commit": "fe3701aabbccddeeff00112233445566778899aa",
				"repo":   "acme/sprockets", "actor": "forgejo-bot",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.flavour, func(t *testing.T) {
			sink := newRecordingSink()
			srv := serve(t, testConfig(c.flavour), sink)

			res, out := post(t, srv.URL, c.body, c.headers(sign(testSecret, c.body)))
			require.Equal(t, nethttp.StatusAccepted, res,
				"a signed %s payload was refused: %s", c.flavour, out)
			require.Len(t, sink.fires(), 1)

			got := sink.fires()[0].inputs
			for input, want := range c.want {
				require.Contains(t, got, input)
				require.Equal(t, want, trigger.UntaintedValue(got[input]).GetStringValue(),
					"%s payload: input %q", c.flavour, input)
			}
			require.Equal(t, c.flavour,
				trigger.UntaintedValue(got["flavour"]).GetStringValue(),
				"the fired run does not say which forge sent the event")
		})
	}

	// Forgejo sends the Gitea headers too, for compatibility. It must still be
	// reported as Forgejo, or the flavour field is a coin toss.
	t.Run("forgejo with gitea compatibility headers", func(t *testing.T) {
		sink := newRecordingSink()
		srv := serve(t, testConfig("compat"), sink)

		headers := forgejoHeaders(sign(testSecret, forgejoPush))
		headers["X-Gitea-Event"] = "push"
		headers["X-Gitea-Signature"] = sign(testSecret, forgejoPush)

		res, out := post(t, srv.URL, forgejoPush, headers)
		require.Equal(t, nethttp.StatusAccepted, res, "%s", out)
		require.Equal(t, "forgejo",
			trigger.UntaintedValue(sink.fires()[0].inputs["flavour"]).GetStringValue())
	})

	t.Run("an unrecognised forge", func(t *testing.T) {
		sink := newRecordingSink()
		srv := serve(t, testConfig("unknown"), sink)

		res, out := post(t, srv.URL, githubPush, map[string]string{
			"X-Hub-Signature-256": sign(testSecret, githubPush)})
		require.Equal(t, nethttp.StatusBadRequest, res,
			"an event from no recognised forge was accepted: %s", out)
		require.Equal(t, 0, sink.count())
	})
}

// TestGitWebhookMarksPayloadTainted. A webhook body is attacker-controlled by
// definition, and ADR 0015 says data entering from an untrusted trigger is
// tainted AT THE BOUNDARY. This is that boundary: if the mark is not applied
// here there is nothing downstream for the taint check to propagate.
func TestGitWebhookMarksPayloadTainted(t *testing.T) {
	const id = "release-webhook"
	sink := newRecordingSink()
	srv := serve(t, testConfig(id), sink)

	res, out := post(t, srv.URL, githubPush, githubHeaders(sign(testSecret, githubPush)))
	require.Equal(t, nethttp.StatusAccepted, res, "%s", out)
	require.Len(t, sink.fires(), 1)

	inputs := sink.fires()[0].inputs
	require.NotEmpty(t, inputs)
	for name, v := range inputs {
		require.True(t, trigger.IsTainted(v),
			"input %q arrived from a git webhook unmarked: an untrusted payload "+
				"would reach an effectful step as if an operator had typed it", name)
		require.Contains(t, trigger.TaintSource(v), "git",
			"input %q carries a mark that does not name the trigger source", name)
		require.Contains(t, trigger.TaintSource(v), id,
			"input %q carries a mark naming no trigger: a taint that cannot be "+
				"traced back to the trigger that admitted it cannot be reasoned about", name)
	}

	// The mark is a wrapper, not a replacement: the value survives it.
	require.Equal(t, "refs/heads/main",
		trigger.UntaintedValue(inputs["ref"]).GetStringValue())
}

// TestGitWebhookChecksThePayloadAgainstTheDeclaredInputs. A verified payload
// is authentic, not correct: the forge signed whatever it sent, including a
// push with no pusher and no commit. The pipeline's ports say what a run needs
// to start, and a signed payload that cannot fill them is a 400 with the
// reason, not a run that dies in its first step (ADR 0007).
func TestGitWebhookChecksThePayloadAgainstTheDeclaredInputs(t *testing.T) {
	sink := newRecordingSink()
	srv := serve(t, testConfig("checked"), sink)

	// Signed, well-formed, and missing the fields three of the bound inputs
	// read: `commit` and `actor` would both arrive empty.
	body := `{"ref":"refs/heads/main","repository":{"full_name":"acme/widgets"}}`
	res, out := post(t, srv.URL, body, githubHeaders(sign(testSecret, body)))
	require.Equal(t, nethttp.StatusBadRequest, res,
		"a signed payload that cannot fill the pipeline's inputs was accepted: %s", out)
	require.Contains(t, out, "minLength",
		"the refusal does not carry the validation error: %s", out)
	require.Equal(t, 0, sink.count(),
		"a payload failing the pipeline's declared input schema started a run")
}

// TestGitWebhookBoundsTheBodyItReads. Same vector as the HTTP trigger, with
// one extra edge: the HMAC is computed over the body, so a handler that reads
// the whole thing before deciding anything has already lost.
func TestGitWebhookBoundsTheBodyItReads(t *testing.T) {
	cfg := testConfig("bounded")
	cfg.MaxBodyBytes = 2048
	sink := newRecordingSink()
	srv := serve(t, cfg, sink)

	big := oversized(8192)
	res, out := post(t, srv.URL, big, githubHeaders(sign(testSecret, big)))
	require.Equal(t, nethttp.StatusRequestEntityTooLarge, res,
		"a correctly signed but oversized body was read anyway: %s", out)
	require.Equal(t, 0, sink.count())

	res, _ = post(t, srv.URL, githubPush, githubHeaders(sign(testSecret, githubPush)))
	require.Equal(t, nethttp.StatusAccepted, res)

	// A trigger that names no limit still has one. This is the case that
	// matters in production, because nobody sets the field.
	require.Positive(t, gittrigger.DefaultMaxBodyBytes)
	unbounded := newRecordingSink()
	defaults := serve(t, testConfig("default-bounded"), unbounded)
	huge := oversized(int(gittrigger.DefaultMaxBodyBytes) + 1)
	res, out = post(t, defaults.URL, huge, githubHeaders(sign(testSecret, huge)))
	require.Equal(t, nethttp.StatusRequestEntityTooLarge, res,
		"a trigger configured with no explicit limit read an unbounded webhook body: %s", out)
	require.Equal(t, 0, unbounded.count())
}

// TestGitWebhookIgnoresEventsItWasNotConfiguredFor. A `push` trigger firing on
// every comment on every issue is a pipeline running a hundred times a day for
// no reason.
func TestGitWebhookIgnoresEventsItWasNotConfiguredFor(t *testing.T) {
	sink := newRecordingSink()
	srv := serve(t, testConfig("push-only"), sink)

	headers := githubHeaders(sign(testSecret, githubPush))
	headers["X-GitHub-Event"] = "issue_comment"
	res, out := post(t, srv.URL, githubPush, headers)
	require.Equal(t, nethttp.StatusNoContent, res, "%s", out)
	require.Equal(t, 0, sink.count(), "a trigger configured for `push` fired on an issue comment")
}

// TestGitWebhookConfigurationIsRefusedWithoutTenantOrSecret.
func TestGitWebhookConfigurationIsRefusedWithoutTenantOrSecret(t *testing.T) {
	cfg := testConfig("unscoped")
	cfg.TenantID = ""
	_, err := gittrigger.New(cfg)
	require.ErrorIs(t, err, trigger.ErrTenantRequired)
	require.Contains(t, err.Error(), "tenant scope required")

	cfg = testConfig("secretless")
	cfg.Secret = ""
	_, err = gittrigger.New(cfg)
	require.Error(t, err, "a webhook trigger with no secret was accepted: it can never verify anything")
	require.Contains(t, err.Error(), "secret")

	cfg = testConfig("misbound")
	cfg.Binding.InputMapping = map[string]string{"ref": "phase_of_the_moon"}
	_, err = gittrigger.New(cfg)
	require.Error(t, err, "a binding reading a field a git event does not carry was accepted")
	require.Contains(t, err.Error(), `"phase_of_the_moon"`)
}

// --- fixtures -------------------------------------------------------------

// The payloads are the shape each forge actually sends, trimmed to the fields
// a trigger reads. They differ on purpose: identical fixtures would prove
// nothing about three parsers.

const githubPush = `{
  "ref": "refs/heads/main",
  "before": "0000000000000000000000000000000000000000",
  "after": "9a1b2c3d4e5f60718293a4b5c6d7e8f901234567",
  "repository": {
    "full_name": "acme/widgets",
    "clone_url": "https://github.com/acme/widgets.git",
    "default_branch": "main"
  },
  "pusher": {"name": "octocat", "email": "octocat@example.com"},
  "head_commit": {"id": "9a1b2c3d4e5f60718293a4b5c6d7e8f901234567", "message": "ship it"},
  "sender": {"login": "octocat"}
}`

const giteaPush = `{
  "ref": "refs/heads/release",
  "before": "0000000000000000000000000000000000000000",
  "after": "c0ffee1122334455667788990011223344556677",
  "compare_url": "https://gitea.example/acme/gadgets/compare/a...b",
  "repository": {
    "full_name": "acme/gadgets",
    "clone_url": "https://gitea.example/acme/gadgets.git",
    "default_branch": "release"
  },
  "pusher": {"id": 7, "login": "gitea-bot", "full_name": "", "email": "bot@example.com"},
  "sender": {"id": 7, "login": "gitea-bot"}
}`

const forgejoPush = `{
  "ref": "refs/heads/trunk",
  "before": "0000000000000000000000000000000000000000",
  "after": "fe3701aabbccddeeff00112233445566778899aa",
  "compare_url": "https://forgejo.example/acme/sprockets/compare/a...b",
  "repository": {
    "full_name": "acme/sprockets",
    "clone_url": "https://forgejo.example/acme/sprockets.git",
    "default_branch": "trunk"
  },
  "pusher": {"id": 11, "login": "forgejo-bot", "full_name": "", "email": "bot@example.com"},
  "sender": {"id": 11, "login": "forgejo-bot"}
}`

func githubHeaders(signature string) map[string]string {
	return map[string]string{
		"X-GitHub-Event":      "push",
		"X-GitHub-Delivery":   "8b1f0f00-0000-4000-8000-000000000001",
		"X-Hub-Signature-256": prefixed(signature),
	}
}

func giteaHeaders(signature string) map[string]string {
	return map[string]string{
		"X-Gitea-Event":     "push",
		"X-Gitea-Delivery":  "8b1f0f00-0000-4000-8000-000000000002",
		"X-Gitea-Signature": signature,
	}
}

func forgejoHeaders(signature string) map[string]string {
	return map[string]string{
		"X-Forgejo-Event":     "push",
		"X-Forgejo-Delivery":  "8b1f0f00-0000-4000-8000-000000000003",
		"X-Forgejo-Signature": signature,
	}
}

// prefixed keeps GitHub's `sha256=` form where the test has not already
// written one in (the forgery cases pass raw values through).
func prefixed(signature string) string {
	if signature == "" || strings.Contains(signature, "=") {
		return signature
	}
	return "sha256=" + signature
}

func sign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return hex.EncodeToString(mac.Sum(nil))
}

func oversized(n int) string {
	var payload map[string]any
	if err := json.Unmarshal([]byte(githubPush), &payload); err != nil {
		panic(err)
	}
	payload["padding"] = strings.Repeat("a", n)
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return string(body)
}

// testPipeline is one pipeline declaring five free structured inputs and one
// blob input that no trigger may bind.
func testPipeline() *dholev1.Pipeline {
	structured := func(name string) *dholev1.Port {
		return &dholev1.Port{Name: name, Type: &dholev1.PortType{
			Kind: &dholev1.PortType_Structured{Structured: &dholev1.StructType{
				SchemaId: "https://dhole.dev/schema/" + name,
				Schema:   `{"type":"string","minLength":1}`,
			}},
		}}
	}
	return &dholev1.Pipeline{
		Id: testPipelineID,
		Steps: []*dholev1.Step{{
			Id: "build",
			Inputs: []*dholev1.Port{
				structured("ref"), structured("branch"), structured("commit"),
				structured("repo"), structured("actor"), structured("flavour"),
				{Name: "src", Type: &dholev1.PortType{
					Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
				}},
			},
		}},
	}
}

func testConfig(id string) gittrigger.Config {
	return gittrigger.Config{
		ID:       id,
		TenantID: testTenant,
		Secret:   testSecret,
		Binding: trigger.Binding{
			PipelineID: testPipelineID,
			InputMapping: map[string]string{
				"ref":     gittrigger.FieldRef,
				"branch":  gittrigger.FieldBranch,
				"commit":  gittrigger.FieldCommitSHA,
				"repo":    gittrigger.FieldRepository,
				"actor":   gittrigger.FieldPusher,
				"flavour": gittrigger.FieldFlavour,
			},
		},
		Pipeline: testPipeline(),
	}
}

func serve(t *testing.T, cfg gittrigger.Config, sink trigger.Sink) *httptest.Server {
	t.Helper()
	tr, err := gittrigger.New(cfg)
	require.NoError(t, err)
	require.Equal(t, "git", tr.Kind())
	srv := httptest.NewServer(tr.Handler(sink))
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url, body string, headers map[string]string) (int, string) {
	t.Helper()
	req, err := nethttp.NewRequestWithContext(
		context.Background(), nethttp.MethodPost, url, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = res.Body.Close() }()
	got, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return res.StatusCode, string(got)
}

type fire struct {
	tenantID   string
	pipelineID string
	inputs     map[string]*structpb.Value
}

type recordingSink struct {
	mu  sync.Mutex
	got []fire
}

func newRecordingSink() *recordingSink { return &recordingSink{} }

func (s *recordingSink) Fire(
	_ context.Context, tenantID, pipelineID string, inputs map[string]*structpb.Value,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, fire{tenantID: tenantID, pipelineID: pipelineID, inputs: inputs})
	return nil
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func (s *recordingSink) fires() []fire {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fire(nil), s.got...)
}
