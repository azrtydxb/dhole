package catalog

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// Kind says what sort of type a manifest describes. The values are stored
// verbatim, so they are a persistence contract: add new ones, never rename an
// existing one.
type Kind string

// The kinds the catalog holds.
const (
	KindStep    Kind = "step"
	KindTrigger Kind = "trigger"
	KindEngine  Kind = "engine"
)

// Manifest is what a type declares about itself: its identity, the digest it
// resolves to, the effect class it operates under (ADR 0002), the capabilities
// it needs, its input and output schemas, and the engine types that can run it.
//
// Digest is a pointer because every generated protobuf message embeds a mutex;
// passing one by value trips go vet's copylocks check.
type Manifest struct {
	Namespace    string
	Name         string
	Version      string
	Digest       *dholev1.Digest
	Kind         Kind
	EffectClass  dholev1.EffectClass
	Capabilities []dholev1.Capability
	InputSchema  []byte
	OutputSchema []byte
	EngineTypes  []string
}

// Ref is the manifest's canonical reference, the form every other subsystem
// names it by.
func (m Manifest) Ref() string {
	return m.Namespace + "/" + m.Name + "@" + m.Version
}

// Entry is a published manifest as the catalog hands it back: the declaration
// itself, plus anything the caller ought to know about how it was arrived at.
//
// OverrideWarnings is empty for a plain Resolve. It is populated by ResolveStep
// when a step's own declaration disagrees with its plugin's in the dangerous
// direction — see widenWarning.
type Entry struct {
	Manifest
	OverrideWarnings []string
}

// ErrMalformedRef is returned for a reference that does not parse. It is
// deliberately distinct from ErrNotFound: "you typed it wrong" and "it is not
// published" are different problems with different fixes.
var ErrMalformedRef = errors.New("malformed catalog ref")

// refForm is the only accepted shape, and is quoted back in every parse error
// so the caller is told what would have worked.
const refForm = "namespace/name@version"

// parseRef splits a reference into its three parts. A version is mandatory:
// dispatch resolves to an exact version and digest (ADR 0010), so a floating
// reference has no meaning here, and silently picking "the latest" would make
// the lockfile a suggestion.
func parseRef(ref string) (namespace, name, version string, err error) {
	bad := func() (string, string, string, error) {
		return "", "", "", fmt.Errorf("%w %q: expected %q", ErrMalformedRef, ref, refForm)
	}

	at := strings.LastIndex(ref, "@")
	if at < 0 {
		return bad()
	}
	path, version := ref[:at], ref[at+1:]
	slash := strings.Index(path, "/")
	if slash < 0 || strings.Contains(path[slash+1:], "/") {
		return bad()
	}
	namespace, name = path[:slash], path[slash+1:]
	if namespace == "" || name == "" || version == "" {
		return bad()
	}
	return namespace, name, version, nil
}

// validate refuses a manifest that could not be acted on later. The catalog is
// durable and everything downstream trusts it, so this is the last point at
// which a half-declared type can be caught by the system rather than by a
// scheduler guessing at run time.
func (m Manifest) validate() error {
	switch {
	case m.Namespace == "":
		return errors.New("catalog: manifest namespace is required")
	case m.Name == "":
		return errors.New("catalog: manifest name is required")
	case m.Version == "":
		return errors.New("catalog: manifest version is required")
	case m.Digest.GetAlgo() == "" || m.Digest.GetHex() == "":
		return errors.New("catalog: manifest digest is required")
	}
	switch m.Kind {
	case KindStep, KindTrigger, KindEngine:
	default:
		return fmt.Errorf("catalog: unknown manifest kind %q: expected one of %q, %q, %q",
			string(m.Kind), KindStep, KindTrigger, KindEngine)
	}
	// ADR 0002 makes the effect class mandatory rather than defaulted: an
	// unspecified value would have to mean something, and every choice of
	// meaning is wrong for one of the two populations the engine hosts.
	if m.EffectClass == dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED {
		return fmt.Errorf("catalog: %s: effect class is required", m.Ref())
	}
	for _, c := range m.Capabilities {
		if c == dholev1.Capability_CAPABILITY_UNSPECIFIED {
			return fmt.Errorf("catalog: %s: unspecified capability declared", m.Ref())
		}
	}
	if err := validateSchema("input schema", m.InputSchema); err != nil {
		return fmt.Errorf("catalog: %s: %w", m.Ref(), err)
	}
	if err := validateSchema("output schema", m.OutputSchema); err != nil {
		return fmt.Errorf("catalog: %s: %w", m.Ref(), err)
	}
	return nil
}

// validateSchema compiles one declared JSON Schema document. The label names
// which of the two failed and is part of the error text on purpose: a publisher
// holding an input and an output document must not have to guess which one the
// compiler choked on.
func validateSchema(label string, doc []byte) error {
	if len(bytes.TrimSpace(doc)) == 0 {
		return fmt.Errorf("%s is required", label)
	}
	parsed, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return fmt.Errorf("%s is not valid JSON: %w", label, err)
	}
	c := jsonschema.NewCompiler()
	// Compiling offline: a manifest must not be able to make the control plane
	// fetch a URL at publish time.
	c.UseLoader(offlineLoader{})
	const resource = "dhole:manifest-schema"
	if err := c.AddResource(resource, parsed); err != nil {
		return fmt.Errorf("%s is not a valid JSON Schema: %w", label, err)
	}
	if _, err := c.Compile(resource); err != nil {
		return fmt.Errorf("%s is not a valid JSON Schema: %w", label, err)
	}
	return nil
}

// offlineLoader refuses every remote reference. A published manifest is
// durable and shared; letting one name an external $ref would make validation
// depend on somebody else's uptime and turn publishing into an outbound
// request.
type offlineLoader struct{}

func (offlineLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("remote schema reference %q is not allowed", url)
}

// effectRank orders the effect classes from weakest guarantee to strongest.
// The order is ADR 0002's: `pure` may be cached and retried freely,
// `idempotent` may be retried but never cached, `at-most-once` may be neither.
func effectRank(c dholev1.EffectClass) int {
	switch c {
	case dholev1.EffectClass_EFFECT_CLASS_PURE:
		return 1
	case dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT:
		return 2
	case dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE:
		return 3
	case dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED:
		return 0
	default:
		return 0
	}
}

// widenWarning reports an override that claims a WEAKER effect class than the
// plugin declares, and says nothing about the opposite.
//
// The asymmetry is deliberate. Narrowing — a step insisting on at-most-once
// over a pure plugin — only forgoes the cache and the automatic retry; being
// more careful than the plugin author asked for cannot break anything.
// Widening is the dangerous direction: a step claiming `pure` over a plugin
// that declared `at-most-once` makes an unrepeatable action content-addressed,
// cacheable and freely retried, which is how a notification gets sent twice or
// a card gets charged twice. The override is still honoured — the step author
// may genuinely know the plugin is over-declared — but it never happens
// quietly.
func widenWarning(stepID string, declared, plugin dholev1.EffectClass, pluginRef string) string {
	return fmt.Sprintf(
		"step %q widens effect class to %s over %s declared by %s: "+
			"the step becomes cacheable and automatically retried",
		stepID, declared, plugin, pluginRef)
}
