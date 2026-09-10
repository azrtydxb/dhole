package secrets

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// planeHandleTTL is how long a handle the plane mints for ITSELF lives.
//
// It is short because the redemption is the very next thing that happens: the
// handle is issued, requested on the subject this process is already serving,
// and spent, all inside one model call's set-up. A minute would be a minute in
// which a leaked handle is a live credential for no benefit at all.
const planeHandleTTL = 30 * time.Second

// ErrNoSecret is what a Source reports when it holds no secret of that name
// for that tenant. It is distinguishable so a step's failure can say "nothing
// is configured" rather than handing a provider an empty string and reporting
// whatever authentication failure comes back — which names no secret at all.
var ErrNoSecret = errors.New("secrets: no secret of that name is configured for this tenant")

// ErrNoSource is what a plane with no backing secrets reports. It is the
// honest failure ADR 0024 asks for: a deployment that configured no credential
// fails the step with that reason, rather than pretending, and rather than
// dereferencing nil.
var ErrNoSource = errors.New(
	"secrets: this control plane was given no secret source, so it can resolve no secret of its own")

// Source is where the values BEHIND the plane's own handles come from.
//
// It is deliberately the narrowest thing that can answer "what is this
// tenant's secret called X": ADR 0024 leaves where a distributed deployment
// keeps those values as a separate decision, and an interface with one method
// is what lets that decision be made later without touching anything that
// redeems.
type Source interface {
	Value(ctx context.Context, tenantID, name string) (string, error)
}

// MapSource holds values in memory, scoped by tenant. It is what a single
// binary is given on the command line, and it is not a secret manager: the
// values live in this process for as long as it runs, which is exactly the
// property a deployment replaces by supplying its own Source.
type MapSource struct {
	mu     sync.Mutex
	values map[string]string
}

var _ Source = (*MapSource)(nil)

// NewMapSource returns an empty source.
func NewMapSource() *MapSource {
	return &MapSource{values: map[string]string{}}
}

// Set records one tenant's secret. There is no unscoped form: a value that
// belonged to every tenant would be the ambient credential this whole design
// refuses.
func (m *MapSource) Set(tenantID, name, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[tenantID+"\x00"+name] = value
}

// Value implements Source.
func (m *MapSource) Value(_ context.Context, tenantID, name string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, ok := m.values[tenantID+"\x00"+name]
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrNoSecret, name)
	}
	return value, nil
}

// PlaneResolver is the control plane's own way to a secret value, and it is
// the SAME way an engine's: mint a handle, redeem it over the endpoint this
// process serves, hold the value for the duration of one call.
//
// The round trip looks redundant — this process has the value in its hand
// before it issues the handle — and it is the point of ADR 0024. The
// alternative is a second path by which a credential reaches running code,
// exempt from the single-use rule, the expiry the issuer enforces, and the
// refusal that names neither handle nor value. One path is worth one local
// request against an in-memory broker.
//
// Nothing here caches. A provider's key rotates without restarting the plane,
// and a run that takes an hour does not hold a value for an hour.
type PlaneResolver struct {
	broker  *Broker
	source  Source
	req     Requester
	subject string
	ttl     time.Duration
}

// NewPlaneResolver returns the resolver a control plane holds. subject must be
// the one the broker is served on: a resolver pointed elsewhere redeems
// nothing, which is a start-up fault and reads as one.
func NewPlaneResolver(b *Broker, src Source, req Requester, subject string) *PlaneResolver {
	return &PlaneResolver{broker: b, source: src, req: req, subject: subject, ttl: planeHandleTTL}
}

// Resolve returns the value of tenantID's secret called name, for this call
// and no longer.
//
// The error path is written for what it will be pasted into: a JobStatus,
// which is durable and archived. It names the SECRET the step asked for and
// never the value and never the handle.
func (p *PlaneResolver) Resolve(ctx context.Context, tenantID, name string) (string, error) {
	switch {
	case p == nil || p.broker == nil || p.source == nil || p.req == nil || p.subject == "":
		return "", fmt.Errorf("%w: %q", ErrNoSource, name)
	case tenantID == "":
		// No unscoped resolution, even while only one tenant exists. A model
		// credential is a tenant's, and a lookup that dropped the tenant would
		// have to be redesigned the day one deployment wants two.
		return "", errors.New("secrets: a tenant is required to resolve a secret")
	case name == "":
		return "", errors.New("secrets: a name is required to resolve a secret")
	}

	value, err := p.source.Value(ctx, tenantID, name)
	if err != nil {
		return "", fmt.Errorf("secrets: the secret named %q: %w", name, err)
	}
	// Issued to the STEP's tenant, so the broker's own scoping is the same
	// scoping the source was asked under.
	ref, err := p.broker.Issue(tenantID, name, value, p.ttl)
	if err != nil {
		return "", fmt.Errorf("secrets: issuing a handle for the secret named %q: %w", name, err)
	}
	return NewBusRedeemer(p.req, p.subject).Redeem(ctx, ref)
}
