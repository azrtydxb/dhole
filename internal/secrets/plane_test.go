package secrets_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/secrets"
)

// planeSecret is the value every case in this file resolves. It is long and
// unlikely so that "does this byte sequence appear anywhere it should not" is
// a question with one honest answer.
const planeSecret = "correcthorsebatterystaple"

// recordingRequester is the plane's own bus connection with a notebook. It
// records the handles that actually crossed the redemption subject, which is
// how a case tells "the plane went through the broker it serves" apart from
// "the plane read the value out of the source and called it a redemption".
type recordingRequester struct {
	inner   secrets.Requester
	mu      sync.Mutex
	handles []string
}

func (r *recordingRequester) RequestRaw(ctx context.Context, subject string, body []byte) ([]byte, error) {
	r.mu.Lock()
	r.handles = append(r.handles, string(body))
	r.mu.Unlock()
	return r.inner.RequestRaw(ctx, subject, body)
}

func (r *recordingRequester) redeemed() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.handles...)
}

// countingSource answers with a different value each time, and remembers who
// asked. Rotation is the property: a plane that cached would go on handing out
// the first value for ever.
type countingSource struct {
	mu      sync.Mutex
	values  []string
	tenants []string
	names   []string
	err     error
}

func (s *countingSource) Value(_ context.Context, tenantID, name string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants = append(s.tenants, tenantID)
	s.names = append(s.names, name)
	if s.err != nil {
		return "", s.err
	}
	value := s.values[0]
	if len(s.values) > 1 {
		s.values = s.values[1:]
	}
	return value, nil
}

func (s *countingSource) asked() ([]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.tenants...), append([]string{}, s.names...)
}

// planeBroker stands a served broker up on a real bus and returns the resolver
// a control plane would hold, along with the notebook of what crossed the wire.
func planeBroker(
	ctx context.Context, t *testing.T, source secrets.Source,
) (*secrets.PlaneResolver, *recordingRequester) {
	t.Helper()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)

	broker := secrets.NewBroker()
	stop, err := secrets.Serve(ctx, plane, broker, bus.SubjectSecretRedeem())
	require.NoError(t, err)
	t.Cleanup(stop)

	req := &recordingRequester{inner: plane}
	return secrets.NewPlaneResolver(broker, source, req, bus.SubjectSecretRedeem()), req
}

// TestThePlaneRedeemsItsOwnSecretThroughTheBrokerItServesRatherThanReadingTheValueDirectly
// is ADR 0024's decision in one assertion. The plane could reach into the
// source it was handed and be done; then there would be two ways a credential
// reaches a running thing in Dhole and only one of them single-use, expiring
// and auditable. It mints a handle and redeems it over the endpoint it serves,
// exactly as an engine does.
func TestThePlaneRedeemsItsOwnSecretThroughTheBrokerItServesRatherThanReadingTheValueDirectly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resolver, wire := planeBroker(ctx, t, &countingSource{values: []string{planeSecret}})

	value, err := resolver.Resolve(ctx, "t1", "ANTHROPIC_API_KEY")
	require.NoError(t, err)
	require.Equal(t, planeSecret, value)

	handles := wire.redeemed()
	require.Len(t, handles, 1, "the value must have crossed the redemption subject, not a back door")
	require.NotEmpty(t, handles[0])
	require.NotContains(t, handles[0], planeSecret, "a handle is not the value")
}

// TestThePlaneRedeemsOncePerCallSoAProviderKeyRotatesWithoutARestart is the
// consequence ADR 0024 names: a value held for the lifetime of a deployment is
// the secret at rest that SecretRef exists to avoid, and a run that takes an
// hour must not hold a credential for an hour.
//
// Two calls, two redemptions, two values — and the second call sees the
// rotated key without anything having been restarted.
func TestThePlaneRedeemsOncePerCallSoAProviderKeyRotatesWithoutARestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	source := &countingSource{values: []string{planeSecret, "rotated-" + planeSecret}}
	resolver, wire := planeBroker(ctx, t, source)

	first, err := resolver.Resolve(ctx, "t1", "ANTHROPIC_API_KEY")
	require.NoError(t, err)
	second, err := resolver.Resolve(ctx, "t1", "ANTHROPIC_API_KEY")
	require.NoError(t, err)

	require.Equal(t, planeSecret, first)
	require.Equal(t, "rotated-"+planeSecret, second,
		"the second call reused a cached value instead of redeeming again")

	handles := wire.redeemed()
	require.Len(t, handles, 2, "each call redeems its own handle")
	require.NotEqual(t, handles[0], handles[1], "a handle is single-use, so a second call cannot reuse the first's")
}

// TestAPlaneResolutionIsScopedToTheTenantWhoseStepIsRunning holds the last of
// ADR 0024's consequences. Nothing yet configures a credential per tenant, and
// the resolution carries the tenant anyway: a resolver that dropped it would
// have to be redesigned the day one deployment wants two tenants' keys, and
// would meanwhile be the unscoped lookup this system does not have.
func TestAPlaneResolutionIsScopedToTheTenantWhoseStepIsRunning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	source := &countingSource{values: []string{planeSecret}}
	resolver, _ := planeBroker(ctx, t, source)

	_, err := resolver.Resolve(ctx, "tenant-a", "ANTHROPIC_API_KEY")
	require.NoError(t, err)
	_, err = resolver.Resolve(ctx, "tenant-b", "ANTHROPIC_API_KEY")
	require.NoError(t, err)

	tenants, names := source.asked()
	require.Equal(t, []string{"tenant-a", "tenant-b"}, tenants)
	require.Equal(t, []string{"ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"}, names)

	_, err = resolver.Resolve(ctx, "", "ANTHROPIC_API_KEY")
	require.Error(t, err, "there is no unscoped resolution, even while only one tenant exists")
}

// TestAPlaneWithNoSecretSourceNamesTheSecretItCouldNotResolveAndNothingElse is
// the honest failure. A deployment that configured no credential fails the
// step with a reason an operator can act on — the NAME it was asked for — and
// never with a nil dereference and never by quietly carrying on.
func TestAPlaneWithNoSecretSourceNamesTheSecretItCouldNotResolveAndNothingElse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resolver, _ := planeBroker(ctx, t, nil)

	_, err := resolver.Resolve(ctx, "t1", "ANTHROPIC_API_KEY")
	require.Error(t, err)
	require.ErrorIs(t, err, secrets.ErrNoSource)
	require.Contains(t, err.Error(), "ANTHROPIC_API_KEY")

	// And the same from a resolver nobody built at all, because a nil
	// dereference is not a named reason.
	var none *secrets.PlaneResolver
	_, err = none.Resolve(ctx, "t1", "ANTHROPIC_API_KEY")
	require.ErrorIs(t, err, secrets.ErrNoSource)
}

// TestAFailedPlaneResolutionNamesNeitherTheValueNorTheHandle is the property
// this whole design exists to protect. The error goes into a JobStatus, which
// is durable and archived: a model API key in one is a credential at rest in
// the run history.
func TestAFailedPlaneResolutionNamesNeitherTheValueNorTheHandle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A broker that is NOT served: the request goes nowhere, which is the
	// shape of every failure on this path — a plane still starting, a bus that
	// dropped, a handle already spent.
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	plane, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)

	wire := &recordingRequester{inner: plane}
	resolver := secrets.NewPlaneResolver(
		secrets.NewBroker(), &countingSource{values: []string{planeSecret}}, wire, bus.SubjectSecretRedeem())

	_, err = resolver.Resolve(ctx, "t1", "ANTHROPIC_API_KEY")
	require.Error(t, err)
	require.NotContains(t, err.Error(), planeSecret)
	require.NotContains(t, strings.ToLower(err.Error()), "correcthorse")
	for _, handle := range wire.redeemed() {
		require.NotContains(t, err.Error(), handle)
	}
}

// TestAMapSourceHoldsAValuePerTenantAndRefusesAnyOtherName is the development
// source: what a single binary is given on the command line. It refuses by
// NAME rather than answering empty, because an empty API key reaches the
// provider and comes back as an authentication failure with nothing in it
// about which secret was missing.
func TestAMapSourceHoldsAValuePerTenantAndRefusesAnyOtherName(t *testing.T) {
	source := secrets.NewMapSource()
	source.Set("t1", "ANTHROPIC_API_KEY", planeSecret)

	value, err := source.Value(context.Background(), "t1", "ANTHROPIC_API_KEY")
	require.NoError(t, err)
	require.Equal(t, planeSecret, value)

	_, err = source.Value(context.Background(), "t1", "OPENAI_API_KEY")
	require.ErrorIs(t, err, secrets.ErrNoSecret)
	require.Contains(t, err.Error(), "OPENAI_API_KEY")

	_, err = source.Value(context.Background(), "t2", "ANTHROPIC_API_KEY")
	require.ErrorIs(t, err, secrets.ErrNoSecret,
		"one tenant's credential is not another's")
}
