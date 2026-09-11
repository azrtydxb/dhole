package tenant_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/tenant"
)

// TestTenantIsolationAcrossStoreAndBus is the system-level statement the
// per-package tenant checks cannot make on their own: with tenant `a` holding
// a run, a blob and a live bus connection, tenant `b` reaches none of them.
//
// Each package already refuses an empty tenant. That is not the same property.
// This test asks whether a VALID but DIFFERENT tenant is kept out, which is
// what an actual cross-tenant read looks like — no caller ever passes "".
func TestTenantIsolationAcrossStoreAndBus(t *testing.T) {
	ctx := context.Background()
	const (
		tenantA = "acme"
		tenantB = "globex"
		runID   = "run-1"
	)

	// The run store: b cannot replay a's run.
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	require.NoError(t, store.Append(ctx, tenantA, runstore.Event{
		RunID: runID, StepID: "build", Attempt: 1, Sequence: 1,
		Type: runstore.RunCreated, At: time.Unix(1, 0).UTC(),
	}))

	own, err := store.Replay(ctx, tenantA, runID)
	require.NoError(t, err)
	require.Len(t, own, 1, "the run must be readable by the tenant that wrote it")

	stolen, err := store.Replay(ctx, tenantB, runID)
	require.NoError(t, err)
	require.Empty(t, stolen, "tenant %q replayed tenant %q's run", tenantB, tenantA)

	seq, err := store.LastSequence(ctx, tenantB)
	require.NoError(t, err)
	require.Zero(t, seq, "tenant %q sees tenant %q's sequence", tenantB, tenantA)

	// The CAS: b cannot read a's blob, even holding the digest.
	blobs := cas.NewFilesystem(t.TempDir())
	digest, err := blobs.Put(ctx, tenantA, strings.NewReader("secret artifact"))
	require.NoError(t, err)

	rc, err := blobs.Get(ctx, tenantA, digest)
	require.NoError(t, err, "the blob must be readable by the tenant that wrote it")
	require.NoError(t, rc.Close())

	_, err = blobs.Get(ctx, tenantB, digest)
	require.ErrorIs(t, err, cas.ErrNotFound, "tenant %q read tenant %q's blob", tenantB, tenantA)

	has, err := blobs.Has(ctx, tenantB, digest)
	require.NoError(t, err)
	require.False(t, has, "tenant %q sees tenant %q's blob", tenantB, tenantA)

	// The bus: b cannot receive what is published on a's dispatch subject.
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	// An id that could forge a subject or an account name never reaches the
	// server. Refusing it in Validate is not enough on its own: the guard has
	// to be on the path that actually provisions, or a caller that skipped
	// validation would still get an account.
	for _, bad := range []string{"", "a>", "a*", "a.b", "../etc"} {
		_, err := tenant.ProvisionAccount(ctx, srv, bad)
		require.Error(t, err, "ProvisionAccount accepted the tenant id %q", bad)
		require.ErrorIs(t, err, tenant.ErrInvalidTenant)
	}

	credsA, err := tenant.ProvisionAccount(ctx, srv, tenantA)
	require.NoError(t, err)
	credsB, err := tenant.ProvisionAccount(ctx, srv, tenantB)
	require.NoError(t, err)
	require.NotEqual(t, credsA, credsB, "both tenants were given the same credentials")

	connA := dialTenant(t, credsA)
	watcherA := dialTenant(t, credsA)
	connB := dialTenant(t, credsB)

	subject := bus.SubjectDispatch("trusted", "capsdeadbeef")

	// The control: the same publish IS delivered inside a's own account.
	// Without it a broken publish would make the isolation check vacuous.
	inA := make(chan []byte, 1)
	subA, err := watcherA.Subscribe(subject, func(m *nats.Msg) { inA <- m.Data })
	require.NoError(t, err)
	t.Cleanup(func() { _ = subA.Unsubscribe() })

	inB := make(chan []byte, 1)
	subB, err := connB.Subscribe(subject, func(m *nats.Msg) { inB <- m.Data })
	require.NoError(t, err)
	t.Cleanup(func() { _ = subB.Unsubscribe() })

	require.NoError(t, connA.Flush())
	require.NoError(t, connB.Flush())
	require.NoError(t, watcherA.Flush())

	require.NoError(t, connA.Publish(subject, []byte("dispatch for acme")))
	require.NoError(t, connA.Flush())

	select {
	case got := <-inA:
		require.Equal(t, "dispatch for acme", string(got))
	case <-time.After(3 * time.Second):
		t.Fatal("the publishing tenant did not receive its own dispatch: the test proves nothing")
	}

	select {
	case got := <-inB:
		t.Fatalf("tenant %q received tenant %q's dispatch: %q", tenantB, tenantA, got)
	case <-time.After(300 * time.Millisecond):
	}

	// And b's credentials do not reach outside the tenant subject space at
	// all: a wildcard subscription is refused by the server, not by the
	// control plane's good manners.
	requirePermissionRefused(t, credsB, ">")
	requirePermissionRefused(t, credsB, "$SYS.>")
}

// TestFromContextRefusesAnUnscopedContext pins the rule that matters most in
// this package: a context with no tenant is an ERROR, never the empty string
// with a nil error. Returning "" would hand every store a value it must then
// reject, and any store that forgot would run an unscoped query.
func TestFromContextRefusesAnUnscopedContext(t *testing.T) {
	id, err := tenant.FromContext(context.Background())
	require.Error(t, err)
	require.Empty(t, id)
	require.ErrorIs(t, err, tenant.ErrNoTenant)
	require.Contains(t, err.Error(), "tenant scope required")

	id, err = tenant.FromContext(tenant.WithTenant(context.Background(), "acme"))
	require.NoError(t, err)
	require.Equal(t, "acme", id)
}

// TestInjectableTenantIDsAreRefused covers the ids that would turn a tenant
// scope into a wildcard: a `>` or `*` in a subject segment subscribes to
// everything, and a separator in an account name lets one tenant name
// another's account.
func TestInjectableTenantIDsAreRefused(t *testing.T) {
	for _, id := range []string{
		"", " ", "a b", "a>", ">", "*", "a*", "a.b", ".", "..", "a/b", `a\b`,
		"a\x00b", "A-CME", strings.Repeat("a", 200),
	} {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			require.Error(t, tenant.Validate(id), "id %q was accepted", id)
			require.Empty(t, tenant.AccountName(id),
				"AccountName produced a usable account name for %q", id)

			_, err := tenant.FromContext(tenant.WithTenant(context.Background(), id))
			require.Error(t, err, "FromContext returned %q as a usable tenant", id)
		})
	}

	require.NoError(t, tenant.Validate("acme"))
	require.NoError(t, tenant.Validate("acme-2_x"))
	require.NotEmpty(t, tenant.AccountName("acme"))
	require.NotEqual(t, tenant.AccountName("acme"), tenant.AccountName("globex"))
}

// storeUnderSweep is one interface the sweep below walks.
type storeUnderSweep struct {
	name string
	// iface is the interface type whose FULL method set is swept. Driving off
	// the interface rather than a hand-written list is the whole point: a
	// method added to the interface tomorrow is covered without anyone
	// remembering to extend this test.
	iface reflect.Type
	impl  any
	// sentinel is the error the package documents for a refused tenant.
	sentinel error
	// skip names methods that genuinely cannot take a tenant, with the reason.
	// Skipping is BY NAME and visible here — never by a silent filter that
	// would also swallow a method that simply forgot its guard.
	skip map[string]string
}

// TestNoStoreMethodAcceptsEmptyTenant sweeps every exported method of
// runstore.Store, cas.Store and blobstore.Store BY REFLECTION and requires
// each to refuse an empty tenant.
//
// This is the test the individual packages cannot write. Each of them checks
// its own methods; none of them notices when a fourth method is added to a
// third interface without a guard. The sweep fails on the method it has never
// heard of, which is exactly the hole this task exists to close.
func TestNoStoreMethodAcceptsEmptyTenant(t *testing.T) {
	runs, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	sweepInterface(t, storeUnderSweep{
		name:     "runstore.Store",
		iface:    reflect.TypeOf((*runstore.Store)(nil)).Elem(),
		impl:     runs,
		sentinel: runstore.ErrTenantRequired,
		skip: map[string]string{
			// WithTx opens a transaction and hands it to the caller; the
			// tenant is supplied per statement inside it, and Tx.Append is
			// swept below in its own right.
			"WithTx": "takes no tenant: the tenant is passed to Tx.Append inside the transaction",
			// Close releases the store's connections. It touches no tenant data.
			"Close": "takes no tenant: releases the store's own resources",
		},
	})

	sweepInterface(t, storeUnderSweep{
		name:     "cas.Store",
		iface:    reflect.TypeOf((*cas.Store)(nil)).Elem(),
		impl:     cas.NewFilesystem(t.TempDir()),
		sentinel: cas.ErrInvalidTenant,
		skip:     map[string]string{},
	})

	sweepInterface(t, storeUnderSweep{
		name:     "blobstore.Store",
		iface:    reflect.TypeOf((*blobstore.Store)(nil)).Elem(),
		impl:     blobstore.NewFilesystem(t.TempDir()),
		sentinel: blobstore.ErrInvalidKey,
		skip:     map[string]string{},
	})

	// runstore.Tx is swept INSIDE its own transaction, on its own database.
	// Borrowing an open transaction and sweeping the rest of the store
	// alongside it deadlocks on SQLite's write lock the moment a method loses
	// its guard and actually reaches the database — which is a hang instead of
	// a failure, and a hang is not a test result.
	txStore, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "tx.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = txStore.Close() })

	rollback := errors.New("rollback: this transaction only existed to be swept")
	err = txStore.WithTx(context.Background(), func(tx runstore.Tx) error {
		sweepInterface(t, storeUnderSweep{
			name:     "runstore.Tx",
			iface:    reflect.TypeOf((*runstore.Tx)(nil)).Elem(),
			impl:     tx,
			sentinel: runstore.ErrTenantRequired,
			skip: map[string]string{
				// Exec, Query and Dialect are the documented escape hatch for
				// tables that are not the event log. They take raw SQL, so the
				// tenant lives in the caller's statement and this sweep cannot
				// see it.
				"Exec":    "takes raw SQL: the tenant column is in the caller's own statement",
				"Query":   "takes raw SQL: the tenant column is in the caller's own statement",
				"Dialect": "returns a constant: touches no tenant data",
			},
		})
		return rollback
	})
	require.ErrorIs(t, err, rollback)
}

// sweepInterface walks one interface's whole method set. It is driven by the
// interface type, never by a list, so a method added tomorrow is swept without
// anyone remembering this file exists.
func sweepInterface(t *testing.T, sweep storeUnderSweep) {
	t.Helper()
	swept := 0
	for i := range sweep.iface.NumMethod() {
		method := sweep.iface.Method(i)
		if reason, skipped := sweep.skip[method.Name]; skipped {
			require.NotEmpty(t, reason, "%s.%s is skipped with no reason", sweep.name, method.Name)
			continue
		}
		t.Run(sweep.name+"."+method.Name, func(t *testing.T) {
			err := callWithEmptyTenant(t, sweep.impl, method.Name)
			require.Error(t, err,
				"%s.%s accepted an empty tenant", sweep.name, method.Name)
			require.ErrorIs(t, err, sweep.sentinel,
				"%s.%s refused an empty tenant with the wrong error: %v",
				sweep.name, method.Name, err)
			require.Contains(t, strings.ToLower(err.Error()), "tenant",
				"%s.%s refused an empty tenant with an error that does not name the tenant: %v",
				sweep.name, method.Name, err)
		})
		swept++
	}
	require.NotZero(t, swept, "%s: the sweep covered no method at all", sweep.name)
}

// callWithEmptyTenant invokes one method with an empty tenant and plausible,
// NON-empty values for everything else, so the only thing under test is the
// tenant. It returns the method's error result.
func callWithEmptyTenant(t *testing.T, impl any, name string) error {
	t.Helper()
	value := reflect.ValueOf(impl).MethodByName(name)
	require.True(t, value.IsValid(), "%T has no method %s", impl, name)
	typ := value.Type()

	tenantSeen := false
	args := make([]reflect.Value, typ.NumIn())
	for i := range typ.NumIn() {
		in := typ.In(i)
		switch {
		case in == reflect.TypeOf((*context.Context)(nil)).Elem():
			// A bounded context, so a method that lost its guard and went to
			// the database comes back with an error instead of blocking on a
			// lock. A hang is not a test result.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			t.Cleanup(cancel)
			args[i] = reflect.ValueOf(ctx)
		case in.Kind() == reflect.String && !tenantSeen:
			// The tenant is the FIRST string parameter in all three
			// interfaces. Everything after it gets a usable value.
			tenantSeen = true
			args[i] = reflect.ValueOf("")
		case in.Kind() == reflect.String:
			args[i] = reflect.ValueOf("some-key")
		case in == reflect.TypeOf((*io.Reader)(nil)).Elem():
			args[i] = reflect.ValueOf(io.Reader(bytes.NewReader([]byte("bytes"))))
		case in == reflect.TypeOf(&dholev1.Digest{}):
			args[i] = reflect.ValueOf(&dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("ab", 32)})
		case in == reflect.TypeOf(runstore.Event{}):
			args[i] = reflect.ValueOf(runstore.Event{
				RunID: "run-1", StepID: "build", Attempt: 1, Sequence: 1,
				Type: runstore.RunCreated, At: time.Unix(1, 0).UTC(),
			})
		case in == reflect.TypeOf(time.Duration(0)):
			args[i] = reflect.ValueOf(time.Minute)
		default:
			t.Fatalf("the sweep does not know how to build a %s for %s: "+
				"add it here rather than skipping the method", in, name)
		}
	}
	require.True(t, tenantSeen, "%s takes no tenant: skip it by name, with a reason", name)

	// The watchdog covers the methods that would ignore the context: without
	// it, an unguarded call that blocks turns this test into a two-minute
	// timeout with no message saying which method did it.
	type result struct{ out []reflect.Value }
	done := make(chan result, 1)
	go func() { done <- result{value.Call(args)} }()
	var out []reflect.Value
	select {
	case r := <-done:
		out = r.out
	case <-time.After(15 * time.Second):
		t.Fatalf("%s blocked on an empty tenant instead of refusing it", name)
	}
	require.NotEmpty(t, out, "%s returns nothing, so it cannot refuse anything", name)
	last := out[len(out)-1]
	require.Equal(t, reflect.TypeOf((*error)(nil)).Elem(), last.Type(),
		"%s does not return an error, so it cannot refuse an empty tenant", name)
	if last.IsNil() {
		return nil
	}
	err, ok := last.Interface().(error)
	require.True(t, ok)
	// Close anything the call handed back, so a method that wrongly succeeded
	// does not leak a file handle into the rest of the suite.
	for _, o := range out[:len(out)-1] {
		if c, ok := o.Interface().(io.Closer); ok && c != nil {
			_ = c.Close()
		}
	}
	return err
}

// subjectBuilders is every exported builder in internal/bus/subjects.go, with
// arguments. The test below cross-checks these names against the file itself,
// so a builder added there fails this test until it is covered here.
func subjectBuilders() map[string]string {
	return map[string]string{
		"SubjectDispatch":           bus.SubjectDispatch("trusted", "capsdeadbeef"),
		"SubjectDispatchKind":       bus.SubjectDispatchKind("trusted", "capsdeadbeef", "vm"),
		"SubjectDispatchWildcard":   bus.SubjectDispatchWildcard("trusted"),
		"SubjectStatus":             bus.SubjectStatus("run-1", "build"),
		"SubjectLogs":               bus.SubjectLogs("run-1", "build"),
		"SubjectEngineControl":      bus.SubjectEngineControl("engine-1"),
		"SubjectEngineHeartbeat":    bus.SubjectEngineHeartbeat("engine-1"),
		"SubjectEngineRegistration": bus.SubjectEngineRegistration(),
		"SubjectSecretRedeem":       bus.SubjectSecretRedeem(),
	}
}

// TestSubjectBuildersIncludeTenant asserts that every subject the bus builders
// produce is confined to one tenant.
//
// READ THIS BEFORE CHANGING IT. The subject strings in
// docs/wire-contract.md carry no `<tenant>` segment — `job.dispatch.<tier>.<caps>`
// is the documented, engine-visible spelling — while the same document says
// "Every subject is tenant-scoped. There is no unscoped subject." Those two
// statements are only consistent under ADR 0014, which maps a tenant onto a
// NATS ACCOUNT: an account is its own subject namespace, so the same subject
// string in two accounts is two different subjects, isolated by the server.
// The tenant is therefore in the CONNECTION, not in the name — and this test
// asserts the property that scoping actually has to deliver, on a live server,
// rather than a substring.
//
// The consequence is that provisioning is load-bearing. If ProvisionAccount
// ever put two tenants in one account, or handed out permissions wide enough
// to reach past the tenant's own subjects, every subject in the table would
// silently become shared. That is what fails here.
func TestSubjectBuildersIncludeTenant(t *testing.T) {
	ctx := context.Background()

	// Drive off the file, so a builder added to subjects.go is covered here
	// or fails loudly. A silent gap is how a new subject leaks.
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join("..", "bus", "subjects.go"), nil, 0)
	require.NoError(t, err)
	declared := map[string]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !fn.Name.IsExported() {
			continue
		}
		declared[fn.Name.Name] = true
	}
	require.NotEmpty(t, declared)

	covered := subjectBuilders()
	for name := range declared {
		require.Contains(t, covered, name,
			"internal/bus/subjects.go declares %s, which no tenancy test covers", name)
	}
	for name := range covered {
		require.True(t, declared[name], "%s is no longer declared in internal/bus/subjects.go", name)
	}

	// The permission set a tenant's credentials carry must not reach past the
	// tenant subject space. A `>` here would give every tenant everything the
	// account can see, including the system account's traffic.
	perms, err := tenant.AccountPermissions("acme")
	require.NoError(t, err)
	for _, group := range [][]string{perms.Publish.Allow, perms.Subscribe.Allow} {
		require.NotEmpty(t, group)
		for _, allowed := range group {
			require.NotEqual(t, ">", allowed, "a tenant's permissions allow every subject")
			require.NotEqual(t, "*", allowed, "a tenant's permissions allow every subject")
			require.True(t, hasKnownPrefix(allowed),
				"a tenant's permissions allow %q, which is outside the tenant subject space", allowed)
		}
	}

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	credsA, err := tenant.ProvisionAccount(ctx, srv, "acme")
	require.NoError(t, err)
	credsB, err := tenant.ProvisionAccount(ctx, srv, "globex")
	require.NoError(t, err)

	publisher := dialTenant(t, credsA)
	watcher := dialTenant(t, credsA)
	outsider := dialTenant(t, credsB)

	for name, subject := range covered {
		t.Run(name, func(t *testing.T) {
			require.True(t, subjectAllowed(subject, perms.Publish.Allow) ||
				subjectAllowed(subject, perms.Subscribe.Allow),
				"%s produces %q, which a tenant's own credentials do not cover",
				name, subject)

			// A wildcard builder is a subscription pattern, not something to
			// publish on; publish on a concrete subject it matches.
			concrete := strings.ReplaceAll(subject, "*", "concrete")

			mine := make(chan struct{}, 1)
			subMine, err := watcher.Subscribe(subject, func(*nats.Msg) { mine <- struct{}{} })
			require.NoError(t, err)
			defer func() { _ = subMine.Unsubscribe() }()

			theirs := make(chan struct{}, 1)
			subTheirs, err := outsider.Subscribe(subject, func(*nats.Msg) { theirs <- struct{}{} })
			require.NoError(t, err)
			defer func() { _ = subTheirs.Unsubscribe() }()

			require.NoError(t, watcher.Flush())
			require.NoError(t, outsider.Flush())
			require.NoError(t, publisher.Publish(concrete, []byte("payload")))
			require.NoError(t, publisher.Flush())

			select {
			case <-mine:
			case <-time.After(3 * time.Second):
				t.Fatalf("%s: the tenant did not receive its own message on %q, so the isolation check below proves nothing", name, concrete)
			}
			select {
			case <-theirs:
				t.Fatalf("%s: another tenant received a message on %q", name, concrete)
			case <-time.After(300 * time.Millisecond):
			}
		})
	}
}

func hasKnownPrefix(subject string) bool {
	for _, prefix := range []string{"job.", "engine.", "secret.", "_INBOX.", "$JS."} {
		if strings.HasPrefix(subject, prefix) {
			return true
		}
	}
	return subject == "engine.registration"
}

// subjectAllowed applies NATS wildcard matching: `*` covers one token and `>`
// covers the rest.
func subjectAllowed(subject string, allow []string) bool {
	for _, pattern := range allow {
		if subjectMatches(subject, pattern) {
			return true
		}
	}
	return false
}

func subjectMatches(subject, pattern string) bool {
	subjectTokens := strings.Split(subject, ".")
	patternTokens := strings.Split(pattern, ".")
	for i, want := range patternTokens {
		if want == ">" {
			return i <= len(subjectTokens)
		}
		if i >= len(subjectTokens) {
			return false
		}
		if want != "*" && want != subjectTokens[i] {
			return false
		}
	}
	return len(subjectTokens) == len(patternTokens)
}

func dialTenant(t *testing.T, creds string) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(creds, nats.Timeout(5*time.Second))
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

// requirePermissionRefused asserts the server itself refuses the subscription,
// which is the difference between isolation and a convention.
func requirePermissionRefused(t *testing.T, creds, subject string) {
	t.Helper()
	refused := make(chan error, 1)
	conn, err := nats.Connect(creds,
		nats.Timeout(5*time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			select {
			case refused <- err:
			default:
			}
		}))
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Subscribe(subject, func(*nats.Msg) {})
	require.NoError(t, err)
	require.NoError(t, conn.Flush())

	select {
	case err := <-refused:
		require.Contains(t, strings.ToLower(err.Error()), "permissions violation",
			"subscribing to %q failed for the wrong reason", subject)
	case <-time.After(3 * time.Second):
		t.Fatalf("a tenant subscribed to %q and the server allowed it", subject)
	}
}
