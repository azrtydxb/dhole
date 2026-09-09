package tenant

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"sync"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/azrtydxb/dhole/internal/bus"
)

// The subject space one tenant's credentials may reach. It is the table from
// docs/wire-contract.md and nothing else — no `>`, no `$SYS`, no other
// tenant's account.
//
// These prefixes carry no tenant segment, and that is not an omission. The
// documented subjects are `job.dispatch.<tier>.<caps>` and friends; the tenant
// is the ACCOUNT the connection is in (ADR 0014), and an account is its own
// subject namespace. Putting the tenant in the name as well would change a
// public wire contract that engines in other languages are written against,
// and would still leave isolation depending on everyone spelling it right.
var (
	tenantSubjects = []string{
		"job.dispatch.>",
		"job.status.>",
		"job.logs.>",
		"engine.control.>",
		"engine.heartbeat.>",
		"engine.registration",
		"_INBOX.>",
		"$JS.API.>",
	}
	// Acking a JetStream delivery is a publish, so it is a publish-only
	// addition rather than part of the shared list.
	tenantPublishOnly = []string{"$JS.ACK.>"}
)

// provisionMu serialises the read-modify-write around the server's options:
// two tenants provisioned concurrently would each read the same configuration,
// and the second reload would drop the first tenant's account.
var provisionMu sync.Mutex

// AccountPermissions is the subject permission set one tenant's credentials
// carry. It is exported so a test can assert what it allows without starting a
// server — a permission set that quietly grew a `>` is the kind of change that
// passes every functional test.
func AccountPermissions(tenantID string) (*server.Permissions, error) {
	if err := Validate(tenantID); err != nil {
		return nil, err
	}
	subscribe := make([]string, len(tenantSubjects))
	copy(subscribe, tenantSubjects)
	publish := make([]string, 0, len(tenantSubjects)+len(tenantPublishOnly))
	publish = append(publish, tenantSubjects...)
	publish = append(publish, tenantPublishOnly...)
	return &server.Permissions{
		Subscribe: &server.SubjectPermission{Allow: subscribe},
		Publish:   &server.SubjectPermission{Allow: publish},
	}, nil
}

// ProvisionAccount gives tenantID its own NATS account on the running server
// and returns a client URL carrying credentials for it.
//
// One account per tenant is what makes tenancy a transport rule instead of a
// convention: two tenants publishing the identical subject string never see
// each other, because the subject is resolved inside the account. A Go-side
// check would be a habit somebody eventually forgets; this is enforced by the
// server, and a tenant reaching past its own subjects gets a permissions
// error.
//
// It is idempotent: provisioning a tenant that already has an account returns
// the credentials it already has, so a restarted caller does not lock itself
// out.
func ProvisionAccount(ctx context.Context, srv *bus.Embedded, tenantID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if srv == nil {
		return "", fmt.Errorf("tenant: provision account: no server")
	}
	if err := Validate(tenantID); err != nil {
		return "", err
	}
	perms, err := AccountPermissions(tenantID)
	if err != nil {
		return "", err
	}
	name := AccountName(tenantID)

	provisionMu.Lock()
	defer provisionMu.Unlock()

	opts := srv.Options()
	for _, existing := range opts.Users {
		if existing.Username == name {
			return credentialURL(srv, name, existing.Password)
		}
	}

	password, err := secret()
	if err != nil {
		return "", err
	}
	account := server.NewAccount(name)
	opts.Accounts = append(opts.Accounts, account)
	opts.Users = append(opts.Users, &server.User{
		Username:    name,
		Password:    password,
		Permissions: perms,
		Account:     account,
	})
	// A server with users defined refuses anonymous connections, so the
	// no-auth path has to be closed explicitly rather than left to chance:
	// an unauthenticated client landing in the global account would see
	// every tenant that has not been provisioned yet.
	opts.NoAuthUser = ""

	if err := srv.Reload(opts); err != nil {
		return "", fmt.Errorf("tenant: provision account %q: %w", name, err)
	}
	if registered, lookupErr := srv.Server().LookupAccount(name); lookupErr == nil {
		// JetStream is per-account. Without this the tenant's engines could
		// connect but not bind a durable consumer, and dispatch would be a
		// core-NATS fire-and-forget — the exact "bus as truth" mistake
		// ADR 0005 forbids.
		if !registered.JetStreamEnabled() {
			_ = registered.EnableJetStream(nil, nil)
		}
	}
	return credentialURL(srv, name, password)
}

// credentialURL builds the client URL a tenant connects with. The password is
// in the URL because that is what nats.Connect takes; it is a credential for
// one account and grants nothing outside it.
func credentialURL(srv *bus.Embedded, user, password string) (string, error) {
	parsed, err := url.Parse(srv.URL())
	if err != nil {
		return "", fmt.Errorf("tenant: client url: %w", err)
	}
	parsed.User = url.UserPassword(user, password)
	return parsed.String(), nil
}

func secret() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("tenant: generating credentials: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
