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
		"secret.redeem",
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
	enableJetStream(srv, name)
	return credentialURL(srv, name, password)
}

// TierAccountName is the username an engine of one tier connects as inside a
// tenant's account. It is not a subject, so its shape matters only for being
// unique and readable.
func TierAccountName(tenantID, tier string) string {
	return AccountName(tenantID) + "-" + tier
}

// ProvisionTierUser adds a credential inside tenantID's EXISTING account that
// is limited to one trust tier, and returns a client URL carrying it.
//
// This is what a distributed deployment must hand an engine. The account
// credential ProvisionAccount returns reaches the tenant's whole subject space
// — including `job.dispatch.>`, every tier of it — because it is also the
// identity the control plane's own components use. Giving that to an engine
// makes tier isolation nothing but the engine's own good manners, which is the
// thing [S-5] exists to refuse: "refused at the bus subject level rather than
// by application code".
//
// The permissions come from bus.TierPermissions, the same table the embedded
// tiered server uses, and not from a second copy here: the pull-consumer hole
// closed in internal/bus was open for as long as it was partly because the
// subject list lived in two places and only one of them was ever looked at.
//
// Idempotent for the same reason ProvisionAccount is: a re-provision must not
// lock out engines already holding the credential.
func ProvisionTierUser(ctx context.Context, srv *bus.Embedded, tenantID, tier string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if srv == nil {
		return "", fmt.Errorf("tenant: provision tier user: no server")
	}
	if err := Validate(tenantID); err != nil {
		return "", err
	}
	perms, err := bus.TierPermissions(tier)
	if err != nil {
		return "", fmt.Errorf("tenant: provision tier user: %w", err)
	}
	account := AccountName(tenantID)
	name := TierAccountName(tenantID, tier)

	provisionMu.Lock()
	defer provisionMu.Unlock()

	opts := srv.Options()
	var target *server.Account
	for _, existing := range opts.Users {
		if existing.Username == name {
			return credentialURL(srv, name, existing.Password)
		}
		// The tier user has to land in the tenant's OWN account object, the
		// one already in these options. A freshly built Account of the same
		// name would be a different account after the reload, and the engine
		// would sit in an empty namespace receiving nothing.
		if existing.Username == account && existing.Account != nil {
			target = existing.Account
		}
	}
	if target == nil {
		return "", fmt.Errorf("tenant: provision tier user %q: tenant %q has no account yet", name, tenantID)
	}

	password, err := secret()
	if err != nil {
		return "", err
	}
	opts.Users = append(opts.Users, &server.User{
		Username:    name,
		Password:    password,
		Permissions: perms,
		Account:     target,
	})
	opts.NoAuthUser = ""

	if err := srv.Reload(opts); err != nil {
		return "", fmt.Errorf("tenant: provision tier user %q: %w", name, err)
	}
	// A reload re-registers the account, and JetStream does not survive that
	// on its own: without this the tenant's stream came back "JetStream not
	// enabled for account" the moment a tier user was added, so adding an
	// engine's credential would have broken the tenant already running.
	enableJetStream(srv, account)
	return credentialURL(srv, name, password)
}

// enableJetStream turns JetStream on for an account that does not have it.
// JetStream is per-account: without it a tenant's engines connect but cannot
// bind a durable consumer, and dispatch degrades to a core-NATS
// fire-and-forget — the exact "bus as truth" mistake ADR 0005 forbids.
func enableJetStream(srv *bus.Embedded, account string) {
	registered, err := srv.Server().LookupAccount(account)
	if err != nil || registered.JetStreamEnabled() {
		return
	}
	_ = registered.EnableJetStream(nil, nil)
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
