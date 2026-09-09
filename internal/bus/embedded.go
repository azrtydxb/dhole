package bus

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// readyTimeout bounds how long StartEmbedded waits for the in-process server to
// accept connections. Nothing here may block forever.
const readyTimeout = 10 * time.Second

// Embedded is a nats-server running in this process with JetStream on a local
// directory. It is what makes `dhole` a single binary for development and
// homelab use; the tuned target is still an external, clustered NATS.
type Embedded struct {
	srv   *server.Server
	users map[string]string // tier -> password

	// optsMu guards opts, which tracks the configuration the server is
	// CURRENTLY running with. A caller adding an account reloads from this,
	// so two accounts added in sequence do not drop each other.
	optsMu sync.Mutex
	opts   *server.Options
}

// StartEmbedded runs an in-process NATS server with JetStream storing under
// dir. It has no accounts: use it where the bus is entirely inside one trust
// boundary, such as tests and single-user development.
func StartEmbedded(dir string) (*Embedded, error) {
	return StartEmbeddedWithTiers(dir, nil)
}

// StartEmbeddedWithTiers runs the embedded server with one user per trust tier
// plus a control-plane user, each with subject permissions.
//
// The tier boundary is enforced HERE, by the bus, not by the control plane
// remembering to filter: an engine holding the untrusted tier's credentials
// gets a permissions error from the server when it subscribes to another
// tier's dispatch subjects. A Go-side check would be a convention; this is a
// rule. See docs/wire-contract.md, "Subjects".
func StartEmbeddedWithTiers(dir string, tiers []string) (*Embedded, error) {
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  dir,
		NoSigs:    true,
		NoLog:     true,
	}

	users := make(map[string]string, len(tiers)+1)
	if len(tiers) > 0 {
		planePassword, err := secret()
		if err != nil {
			return nil, err
		}
		users[PlaneUser] = planePassword
		opts.Users = []*server.User{{
			Username:    PlaneUser,
			Password:    planePassword,
			Permissions: planePermissions(),
		}}
		for _, tier := range tiers {
			password, err := secret()
			if err != nil {
				return nil, err
			}
			users[tier] = password
			opts.Users = append(opts.Users, &server.User{
				Username:    tier,
				Password:    password,
				Permissions: tierPermissions(tier),
			})
		}
	}

	srv, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("embedded nats: %w", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(readyTimeout) {
		srv.Shutdown()
		return nil, fmt.Errorf("embedded nats: not ready within %s", readyTimeout)
	}
	return &Embedded{srv: srv, opts: opts, users: users}, nil
}

// Server is the running nats-server, and Options a copy of what it was
// started with. They exist for internal/tenant, which provisions one NATS
// account per tenant on a LIVE server (ADR 0014) and needs the server object
// and its current configuration to reload it. Nothing else should reach
// through this boundary: everything else in the tree talks to the Bus
// interface, which says nothing about NATS.
func (e *Embedded) Server() *server.Server { return e.srv }

// Options returns a copy of the configuration the server is running with, so a
// caller adding an account cannot mutate it in place.
func (e *Embedded) Options() *server.Options {
	e.optsMu.Lock()
	defer e.optsMu.Unlock()
	return e.opts.Clone()
}

// Reload applies opts to the running server and, only if that succeeds,
// remembers them as the current configuration. Remembering is the load-bearing
// half: a caller that reloaded against a stale copy would silently delete
// whatever the previous reload added — one tenant's account provisioning
// erasing the last one's.
func (e *Embedded) Reload(opts *server.Options) error {
	e.optsMu.Lock()
	defer e.optsMu.Unlock()
	applied := opts.Clone()
	if err := e.srv.ReloadOptions(opts); err != nil {
		return fmt.Errorf("embedded nats: reload: %w", err)
	}
	e.opts = applied
	return nil
}

// PlaneUser is the username the control plane connects as on a tiered embedded
// server. It is the only identity permitted to publish dispatches.
const PlaneUser = "plane"

// URL is the client URL of the running server, with no credentials. On a tiered
// server it will be refused; use TierURL or PlaneURL there.
func (e *Embedded) URL() string {
	return e.srv.ClientURL()
}

// TierURL is the client URL carrying the credentials scoped to one tier. It
// returns an empty string for a tier the server was not started with.
func (e *Embedded) TierURL(tier string) string {
	return e.credentialURL(tier)
}

// PlaneURL is the client URL carrying the control plane's credentials.
func (e *Embedded) PlaneURL() string {
	return e.credentialURL(PlaneUser)
}

func (e *Embedded) credentialURL(user string) string {
	password, ok := e.users[user]
	if !ok {
		return ""
	}
	parsed, err := url.Parse(e.srv.ClientURL())
	if err != nil {
		return ""
	}
	parsed.User = url.UserPassword(user, password)
	return parsed.String()
}

// Close shuts the server down and waits for it to finish, so a test never
// leaks a server into the next case.
func (e *Embedded) Close() {
	e.srv.Shutdown()
	e.srv.WaitForShutdown()
}

// tierPermissions is the whole point of the tiered server: an engine may
// subscribe to its own tier's dispatch wildcard and to nothing else.
func tierPermissions(tier string) *server.Permissions {
	return &server.Permissions{
		Subscribe: &server.SubjectPermission{
			Allow: []string{
				SubjectDispatchWildcard(tier),
				"engine.control.>",
				"_INBOX.>",
				"$JS.API.CONSUMER.>",
			},
		},
		Publish: &server.SubjectPermission{
			Allow: []string{
				"job.status.>",
				"job.logs.>",
				"engine.heartbeat.>",
				SubjectEngineRegistration(),
				"$JS.API.CONSUMER.>",
				"$JS.API.STREAM.INFO.>",
				"$JS.API.STREAM.NAMES",
				"$JS.ACK.>",
				"_INBOX.>",
			},
		},
	}
}

// planePermissions leaves the control plane unrestricted: it is the authority
// the tiers are being separated from.
func planePermissions() *server.Permissions {
	return &server.Permissions{
		Subscribe: &server.SubjectPermission{Allow: []string{">"}},
		Publish:   &server.SubjectPermission{Allow: []string{">"}},
	}
}

func secret() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("embedded nats: generating credentials: %w", err)
	}
	return hex.EncodeToString(raw), nil
}
