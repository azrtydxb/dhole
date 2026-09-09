// Command fixture is the control plane the Playwright suite drives the canvas
// against. It is a REAL one: internal/api serving the generated
// PipelineService over HTTP, internal/identity/Local authenticating a real
// service token, and internal/defstore over a real SQLite database. Nothing
// here stubs an RPC, so an edit the canvas sends is applied by the same code a
// production plane applies it with, and a refusal is the production refusal.
//
// It exists because `dhole serve` does not yet mount internal/api: the
// server package assembles the bus, the scheduler and an engine, and nothing
// in cmd/ or internal/server ever calls api.Server.Handler. Until a task
// wires that up, "point Playwright at `dhole serve`" starts a control plane
// with no HTTP contract on it, and the web suite has nothing to talk to. This
// program is the smallest honest substitute — the API, its real
// authentication, and its real store — and it should be DELETED the day
// `dhole serve` serves the API itself.
//
// It prints nothing secret to a terminal a developer might paste: the token,
// the seeded pipeline and its revision go to a JSON file under
// web/.playwright/, which is gitignored, and the Playwright fixture reads it
// from there.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// handshake is what the browser side of the suite needs to reach this plane.
type handshake struct {
	BaseURL  string `json:"baseUrl"`
	Token    string `json:"token"`
	TenantID string `json:"tenantId"`
}

// seeded is one empty pipeline for one test to edit.
type seeded struct {
	PipelineID string `json:"pipelineId"`
	RevisionID string `json:"revisionId"`
}

// seedPipeline saves an empty pipeline under a fresh id and returns the
// revision to base the first edit on. A pipeline per test, because the head a
// concurrent edit conflicts against is per pipeline: two tests sharing one
// would fail each other for a reason neither is about.
func seedPipeline(defs defstore.Store, tenantID string) http.Handler {
	var n atomic.Uint64
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := fmt.Sprintf("canvas-e2e-%d", n.Add(1))
		rev, err := defs.Save(r.Context(), tenantID, &dholev1.Pipeline{Id: id}, "fixture")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(seeded{PipelineID: id, RevisionID: rev.ID})
	})
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "address to serve the API on")
	state := flag.String("state", ".playwright", "directory for the database and the handshake file")
	flag.Parse()

	if err := run(*addr, *state); err != nil {
		fmt.Fprintf(os.Stderr, "fixture: %v\n", err)
		os.Exit(1)
	}
}

func run(addr, state string) error {
	ctx := context.Background()

	// A scratch database per start: a fixture that accumulated yesterday's
	// pipelines would make a passing test depend on which tests ran before it.
	if err := os.RemoveAll(state); err != nil {
		return fmt.Errorf("clearing %s: %w", state, err)
	}
	if err := os.MkdirAll(state, 0o750); err != nil {
		return fmt.Errorf("creating %s: %w", state, err)
	}
	dsn := filepath.Join(state, "dhole.db")

	// NewSQLite applies the migrations; every store below shares the handle,
	// exactly as the single binary does.
	store, err := runstore.NewSQLite(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	db, err := runstore.OpenSQLite(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// WithoutPinning because these pipelines name no plugin: there is nothing
	// to resolve, and the option is how a caller says so out loud.
	defs := defstore.New(db, defstore.WithoutPinning())
	local := identity.NewLocal(identity.NewSQLStore(db))

	const tenantID = "default"
	token, err := local.IssueToken(ctx, identity.Principal{
		TenantID: tenantID,
		Subject:  "playwright",
		Kind:     identity.PrincipalService,
	}, time.Hour)
	if err != nil {
		return fmt.Errorf("issuing the suite's token: %w", err)
	}

	srv, err := api.NewServer(api.Config{Definitions: defs, Auth: local, Runs: store})
	if err != nil {
		return err
	}

	// The canvas edits an EXISTING pipeline: ApplyOperation takes a base
	// revision, so every test needs one of its own to base its first edit on.
	// Seeding is a fixture route rather than an RPC because the contract has
	// no "create pipeline" call — a definition arrives through `dhole
	// pipeline apply` or a forge, neither of which the canvas suite is about.
	mux := http.NewServeMux()
	mux.Handle("POST /fixture/pipeline", seedPipeline(defs, tenantID))
	mux.Handle("/", srv.Handler())

	// The handshake is written BEFORE the socket opens. Playwright waits for
	// the port and then reads this file, so a file written after the listener
	// could be read while it is still a few microseconds from existing.
	if err := writeHandshake(state, handshake{
		BaseURL:  "http://" + addr,
		Token:    token,
		TenantID: tenantID,
	}); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}
	fmt.Printf("fixture: dhole API on http://%s\n", listener.Addr())

	//nolint:gosec // A local test fixture; the timeouts a public server needs
	// would only cut the suite's own long-poll RPCs short.
	return http.Serve(listener, cors(mux))
}

// writeHandshake records what the browser needs, last, so a reader that sees
// the file sees a plane that is already listening.
func writeHandshake(state string, h handshake) error {
	body, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(state, "fixture.json")
	if err := os.WriteFile(path, append(body, '\n'), 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

// cors lets the page served by Vite on another port talk to this one.
//
// It is permissive because it is a fixture on a loopback address with a token
// that lives an hour, and because the alternative — teaching the suite to run
// same-origin — would test a deployment nobody has. The real plane's CORS
// policy is not this, and this file is not where it should be decided.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("Access-Control-Allow-Origin", "*")
		header.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		header.Set("Access-Control-Allow-Headers", "*")
		// Connect reports a unary error's details in headers the browser can
		// only read when they are exposed.
		header.Set("Access-Control-Expose-Headers", "*")
		header.Set("Access-Control-Max-Age", "86400")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
