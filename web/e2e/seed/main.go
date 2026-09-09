// Command seed gives each canvas test a pipeline of its own to edit.
//
// It is what is LEFT of e2e/fixture, which was a whole parallel control plane
// standing in for a `dhole serve` that did not mount internal/api. It does
// now: the Playwright suite drives the real binary, with the real API, the
// real authentication and the real definition store, and the credential it
// uses is the bootstrap token `dhole serve` mints and writes beside its
// database.
//
// What could not go with the fixture is seeding. ApplyOperation takes a
// base_revision — an edit that cannot conflict overwrites somebody else's
// silently — and the contract has no call that creates a pipeline from
// nothing: a definition arrives through `dhole pipeline apply` against an
// existing one, or through the git mirror, neither of which the canvas suite
// is about. So the first revision has to be written directly to the store,
// which is all this program does. It goes away the day the contract can
// create a pipeline.
//
// A pipeline PER TEST, because the editing head is per pipeline: two tests
// sharing one would conflict with each other for a reason neither is about.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// issued is a credential minted for one tenant. `dhole serve` prints the
// bootstrap credential for the default tenant only, and a suite testing that
// one tenant cannot see another's runs needs a second one; `dhole token issue`
// is the command that mints it, and this is the same call over HTTP so the
// browser side of the suite does not have to shell out.
type issued struct {
	Token string `json:"token"`
}

// seeded is one empty pipeline and the revision to base the first edit on.
type seeded struct {
	PipelineID string `json:"pipelineId"`
	RevisionID string `json:"revisionId"`
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8081", "address to serve the seed route on")
	dsn := flag.String("store-dsn", ".playwright/dhole.db", "the plane's SQLite database")
	waitFor := flag.String("wait-for", "127.0.0.1:8080",
		"the plane's API address; this program waits for it, so the migrations are applied "+
			"by the plane that owns the database rather than raced with it")
	flag.Parse()

	if err := run(*addr, *dsn, *waitFor); err != nil {
		fmt.Fprintf(os.Stderr, "seed: %v\n", err)
		os.Exit(1)
	}
}

func run(addr, dsn, waitFor string) error {
	if err := awaitListener(waitFor); err != nil {
		return err
	}

	// The plane's own database, opened as a second process. SQLite is in WAL
	// mode with a busy timeout (runstore.OpenSQLite), so a write from here
	// queues behind the plane's rather than failing.
	db, err := runstore.OpenSQLite(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// WithoutPinning because these pipelines name no plugin: there is nothing
	// to resolve, and the option is how a caller says so out loud.
	defs := defstore.New(db, defstore.WithoutPinning())

	local := identity.NewLocal(identity.NewSQLStore(db))

	var n atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		tenant := r.URL.Query().Get("tenant")
		if tenant == "" {
			tenant = "default"
		}
		subject := r.URL.Query().Get("subject")
		if subject == "" {
			subject = "e2e"
		}
		token, err := local.IssueToken(r.Context(), identity.Principal{
			TenantID: tenant, Subject: subject, Kind: identity.PrincipalService,
		}, time.Hour)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(issued{Token: token})
	})
	mux.HandleFunc("POST /pipeline", func(w http.ResponseWriter, r *http.Request) {
		id := fmt.Sprintf("canvas-e2e-%d", n.Add(1))
		rev, err := defs.Save(r.Context(), "default", &dholev1.Pipeline{Id: id}, "e2e-seed")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(seeded{PipelineID: id, RevisionID: rev.ID})
	})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}
	fmt.Printf("seed: seeding pipelines on http://%s\n", listener.Addr())

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 15 * time.Second}
	return srv.Serve(listener)
}

// awaitListener blocks until something is accepting on addr.
func awaitListener(addr string) error {
	deadline := time.Now().Add(3 * time.Minute)
	for {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the control plane never started listening on %s", addr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
