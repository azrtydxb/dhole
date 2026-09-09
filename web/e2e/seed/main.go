// Command seed mints a credential for a tenant other than the bootstrapped one.
//
// It is what is LEFT of e2e/fixture, which was a whole parallel control plane
// standing in for a `dhole serve` that did not mount internal/api. It does
// now: the Playwright suite drives the real binary, with the real API, the
// real authentication and the real definition store, and the credential it
// uses is the bootstrap token `dhole serve` mints and writes beside its
// database.
//
// Seeding a pipeline used to live here too, because ApplyOperation requires a
// base_revision and the contract had no call that wrote a first one. It does
// now — CreatePipeline — so the suite creates pipelines the way the canvas
// does, and this program no longer touches the definition store at all.
//
// Publishing a plugin is here for a DIFFERENT reason, and it is worth naming
// so nobody mistakes it for the hole that has just been closed: nothing
// anywhere in Dhole publishes to the catalog. Not the API, not the CLI, not
// the git mirror — internal/catalog.Publish has no caller outside tests. That
// is a real gap and a task of its own; until it has one, a suite that needs a
// published plugin has to write the row, and this is where it does.
//
// What is left is the second tenant. `dhole serve` prints the bootstrap
// credential for the default tenant only, and a suite asserting that one
// tenant cannot see another's runs needs a token for the other one; `dhole
// token issue` is the command an operator uses, and this is the same call over
// HTTP so the browser side of the suite does not have to shell out.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/catalog"
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

// published is one plugin the suite wants in the catalog.
type published struct {
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	Version     string `json:"version"`
	EffectClass string `json:"effectClass"`
	InputSchema any    `json:"inputSchema"`
}

// digestOf is a stand-in for the artefact digest a real publish would carry.
// The catalog refuses a manifest without one, and these plugins have no
// artefact behind them at all.
func digestOf(schema []byte) string {
	sum := sha256.Sum256(schema)
	return hex.EncodeToString(sum[:])
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

	local := identity.NewLocal(identity.NewSQLStore(db))
	plugins := catalog.New(db, runstore.DialectSQLite)

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

	mux.HandleFunc("POST /plugin", func(w http.ResponseWriter, r *http.Request) {
		var body published
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		inputSchema, err := json.Marshal(body.InputSchema)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		effect, ok := dholev1.EffectClass_value[body.EffectClass]
		if !ok {
			http.Error(w, "unknown effect class "+body.EffectClass, http.StatusBadRequest)
			return
		}
		manifest := catalog.Manifest{
			Namespace:   body.Namespace,
			Name:        body.Name,
			Version:     body.Version,
			Digest:      &dholev1.Digest{Algo: "sha256", Hex: digestOf(inputSchema)},
			Kind:        catalog.KindStep,
			EffectClass: dholev1.EffectClass(effect),
			InputSchema: inputSchema,
			// A manifest with no output schema is refused by the catalog,
			// and these plugins are about their inputs.
			OutputSchema: []byte(`{"$id":"https://example.test/out.json","type":"object"}`),
			EngineTypes:  []string{"process"},
		}
		if err := plugins.Publish(r.Context(), "default", manifest); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"ref": manifest.Ref()})
	})

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}
	fmt.Printf("seed: issuing tenant credentials on http://%s\n", listener.Addr())

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
