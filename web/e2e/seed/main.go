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
	"sync/atomic"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/catalog"
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

// seeded is one pipeline of a named shape and the revision to base the first
// edit on. It is what POST /shape/{name} answers with.
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
	// A definition store for the shaped pipelines below. A pipeline with no
	// plugin references is created through CreatePipeline like any client
	// would; a SHAPE carries steps with commands, which no operation can set,
	// and there is nothing to pin — saying so beats leaving it to chance.
	defs := defstore.NewWithDialect(db, runstore.DialectSQLite, defstore.WithoutPinning())
	// One counter, so two shapes of the same name are two pipelines.
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
	// A pipeline of a named SHAPE. The run view needs particular ones — two
	// pure steps where the second consumes the first, a step with no effect
	// class, a bounded loop — and nothing creates them through the contract:
	// ApplyOperation needs a base revision, and no operation sets a step's
	// command or builds a loop node. They are seeded here rather than invented
	// in TypeScript so the definition under test is one the Go types accept.
	mux.HandleFunc("POST /shape/{name}", func(w http.ResponseWriter, r *http.Request) {
		id := fmt.Sprintf("shape-%s-%d", r.PathValue("name"), n.Add(1))
		pipeline, err := shaped(id, r.PathValue("name"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rev, err := defs.Save(r.Context(), "default", pipeline, "e2e-seed")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// A saved revision is a draft, and a run needs an active one. The
		// approver differs from the author because defstore refuses
		// self-approval — an author approving their own work records a review
		// that never happened.
		if err := defs.Approve(r.Context(), "default", rev.ID, "e2e-reviewer"); err != nil {
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

// shaped builds the definitions the run view's assertions need. Each shape is
// named for the property it exists to exercise, not for its contents.
func shaped(id, name string) (*dholev1.Pipeline, error) {
	blob := func(port string) *dholev1.Port {
		return &dholev1.Port{
			Name: port,
			Type: &dholev1.PortType{Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}}},
		}
	}
	step := func(sid, cmd string, class dholev1.EffectClass, in, out []string) *dholev1.Step {
		s := &dholev1.Step{
			Id:          sid,
			Name:        sid,
			PluginRef:   commandRef(cmd),
			EffectClass: class,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
		}
		for _, p := range in {
			s.Inputs = append(s.Inputs, blob(p))
		}
		for _, p := range out {
			s.Outputs = append(s.Outputs, blob(p))
		}
		return s
	}

	switch name {
	case "cacheable":
		// Two pure steps, the second consuming the first: what a cache hit on
		// a second run is visible against.
		// The redirections are load-bearing. A declared output is a FILE in
		// the sandbox named after its port, so `printf one` — which writes to
		// stdout — leaves no "out" to collect and the step fails every time
		// with `get "out": reader exited 1`. It did, and it took three e2e
		// tests down with it while looking like a product bug.
		return &dholev1.Pipeline{Id: id, Steps: []*dholev1.Step{
			step("first", "printf one > out", dholev1.EffectClass_EFFECT_CLASS_PURE, nil, []string{"out"}),
			step("second", "cat in > out", dholev1.EffectClass_EFFECT_CLASS_PURE, []string{"in"}, []string{"out"}),
		}, Edges: []*dholev1.Edge{
			{FromStep: "first", FromPort: "out", ToStep: "second", ToPort: "in"},
		}}, nil
	case "slow":
		// A step slow enough to be caught in the act. The cacheable shape
		// finishes in milliseconds, so a test that means to watch the log
		// switch from the ephemeral subject to the stored object never sees
		// the live half at all — it observes "stored" on its first look and
		// fails claiming the live source is broken when it is merely over.
		//
		// It prints BEFORE it sleeps, so there is something to tail rather
		// than an open stream carrying nothing.
		return &dholev1.Pipeline{Id: id, Steps: []*dholev1.Step{
			step("first", "echo starting; sleep 5; printf one > out",
				dholev1.EffectClass_EFFECT_CLASS_PURE, nil, []string{"out"}),
		}}, nil
	case "impure":
		// No effect class, so cache.Eligible refuses it and the run view must
		// show the reason rather than a blank.
		return &dholev1.Pipeline{Id: id, Steps: []*dholev1.Step{
			step("first", "printf one > out", dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED, nil, []string{"out"}),
		}}, nil
	default:
		return nil, fmt.Errorf("seed: unknown shape %q", name)
	}
}

// commandRef is the inline command scheme the scheduler resolves. It is a
// stopgap in the product too — see internal/scheduler/command.go.
func commandRef(cmd string) string {
	args, _ := json.Marshal(struct {
		Args []string `json:"args"`
	}{Args: []string{"/bin/sh", "-c", cmd}})
	return "command:" + string(args)
}
