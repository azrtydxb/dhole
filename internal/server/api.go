package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/catalog"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/health"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/webui"
)

// DefaultAPIAddr is where a plane that was not told otherwise serves its
// contract. It is the address `dhole --server` dials by default: the binary
// and its CLI disagreeing about the port is the first thing anyone would hit,
// and it would look like the API is not served at all — which is exactly the
// bug this file exists to close.
const DefaultAPIAddr = "127.0.0.1:7777"

// The bootstrap credential a fresh plane mints for its operator.
//
// The API has no unauthenticated call (ADR 0013 and internal/api/auth.go), so
// a plane with no way to issue a first credential is a plane nobody can use —
// and the pressure that creates is exactly how control planes end up
// accepting anonymous calls "just until it is configured". So the plane mints
// one itself, at every start, and prints it once.
//
// It is a REAL service token: identity.Local issues it, the store keeps only
// its SHA-256, and it is authenticated by the same code path as any other
// token. Nothing about it is special-cased in api/auth.go, which is what
// makes "the printed credential actually works" a testable property rather
// than a claim.
//
// It is minted rather than configured because the alternatives are worse. A
// --token flag or a DHOLE_TOKEN environment variable is a static shared
// secret that lands in shell history, `ps`, a unit file and CI logs, and it
// never rotates; a `dhole bootstrap` subcommand would have to open the plane's
// own database while the plane holds it, and leaves a window in which the
// operator has a plane they cannot talk to — which is when somebody reaches
// for an anonymous mode.
const (
	bootstrapSubject = "bootstrap"
	bootstrapTTL     = 24 * time.Hour
	// BootstrapTokenFile is the name, under the plane's state directory, of
	// the file the bootstrap credential is also written to. Mode 0600: a
	// supervised process's stdout goes to a journal many people can read, so
	// the file is what a script should use.
	BootstrapTokenFile = "bootstrap.token"
)

// BootstrapTTL is how long the bootstrap credential lives. It is a function
// rather than an exported constant so that the value the binary prints and the
// value the token is issued with cannot drift apart.
func BootstrapTTL() time.Duration { return bootstrapTTL }

// apiReadHeaderTimeout bounds how long a connection may take to send its
// request headers. It is the ONLY timeout set on this server: a read or write
// deadline would cut WatchRun, which follows a run for as long as the run
// lasts, and a client watching a three-day wait is not a slow client.
const apiReadHeaderTimeout = 15 * time.Second

// startAPI mounts internal/api on a listener and serves it.
//
// This is the wiring whose absence made `dhole serve` a control plane with no
// contract on it: everything below was built, tested and imported by nobody.
// The collaborators are the ones this server already assembles — there is no
// second definition store, no second run log and no second registry, because
// an API answering out of its own copies would be a different system from the
// one the scheduler is running.
func (s *Server) startAPI(runCtx context.Context) (err error) {
	if s.cfg.NoAPI {
		return nil
	}
	addr := s.cfg.APIAddr
	if addr == "" {
		addr = DefaultAPIAddr
	}

	// The credential store shares the run log's database and its dialect: a
	// Postgres deployment whose identity tables were addressed with SQLite
	// placeholders would refuse every login.
	principals := identity.NewSQLStoreWithDialect(s.infra.db, s.infra.dialect)
	local := identity.NewLocal(principals)

	token, err := local.IssueToken(runCtx, identity.Principal{
		TenantID: DefaultTenant,
		Subject:  bootstrapSubject,
		Kind:     identity.PrincipalService,
	}, bootstrapTTL)
	if err != nil {
		return fmt.Errorf("server: minting the bootstrap credential: %w", err)
	}
	if err := s.writeBootstrapToken(token); err != nil {
		return err
	}

	plugins := catalog.New(s.infra.db, s.infra.dialect)

	cfg := api.Config{
		Definitions: s.defs,
		Auth:        local,
		// The STORED head, not the in-memory one: run partitioning makes
		// several planes over one database the normal deployment, and a head
		// each plane remembers privately is no concurrency check between them
		// — both accept an edit against the same base and one of the two
		// disappears.
		Heads: defstore.NewHeads(s.infra.db, s.infra.dialect),
		Runs:  s.infra.store,
		// The same principal table the credential above was authenticated
		// against, because an approval gate is decided by the principal this
		// plane authenticated and by nobody else.
		Approvers: principals,
		Advancer:  s.sched,
		// The same enforcer the guarded CAS and the scheduler answer to, so a
		// tenant's daily runs, its concurrency and its storage are all
		// measured against one limits row. Without it StartRun enforced
		// max_runs_per_day nowhere and wrote no RUN_STARTED row to bill from.
		Quotas: s.quotasLocked(),
		// The GUARDED content-addressed store, which is the one openBlobs
		// left behind: the bytes of a file a definition carries are charged
		// against the tenant's max_cas_bytes like every other stored byte, so
		// an oversized definition is refused by the limit that already exists
		// rather than by a ceiling invented for files (ADR 0023).
		Files: s.infra.cas,
		Cache: s.infra.cache,
		Fleet: s.fleet,
		Drain: s.fleet,
		// The plane's own bus connection, so a cancel travels the same way
		// every dispatch does. An API reaching engines over a connection of
		// its own would be a second control plane.
		Control: api.NewBusControl(s.infra.plane),
		// The same connection carries presence. It is an ephemeral subject
		// per pipeline and nothing about it is stored (internal/api's
		// presence.go); giving the API a bus connection of its own would be a
		// second control plane on the same bus.
		Presence: s.infra.plane,
		// One catalog, read and written. The reader is what Validate and Plan
		// resolve steps against; the writer is what PublishPlugin records
		// into, and until it was passed the only way a declaration reached
		// the store was a process opening this database behind the API's back.
		Catalog:       plugins,
		CatalogWriter: plugins,
		// The two copies of a step's log. Without them the run view's log
		// endpoint answers "this server was built without an object store"
		// for every step in every run — which it did, in a real deployment,
		// while the store sat right here on the same struct.
		//
		// The live copy is the plane's own bus connection rather than a new
		// one: the ephemeral log subject is the same subject the engine
		// publishes to, and a second connection here would be a second
		// control plane on the same bus.
		LiveLogs:   s.infra.plane,
		LogArchive: s.infra.blobs,
		OS:         runtime.GOOS,
		Arch:       runtime.GOARCH,
		// The tier the scheduler dispatches to, because that is whose engines
		// a plan has to ask about the environment. This used to be the plane's
		// own executor, which a distributed plane does not have — so Plan
		// answered "this server was built without an execution environment"
		// on every deployment that is not the single binary (ADR 0021).
		Tier: DefaultTier,
		// The trigger table, and the triggers this plane's own `--triggers`
		// file declares. Both, because a create has to know which ids the
		// file already claims: a declared trigger wins, and a row that
		// shadowed one would fire or not depending on a file the caller
		// cannot see.
		TriggerStore:     s.triggerStore(),
		DeclaredTriggers: s.declaredTriggers(),
	}

	apiSrv, err := api.NewServer(cfg)
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("server: listening for API calls on %s: %w", addr, err)
	}
	defer func() {
		if err != nil {
			_ = listener.Close()
		}
	}()

	httpSrv := &http.Server{
		Handler:           allowOrigins(s.rootHandler(apiSrv.Handler()), s.cfg.APIAllowedOrigins),
		ReadHeaderTimeout: apiReadHeaderTimeout,
		BaseContext:       func(net.Listener) context.Context { return runCtx },
	}
	s.apiHTTP = httpSrv
	s.apiAddr = listener.Addr().String()
	s.bootstrap = token

	// The goroutine serves the server it was given, not whatever s.apiHTTP
	// holds when it happens to start. Stop clears that field, and a Stop that
	// lands between spawn and the first statement here would otherwise leave
	// this dereferencing nil.
	s.spawn(func() {
		if serveErr := httpSrv.Serve(listener); serveErr != nil &&
			!errors.Is(serveErr, http.ErrServerClosed) {
			s.log.Error("API server stopped", "error", serveErr)
		}
	})
	return nil
}

// writeBootstrapToken records the credential where a script can read it,
// readable by nobody else. It is written under the state directory this plane
// owns alone, beside the database whose principals it authenticates against.
func (s *Server) writeBootstrapToken(token string) error {
	if err := os.MkdirAll(s.cfg.BlobRoot, 0o750); err != nil {
		return fmt.Errorf("server: creating the state directory: %w", err)
	}
	path := filepath.Join(s.cfg.BlobRoot, BootstrapTokenFile)
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		return fmt.Errorf("server: writing %s: %w", path, err)
	}
	return nil
}

// stopAPI closes the listener and ends the goroutine serving it.
//
// Shutdown first so a call already in flight finishes, and Close immediately
// after so a WatchRun that is following a run it may follow for days cannot
// hold the plane open. Both are bounded by ctx, which is the caller's stop
// deadline: a Stop that could wait forever is not a Stop.
func (s *Server) stopAPI(ctx context.Context) {
	if s.apiHTTP == nil {
		return
	}
	grace, cancel := context.WithTimeout(ctx, 5*time.Second)
	_ = s.apiHTTP.Shutdown(grace)
	cancel()
	_ = s.apiHTTP.Close()
	s.apiHTTP = nil
	s.apiAddr = ""
	s.bootstrap = ""
}

// APIAddr is the address the contract is being served on, and empty when it is
// not being served. It is the resolved address rather than the configured one:
// a deployment that asked for port zero gets the port it actually got.
func (s *Server) APIAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.apiAddr
}

// BootstrapToken is the credential this plane minted at start-up, and the only
// copy that will ever exist in memory — the store holds its hash. It is empty
// when the plane serves no API, because a credential nothing can be presented
// to is a secret with no purpose and a needless row in the token table.
func (s *Server) BootstrapToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bootstrap
}

// allowOrigins answers CORS preflights for the origins a deployment named, and
// for nobody else.
//
// The default is NO origin, which is the only safe one: a browser page on any
// site could otherwise make credentialed calls to a plane on the developer's
// own loopback address. A deployment that serves its web app from a different
// origin — `vite` on :5173 in development — says so explicitly.
//
// Credentials travel in an Authorization header rather than a cookie, so this
// never sets Allow-Credentials: the browser attaches nothing on its own, and
// a wildcard-with-credentials mistake is impossible here by construction.
func allowOrigins(next http.Handler, origins []string) http.Handler {
	if len(origins) == 0 {
		return next
	}
	allowed := make(map[string]struct{}, len(origins))
	for _, origin := range origins {
		if trimmed := strings.TrimSpace(origin); trimmed != "" {
			allowed[trimmed] = struct{}{}
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if _, ok := allowed[origin]; ok {
			header := w.Header()
			// Vary, or a cache serves one origin the answer given to another.
			header.Add("Vary", "Origin")
			header.Set("Access-Control-Allow-Origin", origin)
			header.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			header.Set("Access-Control-Allow-Headers",
				"Authorization, Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms")
			// Connect reports a unary error's details in headers a browser
			// can only read when they are exposed.
			header.Set("Access-Control-Expose-Headers",
				"Content-Type, Connect-Content-Encoding, Connect-Accept-Encoding")
			header.Set("Access-Control-Max-Age", "86400")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rootHandler puts the probes and the GUI on the same listener as the API.
//
// One port, deliberately. The GUI speaks Connect and holds two SSE streams
// open, and every one of those is a cross-origin request when the app is
// served from anywhere else — so a separately hosted GUI does not work at all
// until someone names its origin, and fails silently in a browser until they
// work out that is why. Same origin, no CORS, one ingress.
//
// The API keeps every path it owns. Connect mounts its services under
// /dhole.<package>. and the SSE endpoints under /v1/, so those two prefixes go
// to the API and everything else falls through to the app — which is what lets
// a deep link like /runs/run_123 open the run it names.
func (s *Server) rootHandler(api http.Handler) http.Handler {
	mux := http.NewServeMux()

	// The probes first, because they must answer whether or not there is a
	// GUI in this build and whatever the API is doing.
	health.Handler(mux, map[string]health.Check{
		// The run log is the plane's memory; without it nothing can be
		// recorded, planned or served, so a plane that cannot reach it is not
		// ready by any definition.
		"store": func(ctx context.Context) error {
			if s.infra == nil || s.infra.db == nil {
				return errors.New("no run store")
			}
			return s.infra.db.PingContext(ctx)
		},
		// And the bus, because a plane that cannot reach it accepts runs it
		// can never dispatch — which looks like a working plane and a stuck
		// queue.
		"bus": func(_ context.Context) error {
			if s.infra == nil || s.infra.plane == nil {
				return errors.New("no bus connection")
			}
			return s.infra.plane.Health()
		},
	})

	// The webhook triggers, on the listener the contract already has. They
	// are registered ahead of the app's catch-all so a deep link cannot
	// shadow one, and they are outside isAPIPath's two prefixes so neither
	// can shadow the other.
	// Mounted unconditionally and dispatched through serveTrigger, which
	// reads the mux as it stands at REQUEST time. A trigger created through
	// the contract has to be served without a restart, and a handler that
	// closed over the mux this plane happened to have at start-up never
	// could be.
	mux.HandleFunc(TriggerPrefix, s.serveTrigger)

	if app, ok := webui.Handler(); ok {
		mux.Handle("/", app)
	} else {
		// No GUI in this build, which is a supported way to build it. The API
		// still owns its own paths above; this only decides what an unknown
		// path gets, and "there is no app here" beats the API's own 404.
		mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "this control plane was built without the web app", http.StatusNotFound)
		})
	}

	// The API is chosen by PREFIX, ahead of the mux, rather than registered on
	// it. A ServeMux pattern only matches a prefix when it ends in "/", so
	// "/dhole." would have been an exact match on that literal path and every
	// Connect POST would have fallen through to the app — which answered 405,
	// because an app serves GET. Being explicit about the two prefixes the API
	// owns is both shorter than enumerating generated route constants and
	// harder to get subtly wrong.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPIPath(r.URL.Path) {
			api.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// isAPIPath reports whether the API owns this path.
//
// Connect mounts every service under /<proto package>., and the SSE endpoints
// the run view holds open live under /v1/. Both are the contract's, and a new
// prefix added to either must be added here — which is what
// TestMountingTheProbesLeavesEveryAPIPathServed exists to catch.
func isAPIPath(path string) bool {
	return strings.HasPrefix(path, "/dhole.") || strings.HasPrefix(path, "/v1/")
}
