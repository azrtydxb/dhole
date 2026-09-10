package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/process"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/tenancy"
)

// infra is everything the control plane sits on. It is assembled by mode and
// torn down in reverse; nothing above it knows which half of the pair it got.
//
// That symmetry is the whole point of embedded mode. The single binary starts
// a NATS server inside itself and then DIALS IT — the engine it hosts reaches
// the scheduler over the same subjects an engine in another datacentre would,
// with the same JetStream work queue, the same fence tokens and the same
// acknowledgements. An in-process function call would have been shorter by a
// hundred lines and would have made the single binary a different system,
// which is a thing you find out at the worst possible moment: when a bug
// reproduces on a laptop and not in the cluster, or the reverse.
type infra struct {
	// busURL is where anything else dials this deployment's bus.
	busURL string
	// plane is the control plane's own connection.
	plane *bus.NATS
	// conn is the raw NATS connection the KV-backed lease manager and engine
	// registry need. They take a *nats.Conn rather than a bus.Bus because a
	// KV bucket is not a subject.
	conn *nats.Conn
	// store is the run event log; db is the same database reached through
	// database/sql, which is how definitions, the catalog and the cache read
	// it.
	store   runstore.Store
	db      *sql.DB
	dialect runstore.Dialect

	cas   cas.Store
	blobs blobstore.Store
	// quotas is the tenant admission check the guarded CAS and the scheduler
	// both answer to. One enforcer over one store, so a tenant's storage and
	// its concurrency are measured against the same limits row.
	quotas *tenancy.Enforcer
	// reclaimer credits storage back when the collector reclaims it, so the
	// ledger nets out instead of only ever growing.
	reclaimer *tenancy.Reclaimer
	// cache is the content-addressed step cache, and refs is the reference
	// index that keeps the blobs an entry points at from being collected.
	// Both live in the run store's database, which is what lets a
	// reclamation span the cache and the runs that pin it in one transaction
	// (ADR 0009).
	cache *cache.Cache
	refs  *cas.GC

	// embedded is the in-process NATS server, nil in distributed mode.
	embedded *bus.Embedded
	// engineBus and exec exist only when this process hosts an engine.
	engineBus *bus.NATS
	exec      executor.Executor

	// closers run in reverse order on teardown.
	closers []func()
}

func (i *infra) close() {
	for n := len(i.closers) - 1; n >= 0; n-- {
		i.closers[n]()
	}
	i.closers = nil
}

func (i *infra) onClose(fn func()) { i.closers = append(i.closers, fn) }

// openInfra builds the infrastructure cfg describes, cleaning up whatever it
// already opened if a later step fails.
func openInfra(ctx context.Context, cfg Config) (_ *infra, err error) {
	i := &infra{}
	defer func() {
		if err != nil {
			i.close()
		}
	}()

	if err = i.openStore(ctx, cfg); err != nil {
		return nil, err
	}
	if err = i.openBlobs(cfg); err != nil {
		return nil, err
	}
	i.openCache()
	if err = i.openBus(ctx, cfg); err != nil {
		return nil, err
	}
	if cfg.Mode == ModeEmbedded {
		if err = i.openEngineSide(ctx, cfg); err != nil {
			return nil, err
		}
	}
	return i, nil
}

// openStore opens the run event log and a database/sql handle on the same
// database. The dialect comes from the DSN rather than from the mode: a
// deployment is free to run distributed against SQLite, and it is the DSN that
// says which SQL the definition store has to speak.
func (i *infra) openStore(ctx context.Context, cfg Config) error {
	if isPostgresDSN(cfg.StoreDSN) {
		store, err := runstore.NewPostgres(ctx, cfg.StoreDSN)
		if err != nil {
			return err
		}
		i.store, i.dialect = store, runstore.DialectPostgres
		i.onClose(func() { _ = store.Close() })

		db, err := runstore.OpenPostgres(ctx, cfg.StoreDSN)
		if err != nil {
			return err
		}
		i.db = db
		i.onClose(func() { _ = db.Close() })
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(cfg.StoreDSN), 0o750); err != nil {
		return fmt.Errorf("server: creating the store directory: %w", err)
	}
	store, err := runstore.NewSQLite(cfg.StoreDSN)
	if err != nil {
		return err
	}
	i.store, i.dialect = store, runstore.DialectSQLite
	i.onClose(func() { _ = store.Close() })

	db, err := runstore.OpenSQLite(cfg.StoreDSN)
	if err != nil {
		return err
	}
	i.db = db
	i.onClose(func() { _ = db.Close() })
	return nil
}

// openBlobs puts the content-addressed store and the blob store under one
// root. They are separate stores because they answer different questions: the
// CAS names bytes by their hash, and a log is named before its content exists.
func (i *infra) openBlobs(cfg Config) error {
	for _, dir := range []string{casDir(cfg.BlobRoot), blobDir(cfg.BlobRoot)} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("server: creating %s: %w", dir, err)
		}
	}

	// A store supplied by the caller wins. It is how a distributed deployment
	// points every process at ONE bucket — the plane and its engines are
	// different processes on different machines, and a plane reading its own
	// disk for an artifact an engine wrote to its own is the failure this
	// exists to prevent.
	if cfg.Blobs != nil {
		i.blobs = cfg.Blobs
		// The CAS is built on the same store rather than taken separately.
		// Two stores configured independently is two chances to point half of
		// a deployment at the wrong one.
		i.cas = cas.NewOverBlobs(cfg.Blobs)
		return i.guardBlobs()
	}

	i.cas = cas.NewFilesystem(casDir(cfg.BlobRoot))
	i.blobs = blobstore.NewFilesystem(blobDir(cfg.BlobRoot))
	return i.guardBlobs()
}

// guardBlobs puts the tenant's storage quota in front of the CAS.
//
// It wraps the store BEFORE anything else takes a reference to it — the cache,
// the collector, the API's artifact routes and the scheduler all hold what
// openBlobs left behind — because a guard applied to one holder and not the
// others is a limit with a way round it. MaxCASBytes was enforceable and
// enforced nowhere: tenancy.GuardCAS had no call site in the tree at all.
func (i *infra) guardBlobs() error {
	tenants, err := tenancy.NewStore(i.db, i.dialect)
	if err != nil {
		return err
	}
	enforcer, err := tenancy.NewEnforcer(tenancy.EnforcerConfig{Store: tenants})
	if err != nil {
		return err
	}
	i.quotas = enforcer
	// The other half of the same ledger. Charging for a write without
	// crediting a collection makes storage usage a number that only ever
	// grows, so a tenant who reclaimed a terabyte stayed charged for it and
	// was eventually refused every write against a store that was nearly
	// empty.
	i.reclaimer = tenancy.NewReclaimer(tenants)
	guarded, err := tenancy.GuardCAS(i.cas, enforcer)
	if err != nil {
		return err
	}
	i.cas = guarded
	return nil
}

// openCache builds the step cache and the blob reference index over the run
// store's own database and the CAS opened above. Neither owns a handle: the
// cache and the collector must be able to commit against the same database as
// the run log.
func (i *infra) openCache() {
	i.cache = cache.New(i.db, i.dialect)
	i.refs = &cas.GC{
		Store: i.cas, Runs: i.store, DB: i.db, Dialect: i.dialect,
		// A nil *Reclaimer here is a non-nil interface holding nil, which the
		// collector's `!= nil` check does not catch. Reclaimer.Collected
		// therefore guards its own receiver rather than relying on that — the
		// same trap this package already avoids for executor.Executor, and it
		// is cheaper to be safe in the method than to reason about it here.
		Collected: i.reclaimer,
	}
}

// openBus starts or dials the bus and declares the two durable streams the
// control plane owns.
func (i *infra) openBus(ctx context.Context, cfg Config) error {
	switch cfg.Mode {
	case ModeEmbedded:
		dir := filepath.Join(cfg.BlobRoot, "nats")
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return fmt.Errorf("server: creating the embedded bus directory: %w", err)
		}
		srv, err := bus.StartEmbedded(dir)
		if err != nil {
			return err
		}
		i.embedded = srv
		i.busURL = srv.URL()
		i.onClose(srv.Close)
	case ModeDistributed:
		if cfg.BusURL == "" {
			return errors.New("server: distributed mode needs a BusURL")
		}
		i.busURL = cfg.BusURL
	default:
		return fmt.Errorf("server: unknown mode %q", cfg.Mode)
	}

	plane, err := bus.Connect(ctx, i.busURL)
	if err != nil {
		return err
	}
	i.plane = plane
	i.onClose(plane.Close)

	// Dispatch is a work queue: exactly one engine takes each message, and an
	// unacknowledged one comes back. Status is durable for the opposite
	// reason — the plane must not miss one (docs/wire-contract.md).
	if err := plane.EnsureWorkQueue(ctx, engine.DispatchStream, []string{"job.dispatch.>"}); err != nil {
		return err
	}
	if err := plane.EnsureWorkQueue(ctx, StatusStream, []string{"job.status.>"}); err != nil {
		return err
	}

	conn, err := nats.Connect(i.busURL)
	if err != nil {
		return fmt.Errorf("server: connecting for KV: %w", err)
	}
	i.conn = conn
	i.onClose(conn.Close)
	return nil
}

// openEngineSide gives the hosted engine its OWN connection to the bus.
//
// Sharing the plane's connection would be free and would be the first step
// back towards an in-process shortcut: two halves that share a client are two
// halves that can start sharing other things. This one dials the bus exactly
// as a remote engine does.
func (i *infra) openEngineSide(ctx context.Context, cfg Config) error {
	engineBus, err := bus.Connect(ctx, i.busURL)
	if err != nil {
		return err
	}
	i.engineBus = engineBus
	i.onClose(engineBus.Close)

	// The backend is the deployment's choice; the host process executor is
	// what a deployment that has not chosen gets. It is named here rather than
	// hard-wired because the environment identity a backend reports is what
	// decides whether ANYTHING is cacheable, and a process executor honestly
	// reports it has none — so a deployment whose steps run somewhere
	// reproducible has to be able to say so by supplying that backend.
	//
	// Nothing here reads that identity. The hosted engine announces it on its
	// registration exactly as an engine in another datacentre does, and the
	// scheduler takes it from the tier's fleet (ADR 0021) — one path, whether
	// or not the engine happens to share this process.
	i.exec = cfg.Executor
	if i.exec == nil {
		i.exec = process.New()
	}
	// The same declaration the standalone engine accepts, for the same reason
	// and by the same name. A single binary on a reproducible host can cache;
	// one on a laptop must not, and only its operator can tell the two apart
	// (ADR 0021, internal/executor.WithDeclaredEnvironment).
	if cfg.EnvironmentIdentity != "" {
		declared, err := executor.WithDeclaredEnvironment(i.exec, cfg.EnvironmentIdentity)
		if err != nil {
			return err
		}
		i.exec = declared
	}
	return nil
}

func casDir(root string) string  { return filepath.Join(root, "cas") }
func blobDir(root string) string { return filepath.Join(root, "blobs") }

// isPostgresDSN reports whether the DSN names Postgres. Anything else is a
// SQLite file path, which is what a single-binary deployment passes.
func isPostgresDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
}
