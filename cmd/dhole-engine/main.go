// Command dhole-engine runs a Dhole engine: it dials the bus, announces
// itself, and works the dispatches for its trust tier.
//
// Every knob is an environment variable and every one of them is outbound. An
// engine opens no port and needs no inbound firewall rule, which is what lets
// it live on a laptop, behind NAT, or inside a customer's network.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/version"
)

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "dhole-engine: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// SIGINT and SIGTERM cancel the context, which is how an engine is asked
	// to stop: in-flight dispatches go unacknowledged and come back to
	// somebody else rather than disappearing.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	busURL := os.Getenv("DHOLE_BUS_URL")
	if busURL == "" {
		return errors.New("DHOLE_BUS_URL is required")
	}
	engineID := os.Getenv("DHOLE_ENGINE_ID")
	if engineID == "" {
		return errors.New("DHOLE_ENGINE_ID is required")
	}
	tier := os.Getenv("DHOLE_TIER")
	if tier == "" {
		return errors.New("DHOLE_TIER is required")
	}

	slots, err := positiveEnv("DHOLE_SLOTS", 1)
	if err != nil {
		return err
	}

	// Chosen before the bus is dialled. A misconfigured backend is a start-up
	// error, and finding it after the engine has announced itself would mean
	// a fleet member that exists, advertises slots, and fails every step it
	// is given.
	exec, err := chooseExecutor()
	if err != nil {
		return err
	}

	// The stores an engine writes through. They default under one state
	// directory so a development engine starts with three variables set.
	stateDir := envOr("DHOLE_STATE_DIR", filepath.Join(os.TempDir(), "dhole-engine"))
	blobDir := filepath.Join(stateDir, "blobs")

	// One store, and the content-addressed store is built on it rather than
	// beside it. An engine whose logs went to a bucket and whose artifacts
	// went to a local disk would half-work in exactly the way that is hardest
	// to see: every step succeeds and half of what it produced is unreachable.
	blobs, shared, err := blobstore.FromEnv(blobDir)
	if err != nil {
		return err
	}
	// A remote engine on a local store is the shape of the bug this warns
	// about: the bytes land on this pod's disk and the control plane, which
	// is somewhere else, looks for them on its own. It is a warning rather
	// than a refusal because a single-host deployment is legitimate and does
	// not deserve to be blocked by a check about a cluster.
	if !shared {
		slog.Warn("this engine writes to a store no other process can read; "+
			"set DHOLE_OBJECT_STORE=s3 for anything distributed",
			"dir", blobDir)
	}

	// An engine and its bus start together, so the first dial routinely fails.
	// Exiting there hands the fleet to Kubernetes' restart backoff, which grows
	// to five minutes — an engine absent for minutes after every bus restart.
	var conn *bus.NATS
	if err := connectWithRetry(ctx, func(ctx context.Context) error {
		c, err := bus.Connect(ctx, busURL)
		if err != nil {
			return err
		}
		conn = c
		return nil
	}, busDialBackoff); err != nil {
		return err
	}
	defer conn.Close()

	agent, err := engine.New(engine.Config{
		EngineID: engineID,
		Tier:     tier,
		Bus:      conn,
		Executor: exec,
		Blobs:    blobs,
		CAS:      cas.NewOverBlobs(blobs),
		Slots:    slots,
	})
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stdout, "dhole-engine %s (%s): engine %q in tier %q, %d slots, %s executor\n",
		version.Version(), version.Commit(), engineID, tier, slots, exec.Kind())

	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func positiveEnv(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, raw)
	}
	return v, nil
}

// busDialBackoff is how long to wait between dials while the bus comes up.
// Short, because the common case is a bus that is seconds away, and bounded
// only by the context so a shutdown is never delayed by a retry.
const busDialBackoff = 2 * time.Second

// connectWithRetry keeps dialling until it succeeds or ctx ends.
//
// It reports the context's error rather than the last dial's when it gives up,
// because "the engine was told to stop" and "the bus never came back" are
// different operational stories and the log has to tell them apart.
func connectWithRetry(ctx context.Context, dial func(context.Context) error, backoff time.Duration) error {
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := dial(ctx)
		if err == nil {
			return nil
		}
		slog.Warn("bus is not reachable yet; retrying",
			"attempt", attempt, "backoff", backoff, "error", err)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
