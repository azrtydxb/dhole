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
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor/process"
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

	// The stores an engine writes through. They default under one state
	// directory so a development engine starts with three variables set.
	stateDir := envOr("DHOLE_STATE_DIR", filepath.Join(os.TempDir(), "dhole-engine"))
	blobDir := envOr("DHOLE_BLOB_DIR", filepath.Join(stateDir, "blobs"))
	casDir := envOr("DHOLE_CAS_DIR", filepath.Join(stateDir, "cas"))

	conn, err := bus.Connect(ctx, busURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	agent, err := engine.New(engine.Config{
		EngineID: engineID,
		Tier:     tier,
		Bus:      conn,
		Executor: process.New(),
		Blobs:    blobstore.NewFilesystem(blobDir),
		CAS:      cas.NewFilesystem(casDir),
		Slots:    slots,
	})
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(os.Stdout, "dhole-engine %s (%s): engine %q in tier %q, %d slots\n",
		version.Version(), version.Commit(), engineID, tier, slots)

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
