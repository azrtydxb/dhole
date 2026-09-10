package server

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/azrtydxb/dhole/internal/registry"
)

// A plane restart made every engine still running against it log an ERROR,
// repeatedly, while the fleet recovered exactly as the wire contract says it
// should. A log that reports correct recovery as failure is a log nobody reads
// by the time something is actually wrong.
func TestAnEngineThatMustReannounceIsNotAnError(t *testing.T) {
	buf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	logHeartbeatRefusal(log, "engine-7", registry.ErrNotRegistered)

	out := buf.String()
	if strings.Contains(out, "level=ERROR") {
		t.Errorf("a routine re-announcement was logged as an error: %s", out)
	}
	if !strings.Contains(out, "engine-7") {
		t.Errorf("the log does not say which engine: %s", out)
	}
}

// The other half, and the one that makes the change safe: a refusal that is
// NOT the self-healing one must still be loud.
func TestARealHeartbeatRefusalIsStillAnError(t *testing.T) {
	buf := &bytes.Buffer{}
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	logHeartbeatRefusal(log, "engine-7", errors.New("the registry bucket is gone"))

	out := buf.String()
	if !strings.Contains(out, "level=ERROR") {
		t.Errorf("a real refusal was demoted below error: %s", out)
	}
	if !strings.Contains(out, "the registry bucket is gone") {
		t.Errorf("the log does not carry the reason: %s", out)
	}
}
