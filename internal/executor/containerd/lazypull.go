// Package containerd will hold the containerd/OCI executor (Task 36, blocked
// on this machine for want of a container runtime). What lives here now is the
// half of Task 38 that is about pull cost rather than about running anything:
// deciding whether an image can be mounted lazily.
//
// Lazy pull is one of the levers ADR 0006 names for making containers feel
// fast without adding a backend to work around a caching deficiency. With the
// stargz snapshotter plugged into containerd, a container starts on an
// eStargz image after fetching its table of contents and the few chunks the
// command actually touches, so a 500MB image running `true` reads a few
// megabytes. Without it, the runtime pulls and unpacks every layer first.
package containerd

import (
	"context"
	"log/slog"
	"slices"
)

const (
	// StargzSnapshotter is the containerd snapshotter that mounts eStargz
	// images lazily, fetching chunks on demand.
	StargzSnapshotter = "stargz"
	// DefaultSnapshotter is the ordinary overlay snapshotter: correct
	// everywhere, and it pulls the whole image before the container starts.
	DefaultSnapshotter = "overlayfs"
)

// SnapshotterLister reports the snapshotters a containerd daemon has plugged
// in. It is a function rather than a client so this decision can be made — and
// tested — without a daemon, which matters because the daemon is exactly what
// is unavailable in some environments.
type SnapshotterLister func(ctx context.Context) ([]string, error)

// PullMode is how images will be fetched: which snapshotter to ask containerd
// for, and whether that snapshotter fetches lazily.
type PullMode struct {
	// Snapshotter is the name to pass to containerd on pull and on container
	// creation. Both must agree, or the container is created against a
	// snapshot the puller never wrote.
	Snapshotter string
	// Lazy is true when the image is mounted on demand rather than pulled
	// whole. Callers record it on the run so a slow start can be explained.
	Lazy bool
}

// SelectPullMode picks the snapshotter to pull with. It enables lazy pull when
// the daemon actually has the stargz snapshotter, and otherwise falls back to
// a full pull with a WARNING — the fallback is silent-by-nature otherwise, and
// a silent fallback presents as "the pipeline got slower for no reason", with
// nothing anywhere to read. The warning names what is missing so an operator
// can install it.
//
// Anything short of a confirmed stargz snapshotter is a fallback, including a
// daemon that could not be asked: assuming lazy capability from an error would
// have containerd fail the pull with an unknown snapshotter instead.
func SelectPullMode(ctx context.Context, list SnapshotterLister, log *slog.Logger) PullMode {
	if log == nil {
		log = slog.Default()
	}
	full := PullMode{Snapshotter: DefaultSnapshotter, Lazy: false}

	if list == nil {
		log.Warn("lazy image pull unavailable, falling back to a full image pull",
			"want_snapshotter", StargzSnapshotter,
			"reason", "no way to list containerd snapshotters was configured")
		return full
	}
	available, err := list(ctx)
	if err != nil {
		log.Warn("lazy image pull unavailable, falling back to a full image pull",
			"want_snapshotter", StargzSnapshotter,
			"reason", err.Error())
		return full
	}
	if !slices.Contains(available, StargzSnapshotter) {
		log.Warn("lazy image pull unavailable, falling back to a full image pull",
			"want_snapshotter", StargzSnapshotter,
			"reason", "containerd has no such snapshotter",
			"available", available)
		return full
	}
	log.Debug("lazy image pull enabled", "snapshotter", StargzSnapshotter)
	return PullMode{Snapshotter: StargzSnapshotter, Lazy: true}
}
