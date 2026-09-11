package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// DispatchStreamPrefix begins the name of every dispatch work queue. The tier
// follows it, and that is the whole point of the name: a JS API subject
// addressing a consumer carries the STREAM as a token
// (`$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>`) but the consumer name only
// as another token, and a NATS wildcard matches a WHOLE token — `engines-
// untrusted-*` is not expressible. With one shared `DISPATCH` stream an
// untrusted connection that guessed a trusted consumer's name pulled trusted
// work off it, creating nothing and so never meeting the consumer-create
// permission. Putting the tier in the stream token makes
// `$JS.API.CONSUMER.MSG.NEXT.DISPATCH_<tier>.*` a tier-scoped permission and
// the consumer name stops mattering.
const DispatchStreamPrefix = "DISPATCH_"

// LegacyDispatchStream is the one shared dispatch stream that existed before
// the split. It is named here only so an upgrade can recognise it; nothing
// creates it any more. See EnsureDispatchStreams.
const LegacyDispatchStream = "DISPATCH"

// maxStreamNameLen is JetStream's JSMaxNameLen. A tier long enough to push the
// stream name past it would be refused by the server at stream creation, which
// is the wrong place and the wrong time to find out.
const maxStreamNameLen = 255

// DispatchStreamName is the work queue carrying one tier's dispatches.
func DispatchStreamName(tier string) (string, error) {
	if err := ValidTierToken(tier); err != nil {
		return "", err
	}
	return DispatchStreamPrefix + tier, nil
}

// ValidTierToken refuses a tier name that cannot be both a single subject
// token and a JetStream stream name.
//
// A STREAM NAME IS NOT A SUBJECT, and the tier is now spelled into both. A
// subject token may not contain `.`, `*`, `>` or whitespace; a stream name may
// not contain those either, and additionally not `/` or `\` (nats-server,
// isValidAssetName), and may not exceed 255 bytes. So `local/dev` is a legal
// subject token and an illegal stream name — the union is what a tier has to
// satisfy, and it is checked HERE, where a tier is configured, rather than at
// the first dispatch of a Friday evening, where it would surface as a stream
// creation error under a scheduler stack trace with the tier nowhere in it.
func ValidTierToken(tier string) error {
	if tier == "" {
		return fmt.Errorf("bus: tier: empty")
	}
	if strings.ContainsAny(tier, ".*> \t\r\n\f/\\") {
		return fmt.Errorf("bus: tier %q: must be a single subject token and a legal "+
			"JetStream stream name: no '.', '*', '>', '/', '\\' or whitespace", tier)
	}
	if len(DispatchStreamPrefix)+len(tier) > maxStreamNameLen {
		return fmt.Errorf("bus: tier %q: too long, the stream name %q%s would exceed %d bytes",
			tier, DispatchStreamPrefix, tier, maxStreamNameLen)
	}
	return nil
}

// EnsureDispatchStreams declares one work-queue stream per tier, and is the
// control plane's job: engine credentials cannot create a stream.
//
// THE OLD STREAM. A `DISPATCH` covering `job.dispatch.>` overlaps every
// per-tier stream's subjects, and JetStream refuses two streams over
// overlapping subjects — so it cannot simply be left in place, and pretending
// it is not there would fail at CreateOrUpdateStream with a message about
// subject overlap that says nothing about an upgrade. It is therefore handled
// explicitly, and which way depends on what is in it:
//
//   - Empty: deleted. It holds no work, and it is nothing but an obstacle.
//   - Non-empty: the plane REFUSES TO START, naming the stream and the count.
//     Those messages are dispatches that steps of live runs are waiting on;
//     deleting them would strand every one of those runs until its lease
//     expired, and would do it silently. Draining it here is not this
//     function's to do either — only the engines that already bound it can,
//     and by the time the plane is new they may already be gone. So the
//     operator is told, and the documented upgrade order (docs/wire-contract.md)
//     is the one that leaves it empty.
func (n *NATS) EnsureDispatchStreams(ctx context.Context, tiers []string) error {
	names := make([]string, 0, len(tiers))
	for _, tier := range tiers {
		name, err := DispatchStreamName(tier)
		if err != nil {
			return err
		}
		names = append(names, name)
	}
	if err := n.retireLegacyDispatchStream(ctx); err != nil {
		return err
	}
	for i, name := range names {
		if err := n.EnsureWorkQueue(ctx, name, []string{SubjectDispatchWildcard(tiers[i])}); err != nil {
			return err
		}
	}
	return nil
}

func (n *NATS) retireLegacyDispatchStream(ctx context.Context) error {
	stream, err := n.js.Stream(ctx, LegacyDispatchStream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("bus: looking for the %s stream: %w", LegacyDispatchStream, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("bus: inspecting the %s stream: %w", LegacyDispatchStream, err)
	}
	if info.State.Msgs > 0 {
		return fmt.Errorf("bus: the %s stream still holds %d dispatch(es): it covers "+
			"job.dispatch.> and so overlaps every %s<tier> stream, but deleting it would "+
			"strand the runs those dispatches belong to — let the engines still bound to it "+
			"drain it, then delete it, and start this plane again",
			LegacyDispatchStream, info.State.Msgs, DispatchStreamPrefix)
	}
	if err := n.js.DeleteStream(ctx, LegacyDispatchStream); err != nil {
		return fmt.Errorf("bus: removing the empty %s stream: %w", LegacyDispatchStream, err)
	}
	n.mu.Lock()
	n.mu.bySubject = map[string]string{}
	n.mu.Unlock()
	return nil
}
