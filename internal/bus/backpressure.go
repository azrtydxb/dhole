package bus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Backpressure is the other half of scaling the control plane out. Partitioning
// decides WHICH work an instance is responsible for; this decides how much of
// it an instance may hold at once.
//
// A consumer that keeps pulling work it cannot start is how a slow plane turns
// into a dead one. Every message it holds unacknowledged is doing nothing: it
// is not being worked on, and it is not available to a plane that could work on
// it, until the ack wait elapses and the server hands it to somebody else — by
// which time this plane has pulled more. Bounding the outstanding set is what
// makes an overloaded plane slow instead of a black hole, and it is the same
// mechanism fair-share scheduling leans on (ADR 0005).
//
// The bound is applied in two places on purpose. The server enforces
// MaxAckPending, which is what protects the STREAM from a client that ignores
// its own limit. The client refuses to fetch while it is full, which is what
// stops it spinning a request at the server every fetch wait for as long as the
// overload lasts.

// defaultMaxAckPending is the bound a bounded subscription uses when the caller
// names none. It is not "unlimited": the whole point of taking this path is to
// have a limit.
const defaultMaxAckPending = 64

// SubOption tunes one subscription.
type SubOption func(*subOptions)

type subOptions struct {
	maxAckPending int
}

// WithMaxAckPending bounds how many deliveries a subscription may hold
// unacknowledged at once. Below one it is ignored, because a consumer that may
// hold nothing can never make progress.
func WithMaxAckPending(n int) SubOption {
	return func(o *subOptions) {
		if n >= 1 {
			o.maxAckPending = n
		}
	}
}

// SubscribePullWithOptions is SubscribePull with backpressure. The durable
// consumer is created with MaxAckPending, and the returned subscription will
// not fetch while that many deliveries are outstanding.
func (n *NATS) SubscribePullWithOptions(ctx context.Context, stream, consumer, subject string, opts ...SubOption) (*BoundedSubscription, error) {
	cfg := subOptions{maxAckPending: defaultMaxAckPending}
	for _, opt := range opts {
		opt(&cfg)
	}
	cons, err := n.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       consumer,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       n.ackWait,
		MaxAckPending: cfg.maxAckPending,
	})
	if err != nil {
		return nil, fmt.Errorf("bus: consumer %q on %q: %w", consumer, stream, err)
	}
	return &BoundedSubscription{
		consumer: cons,
		slots:    make(chan struct{}, cfg.maxAckPending),
		closed:   make(chan struct{}),
	}, nil
}

// BoundedSubscription is a pull consumer that holds at most MaxAckPending
// deliveries at a time.
//
// The bound is a slot taken before each fetch and given back when the delivery
// is acknowledged or returned, so a caller that is slow to ack simply stops
// pulling. Next blocks while every slot is held: there is nothing useful to do
// with a message that cannot be started, and blocking is what lets the work
// reach a plane that has room.
type BoundedSubscription struct {
	consumer jetstream.Consumer
	// slots holds one token per outstanding delivery. Taking a token is what
	// permits a fetch; an Ack or a Nak returns it.
	slots     chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	fetches   atomic.Uint64
}

var _ Subscription = (*BoundedSubscription)(nil)

// Next blocks until a message arrives, ctx is done, or the subscription is
// closed. While the ack-pending limit is reached it blocks WITHOUT fetching —
// that is the backpressure.
func (s *BoundedSubscription) Next(ctx context.Context) (Message, error) {
	for {
		select {
		case <-s.closed:
			return nil, ErrSubscriptionClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// The slot comes first. Nothing below this may touch the server until
		// this subscription has room for what it might return.
		select {
		case s.slots <- struct{}{}:
		case <-s.closed:
			return nil, ErrSubscriptionClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		msg, err := s.fetchOne()
		if err != nil {
			s.releaseSlot()
			return nil, err
		}
		if msg == nil {
			// Nothing waiting. The slot goes back before the next attempt, or
			// an idle consumer would leak its whole budget.
			s.releaseSlot()
			continue
		}
		return msg, nil
	}
}

// fetchOne pulls at most one message, returning nil when the fetch came back
// empty. One at a time keeps the outstanding set exactly the size the slots
// say it is.
func (s *BoundedSubscription) fetchOne() (Message, error) {
	s.fetches.Add(1)
	batch, err := s.consumer.Fetch(1, jetstream.FetchMaxWait(fetchWait))
	if err != nil {
		return nil, fmt.Errorf("bus: fetch: %w", err)
	}
	for msg := range batch.Messages() {
		return &boundedMessage{msg: msg, sub: s}, nil
	}
	if err := batch.Error(); err != nil {
		return nil, fmt.Errorf("bus: fetch: %w", err)
	}
	return nil, nil //nolint:nilnil // an empty fetch is neither a message nor a failure
}

func (s *BoundedSubscription) releaseSlot() {
	select {
	case <-s.slots:
	default:
	}
}

// Close is idempotent and unblocks whatever is parked in Next.
func (s *BoundedSubscription) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

// Outstanding is how many deliveries this subscription is holding
// unacknowledged.
func (s *BoundedSubscription) Outstanding() int { return len(s.slots) }

// Fetches is how many times this subscription has asked the server for a
// message. It is what distinguishes a consumer that stopped pulling from one
// that is merely being refused: both look like "no message", and only one of
// them stops loading the server.
func (s *BoundedSubscription) Fetches() uint64 { return s.fetches.Load() }

// boundedMessage returns its slot the first time it is settled, whichever way
// it is settled. A message acknowledged twice must not hand back two slots, or
// the bound drifts upwards over the life of the subscription.
type boundedMessage struct {
	msg    jetstream.Msg
	sub    *BoundedSubscription
	settle sync.Once
}

func (m *boundedMessage) Data() []byte { return m.msg.Data() }

func (m *boundedMessage) Ack() error {
	err := m.msg.Ack()
	// The slot is returned even when the ack was refused: the delivery is
	// finished as far as this subscription is concerned, and holding the slot
	// for a message the server has already redelivered would shrink the
	// consumer's capacity a little at a time until it stalled.
	m.settle.Do(m.sub.releaseSlot)
	if err != nil && !errors.Is(err, nats.ErrMsgAlreadyAckd) {
		return fmt.Errorf("bus: ack: %w", err)
	}
	return nil
}

func (m *boundedMessage) Nak() error {
	err := m.msg.Nak()
	m.settle.Do(m.sub.releaseSlot)
	if err != nil {
		return fmt.Errorf("bus: nak: %w", err)
	}
	return nil
}
