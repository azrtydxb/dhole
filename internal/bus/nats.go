package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// ErrPermissionDenied wraps every refusal the server hands back for a subject
// the connection's credentials do not cover. An engine reaching for another
// tier's work sees this.
var ErrPermissionDenied = errors.New("bus: permission denied")

// ErrSubscriptionClosed is returned by Next after its Subscription is closed.
var ErrSubscriptionClosed = errors.New("bus: subscription closed")

const (
	// defaultAckWait is how long a dispatch may sit unacknowledged before the
	// server hands it to somebody else. It has to outlast a step's runtime plus
	// the terminal status publish, so engines renew it while they work.
	defaultAckWait = 30 * time.Second
	// fetchWait bounds one Fetch so a cancelled context is noticed promptly
	// rather than at the caller's deadline.
	fetchWait = 2 * time.Second
	// connectTimeout bounds the initial dial.
	connectTimeout = 10 * time.Second
)

type options struct {
	ackWait time.Duration
}

// Option tunes a connection.
type Option func(*options)

// WithAckWait sets how long the server waits for an ack before redelivering.
func WithAckWait(d time.Duration) Option {
	return func(o *options) { o.ackWait = d }
}

// NATS is a Bus over a NATS connection, with JetStream for the durable
// subjects. It is transport only: a successful Publish means NATS accepted the
// bytes, never that a run advanced. Durable state lives in the run store and
// arrives here through the outbox (ADR 0005).
type NATS struct {
	conn    *nats.Conn
	js      jetstream.JetStream
	ackWait time.Duration

	mu      streamCache
	closing sync.Once
}

type streamCache struct {
	sync.Mutex
	bySubject map[string]string
}

var _ Bus = (*NATS)(nil)

// Connect dials url and prepares JetStream. ctx bounds the dial.
func Connect(ctx context.Context, url string, opts ...Option) (*NATS, error) {
	cfg := options{ackWait: defaultAckWait}
	for _, opt := range opts {
		opt(&cfg)
	}

	n := &NATS{
		ackWait: cfg.ackWait,
		mu:      streamCache{bySubject: map[string]string{}},
	}

	conn, err := nats.Connect(url, nats.Timeout(connectTimeout))
	if err != nil {
		return nil, fmt.Errorf("bus: connect: %w", err)
	}
	if err := ctx.Err(); err != nil {
		conn.Close()
		return nil, err
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("bus: jetstream: %w", err)
	}
	n.conn, n.js = conn, js
	return n, nil
}

// Close drains nothing and waits for nothing: callers stop their subscriptions
// first. It is safe to call more than once.
func (n *NATS) Close() {
	n.closing.Do(func() { n.conn.Close() })
}

// EnsureWorkQueue creates or updates a work-queue stream over subjects.
// Exactly one consumer receives each message, and an unacknowledged message is
// redelivered — the property dispatch depends on.
func (n *NATS) EnsureWorkQueue(ctx context.Context, name string, subjects []string) error {
	_, err := n.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  subjects,
		Retention: jetstream.WorkQueuePolicy,
		Storage:   jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("bus: ensure stream %q: %w", name, err)
	}
	n.mu.Lock()
	n.mu.bySubject = map[string]string{}
	n.mu.Unlock()
	return nil
}

// Publish sends msg on subject. When a stream covers the subject the message
// goes through JetStream and Publish returns only once the server has stored
// it; otherwise it is core NATS and best-effort, which is what an ephemeral
// subject such as job.logs.* wants.
//
// Either way this is not a commit. Nothing durable about a run has happened
// because this returned nil.
func (n *NATS) Publish(ctx context.Context, subject string, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("bus: marshal for %q: %w", subject, err)
	}
	stream, err := n.streamFor(ctx, subject)
	if err != nil {
		return err
	}
	if stream != "" {
		if _, err := n.js.Publish(ctx, subject, data); err != nil {
			return fmt.Errorf("bus: publish %q: %w", subject, err)
		}
		return nil
	}
	if err := n.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("bus: publish %q: %w", subject, err)
	}
	return nil
}

// Request sends msg on subject and unmarshals the one reply into out. ctx must
// carry a deadline; without one a dead responder stalls the caller.
func (n *NATS) Request(ctx context.Context, subject string, msg proto.Message, out proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("bus: marshal for %q: %w", subject, err)
	}
	reply, err := n.conn.RequestWithContext(ctx, subject, data)
	if err != nil {
		return fmt.Errorf("bus: request %q: %w", subject, err)
	}
	if err := proto.Unmarshal(reply.Data, out); err != nil {
		return fmt.Errorf("bus: unmarshal reply from %q: %w", subject, err)
	}
	return nil
}

// Respond serves requests on subject until the returned function is called.
// This is the receiving half of Request — how an engine answers EngineControl
// over the connection it dialled.
func (n *NATS) Respond(ctx context.Context, subject string, fn func([]byte) (proto.Message, error)) (func(), error) {
	sub, err := n.conn.Subscribe(subject, func(m *nats.Msg) {
		out, err := fn(m.Data)
		if err != nil {
			return
		}
		data, err := proto.Marshal(out)
		if err != nil {
			return
		}
		_ = m.Respond(data)
	})
	if err != nil {
		return nil, fmt.Errorf("bus: respond on %q: %w", subject, err)
	}
	if err := n.confirmSubscribed(ctx, sub, subject); err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
}

// SubscribeEphemeral delivers raw payloads at most once, with no durability.
// The returned stop function is idempotent and never blocks.
func (n *NATS) SubscribeEphemeral(ctx context.Context, subject string, fn func([]byte)) (func(), error) {
	sub, err := n.conn.Subscribe(subject, func(m *nats.Msg) { fn(m.Data) })
	if err != nil {
		return nil, fmt.Errorf("bus: subscribe %q: %w", subject, err)
	}
	if err := n.confirmSubscribed(ctx, sub, subject); err != nil {
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(func() { _ = sub.Unsubscribe() }) }, nil
}

// SubscribePull binds the durable pull consumer named consumer on stream,
// filtered to subject. Nothing is removed from the stream until a delivery is
// acknowledged, so a subscriber that dies mid-message gives the work back.
func (n *NATS) SubscribePull(ctx context.Context, stream, consumer, subject string) (Subscription, error) {
	cons, err := n.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:       consumer,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       n.ackWait,
	})
	if err != nil {
		return nil, fmt.Errorf("bus: consumer %q on %q: %w", consumer, stream, err)
	}
	return &pullSubscription{consumer: cons, closed: make(chan struct{})}, nil
}

// confirmSubscribed turns the server's refusal into a synchronous error, so a
// subscription the credentials do not cover fails at the call site instead of
// silently never delivering anything.
//
// The server answers a forbidden SUB with -ERR and the flush's PONG after it,
// both handled on the client's one read loop, so once the flush returns the
// refusal has already been recorded on the connection. Matching the subject
// keeps an unrelated earlier error from being blamed on this call.
func (n *NATS) confirmSubscribed(ctx context.Context, sub *nats.Subscription, subject string) error {
	flushErr := n.conn.FlushWithContext(ctx)
	if err := n.conn.LastError(); err != nil &&
		errors.Is(err, nats.ErrPermissionViolation) &&
		strings.Contains(err.Error(), subject) {
		_ = sub.Unsubscribe()
		return fmt.Errorf("%w: %q: %v", ErrPermissionDenied, subject, err)
	}
	if flushErr != nil {
		_ = sub.Unsubscribe()
		return fmt.Errorf("bus: subscribe %q: %w", subject, flushErr)
	}
	return nil
}

// streamFor names the stream covering subject, or "" when none does.
func (n *NATS) streamFor(ctx context.Context, subject string) (string, error) {
	n.mu.Lock()
	cached, ok := n.mu.bySubject[subject]
	n.mu.Unlock()
	if ok {
		return cached, nil
	}

	name, err := n.js.StreamNameBySubject(ctx, subject)
	switch {
	case err == nil:
	case errors.Is(err, jetstream.ErrStreamNotFound):
		name = ""
	default:
		return "", fmt.Errorf("bus: resolving stream for %q: %w", subject, err)
	}

	n.mu.Lock()
	n.mu.bySubject[subject] = name
	n.mu.Unlock()
	return name, nil
}

// pullSubscription pulls one message at a time. Fetching a single message keeps
// the unacknowledged set small: whatever this subscription holds when it dies
// is what has to be redelivered.
type pullSubscription struct {
	consumer  jetstream.Consumer
	closed    chan struct{}
	closeOnce sync.Once
}

func (s *pullSubscription) Next(ctx context.Context) (Message, error) {
	for {
		select {
		case <-s.closed:
			return nil, ErrSubscriptionClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		batch, err := s.consumer.Fetch(1, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			return nil, fmt.Errorf("bus: fetch: %w", err)
		}
		for msg := range batch.Messages() {
			return &jsMessage{msg: msg}, nil
		}
		if err := batch.Error(); err != nil {
			return nil, fmt.Errorf("bus: fetch: %w", err)
		}
	}
}

func (s *pullSubscription) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

type jsMessage struct {
	msg jetstream.Msg
}

func (m *jsMessage) Data() []byte { return m.msg.Data() }
func (m *jsMessage) Ack() error   { return m.msg.Ack() }
func (m *jsMessage) Nak() error   { return m.msg.Nak() }
