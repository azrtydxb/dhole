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

	// violations carries the server's ASYNCHRONOUS permission refusals to
	// whoever is waiting on a call that one of them just killed. See
	// violationWatch.
	violations violationWatch
}

// violationWatch fans one async permission error out to the calls waiting for
// it. The server answers a forbidden PUBLISH with an -ERR on the connection
// rather than by replying to the request, so a JetStream API call that is
// refused simply never gets an answer: without this, creating a consumer the
// credentials do not cover blocked until the caller's deadline and then
// reported "context deadline exceeded" — a permissions failure wearing a
// timeout's clothes, on the path that enforces the tier boundary.
type violationWatch struct {
	sync.Mutex
	waiting map[chan error]struct{}
}

// watch registers a listener for permission violations and returns it with the
// function that removes it. Buffered by one: the notifier never blocks on a
// listener that has already given up.
func (w *violationWatch) watch() (<-chan error, func()) {
	ch := make(chan error, 1)
	w.Lock()
	if w.waiting == nil {
		w.waiting = map[chan error]struct{}{}
	}
	w.waiting[ch] = struct{}{}
	w.Unlock()
	return ch, func() {
		w.Lock()
		delete(w.waiting, ch)
		w.Unlock()
	}
}

func (w *violationWatch) notify(err error) {
	w.Lock()
	defer w.Unlock()
	for ch := range w.waiting {
		select {
		case ch <- err:
		default:
		}
	}
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

	conn, err := nats.Connect(url, nats.Timeout(connectTimeout),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			if err != nil && errors.Is(err, nats.ErrPermissionViolation) {
				n.violations.notify(err)
			}
		}))
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

// RequestRaw sends bytes on subject and returns the reply's bytes unchanged.
//
// It exists for the one exchange whose payload must not be wrapped in a
// protobuf message: secret redemption (docs/wire-contract.md, "Secrets"). A
// redeemed value is opaque bytes, and every field of a wrapper message would
// be one more copy of it — in a decoder's arena, in a reflection path, in
// anything that logs an undecodable message by dumping it. The reply carries
// the value and nothing else.
//
// ctx must carry a deadline; without one a dead responder stalls the caller.
func (n *NATS) RequestRaw(ctx context.Context, subject string, body []byte) ([]byte, error) {
	reply, err := n.conn.RequestWithContext(ctx, subject, body)
	if err != nil {
		return nil, fmt.Errorf("bus: request %q: %w", subject, err)
	}
	return reply.Data, nil
}

// RespondRaw serves byte requests on subject until the returned function is
// called. It is the receiving half of RequestRaw.
func (n *NATS) RespondRaw(ctx context.Context, subject string, fn func([]byte) []byte) (func(), error) {
	sub, err := n.conn.Subscribe(subject, func(m *nats.Msg) {
		_ = m.Respond(fn(m.Data))
	})
	if err != nil {
		return nil, fmt.Errorf("bus: respond on %q: %w", subject, err)
	}
	if err := n.confirmSubscribed(ctx, sub, subject); err != nil {
		return nil, err
	}
	return func() { _ = sub.Unsubscribe() }, nil
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

// SubscribeEphemeralOnSubjects is SubscribeEphemeral over a subject pattern
// covering more than one message type, handing the handler the subject each
// payload arrived on.
//
// It exists because ORDER between related subjects is sometimes load-bearing
// and a separate subscription per subject does not preserve it: the client
// delivers each subscription on its own goroutine, so an engine's registration
// and the heartbeat it sends immediately afterwards can be handled the wrong
// way round. One subscription is one delivery goroutine, and the sender's
// order survives.
func (n *NATS) SubscribeEphemeralOnSubjects(ctx context.Context, pattern string, fn func(subject string, data []byte)) (func(), error) {
	sub, err := n.conn.Subscribe(pattern, func(m *nats.Msg) { fn(m.Subject, m.Data) })
	if err != nil {
		return nil, fmt.Errorf("bus: subscribe %q: %w", pattern, err)
	}
	if err := n.confirmSubscribed(ctx, sub, pattern); err != nil {
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(func() { _ = sub.Unsubscribe() }) }, nil
}

// SubscribePull binds the durable pull consumer named consumer on stream,
// filtered to subject. Nothing is removed from the stream until a delivery is
// acknowledged, so a subscriber that dies mid-message gives the work back.
func (n *NATS) SubscribePull(ctx context.Context, stream, consumer, subject string) (Subscription, error) {
	// The tier boundary is enforced on this call, by the server: the create
	// travels on `$JS.API.CONSUMER.CREATE.<stream>.<consumer>.<subject>`, and
	// a tier's credentials permit that subject only under its own tier's
	// dispatch subtree. A refusal arrives as an -ERR on the connection and
	// never as a reply, so it is watched for rather than returned.
	violation, stop := n.violations.watch()
	defer stop()

	type result struct {
		cons jetstream.Consumer
		err  error
	}
	done := make(chan result, 1)
	go func() {
		cons, err := n.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
			Durable:       consumer,
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       n.ackWait,
		})
		done <- result{cons: cons, err: err}
	}()

	for {
		select {
		case res := <-done:
			if res.err != nil {
				return nil, fmt.Errorf("bus: consumer %q on %q: %w", consumer, stream, res.err)
			}
			return &pullSubscription{consumer: res.cons, closed: make(chan struct{})}, nil
		case err := <-violation:
			// Match on the subject so a SIBLING call's refusal, arriving on
			// the same connection, is not reported as this one's — and keep
			// waiting when it is not ours, since the refusal for this call
			// may still be on its way.
			if strings.Contains(err.Error(), subject) {
				return nil, fmt.Errorf("%w: consumer %q on %q filtered to %q: %v",
					ErrPermissionDenied, consumer, stream, subject, err)
			}
		}
	}
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
	// The flush needs a deadline of its own, and the caller's context is the
	// wrong place to get one: it governs how long the SUBSCRIPTION lives, and
	// a live log tail deliberately lives as long as the viewer's connection —
	// no deadline at all. FlushWithContext refuses such a context outright,
	// which meant every live log subscription in the product failed with
	// "context requires a deadline" and fell back to nothing.
	//
	// So the round trip is bounded here, where the bound belongs, while
	// cancellation still follows the caller.
	flushCtx, cancelFlush := context.WithTimeout(ctx, flushTimeout)
	defer cancelFlush()
	flushErr := n.conn.FlushWithContext(flushCtx)
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

// flushTimeout bounds the server round trip that confirms a subscription. It
// is a round trip to a bus this process is already connected to: generous
// enough to survive a slow moment, short enough that a viewer waiting on a log
// is not left staring at nothing.
const flushTimeout = 10 * time.Second

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

// Health reports whether this connection can carry a message right now.
//
// It exists for the readiness probe. The client reconnects on its own, so a
// brief disconnection is not a reason to restart anything — but a plane that
// is not connected accepts runs it cannot dispatch, which presents as a
// working control plane with a queue that never moves. Readiness is exactly
// the signal that should say so.
//
// IsConnected is false while the client is reconnecting, which is the answer
// this wants: during that window the plane genuinely cannot publish.
func (n *NATS) Health() error {
	switch {
	case n == nil || n.conn == nil:
		return errors.New("bus: no connection")
	case n.conn.IsClosed():
		return errors.New("bus: connection is closed")
	case !n.conn.IsConnected():
		return errors.New("bus: not connected")
	}
	return nil
}
