// Package bus carries transport between the control plane and its engines.
//
// The bus is NOT the source of truth. Durable run state lives in the run store
// and reaches the bus through an outbox (ADR 0005): a publish here says a
// message was handed to NATS, never that anything was committed. Nothing in
// this interface returns a sequence, an offset, or anything else a caller could
// mistake for a durable position — if you need to know a thing happened, read
// the store, not this package.
package bus

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Bus is the transport the control plane and engines share. Every call takes a
// context and must honour its deadline: a bus call that can hang forever hangs
// the scheduler.
type Bus interface {
	// Publish sends msg on subject. It returns when the message has been
	// accepted for delivery — not when anyone has processed it.
	Publish(ctx context.Context, subject string, msg proto.Message) error

	// Request sends msg on subject and unmarshals the single reply into out.
	Request(ctx context.Context, subject string, msg proto.Message, out proto.Message) error

	// SubscribePull binds a durable pull consumer named consumer on stream,
	// filtered to subject. Messages are redelivered until acknowledged, which
	// is what lets an engine that dies mid-step have its work come back.
	SubscribePull(ctx context.Context, stream, consumer, subject string) (Subscription, error)

	// SubscribeEphemeral delivers raw payloads on subject at most once, with no
	// durability at all. It is for live logs and other best-effort traffic. The
	// returned function cancels the subscription and never blocks forever.
	SubscribeEphemeral(ctx context.Context, subject string, fn func([]byte)) (func(), error)
}

// Subscription is a pull consumer's message stream. Close is idempotent.
type Subscription interface {
	// Next blocks until a message arrives or ctx is done.
	Next(ctx context.Context) (Message, error)
	Close() error
}

// Message is one delivery from a pull consumer. A message that is never
// acknowledged is delivered again after the consumer's ack wait elapses.
type Message interface {
	Data() []byte
	// Ack tells the server this delivery is finished. An engine acks only
	// after its terminal status is published; acking first would lose the step
	// silently on a crash between the two.
	Ack() error
	// Nak returns the message for immediate redelivery.
	Nak() error
}
