package api

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
)

// Publisher is the bus, narrowed to the one thing reaching an engine needs.
// bus.Bus satisfies it, and so does a recorder in a test.
type Publisher interface {
	Publish(ctx context.Context, subject string, msg proto.Message) error
}

// BusControl reaches an engine the only way an engine can be reached.
//
// Engines are outbound-only: they dial the bus, and nothing dials them
// (docs/wire-contract.md). The single inbound path is
// `engine.control.<engine-id>`, which is what this publishes on — the subject
// comes from internal/bus rather than being spelled here, because a control
// plane and an engine disagreeing about the subject would look exactly like an
// engine ignoring the message.
type BusControl struct{ Bus Publisher }

// NewBusControl returns the bus-backed EngineControl.
func NewBusControl(b Publisher) *BusControl { return &BusControl{Bus: b} }

// Send publishes one control message to one engine.
func (c *BusControl) Send(ctx context.Context, engineID string, control *dholev1.EngineControl) error {
	if c == nil || c.Bus == nil {
		return errors.New("api: no bus to reach an engine on")
	}
	if engineID == "" {
		return errors.New("api: an engine id is required to send a control message")
	}
	if err := c.Bus.Publish(ctx, bus.SubjectEngineControl(engineID), control); err != nil {
		return fmt.Errorf("api: control %q: %w", engineID, err)
	}
	return nil
}
