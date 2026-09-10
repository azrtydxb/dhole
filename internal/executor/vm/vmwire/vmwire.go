// Package vmwire is the protocol spoken between the VM executor on the host
// and the guest agent inside a microVM.
//
// It exists as its own package because both ends must agree on it and they are
// different programs: the host end is linked into the engine, the guest end is
// a static binary baked into the rootfs. A protocol defined in one of them and
// re-implemented in the other is a protocol that drifts.
//
// The shape is framed messages over a multiplexed stream. Multiplexing is not
// optional: the executor contract reads a file out of the sandbox WHILE a
// command is running in it — that is how it proves a cancelled step left no
// grandchild behind — so a single request-at-a-time channel would deadlock the
// suite rather than fail it.
package vmwire

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Port is the guest vsock port the agent listens on. It is above the
// privileged range and fixed, because the host has no way to ask the guest
// which port it chose.
const Port uint32 = 1024

// MaxFrame caps one frame's payload. It bounds what a guest can make the host
// allocate from a single length field, which is the whole of the trust
// relationship here: the guest ran the step, and the step is the untrusted
// thing.
const MaxFrame = 1 << 20

// Op is what a stream is for. One stream carries one operation.
type Op string

const (
	// OpExec runs a command. The stream then carries stdin, signals and
	// cancellation towards the guest, and stdout, stderr and the exit code
	// back.
	OpExec Op = "exec"
	// OpPut writes a file into the sandbox.
	OpPut Op = "put"
	// OpGet reads a file back out.
	OpGet Op = "get"
	// OpMkdir creates a directory and its parents.
	OpMkdir Op = "mkdir"
	// OpSignal delivers a signal to everything the sandbox is running.
	OpSignal Op = "signal"
)

// Request is the header every stream opens with.
type Request struct {
	Op Op `json:"op"`
	// Args is the full argument vector for OpExec; Args[0] is the program.
	Args []string `json:"args,omitempty"`
	// Env is added to, and overrides, the sandbox environment.
	Env map[string]string `json:"env,omitempty"`
	// Dir is relative to the sandbox root, for OpExec.
	Dir string `json:"dir,omitempty"`
	// Name is the path, relative to the sandbox root, for OpPut, OpGet and
	// OpMkdir.
	Name string `json:"name,omitempty"`
	// Signal is the backend-neutral signal name for OpSignal, and for the
	// FrameSignal an exec stream may carry.
	Signal string `json:"signal,omitempty"`
}

// FrameType tags a frame. Values are on one number line across both
// directions so a frame arriving on the wrong side is a protocol error rather
// than a plausible-looking other message.
type FrameType byte

const (
	// FrameHeader carries the JSON Request that opens a stream.
	FrameHeader FrameType = 1
	// FrameData carries payload bytes towards the guest: stdin for OpExec,
	// file content for OpPut.
	FrameData FrameType = 2
	// FrameEOF ends the client's half. There is no half-close on a
	// multiplexed stream, so the end of input is a message like any other.
	FrameEOF FrameType = 3
	// FrameSignal asks the guest to signal the running command tree.
	FrameSignal FrameType = 4
	// FrameStdout carries the command's standard output back.
	FrameStdout FrameType = 5
	// FrameStderr carries the command's standard error back, kept apart from
	// stdout because a step's output ports are built from one and not the
	// other.
	FrameStderr FrameType = 6
	// FrameExit carries the exit code, big-endian int32.
	FrameExit FrameType = 7
	// FrameStatus ends every stream: an empty payload is success, anything
	// else is the error message. A stream that ends without one ended because
	// something broke, which is why it is a distinct case from a failure the
	// guest reported.
	FrameStatus FrameType = 8
)

// ErrFrameTooLarge is returned when a peer announces a payload above MaxFrame.
var ErrFrameTooLarge = errors.New("vmwire: frame exceeds the maximum size")

// Conn is one multiplexed stream, safe for a reader and several writers. The
// writers are real: an exec streams stdout and stderr from two goroutines and
// then writes the exit code from a third.
type Conn struct {
	rw io.ReadWriteCloser
	mu sync.Mutex
	rb [5]byte
}

// NewConn wraps a stream.
func NewConn(rw io.ReadWriteCloser) *Conn { return &Conn{rw: rw} }

// Write sends one frame.
func (c *Conn) Write(t FrameType, payload []byte) error {
	if len(payload) > MaxFrame {
		return ErrFrameTooLarge
	}
	var hdr [5]byte
	hdr[0] = byte(t)
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload))) // #nosec G115 -- MaxFrame is checked above
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.rw.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := c.rw.Write(payload)
	return err
}

// WriteJSON sends a frame whose payload is v encoded as JSON.
func (c *Conn) WriteJSON(t FrameType, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Write(t, b)
}

// WriteStatus ends the stream: nil is success, an error is its message.
func (c *Conn) WriteStatus(err error) error {
	if err == nil {
		return c.Write(FrameStatus, nil)
	}
	return c.Write(FrameStatus, []byte(err.Error()))
}

// Read returns the next frame. It is called from ONE goroutine.
func (c *Conn) Read() (FrameType, []byte, error) {
	if _, err := io.ReadFull(c.rw, c.rb[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(c.rb[1:])
	if n > MaxFrame {
		return 0, nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, n)
	}
	if n == 0 {
		return FrameType(c.rb[0]), nil, nil
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.rw, payload); err != nil {
		return 0, nil, err
	}
	return FrameType(c.rb[0]), payload, nil
}

// ReadRequest reads the header frame that opens a stream.
func (c *Conn) ReadRequest() (Request, error) {
	t, payload, err := c.Read()
	if err != nil {
		return Request{}, err
	}
	if t != FrameHeader {
		return Request{}, fmt.Errorf("vmwire: first frame is %d, want a header", t)
	}
	var req Request
	if err := json.Unmarshal(payload, &req); err != nil {
		return Request{}, fmt.Errorf("vmwire: malformed request header: %w", err)
	}
	return req, nil
}

// Close closes the underlying stream.
func (c *Conn) Close() error { return c.rw.Close() }

// SandboxRoot is where a sandbox's files live inside the guest. Every path in
// this protocol is relative to it, and the agent refuses to leave it.
const SandboxRoot = "/dhole/work"

// SignalCancel is the FrameSignal payload meaning "the step's context was
// cancelled". It is deliberately not one of the executor's signal names: the
// guest answers it by asking politely and then insisting, and no caller chose
// either of those.
const SignalCancel = "CANCEL"
