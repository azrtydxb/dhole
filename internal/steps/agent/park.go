package agent

// Where a parked agent's position lives.
//
// A run is a state machine driven by a persisted event log, never a goroutine
// (ADR 0003), and an agent's tool-calling loop is no exception: the plane that
// parked the step may be dead, restarted or replaced by the time a person
// decides the gate three days later. So everything needed to re-enter the
// model's loop AT THE CALL IT STOPPED ON is written to the log as one event,
// verbatim, and nothing about the resumed loop is reconstructed from anywhere
// else.
//
// The transcript is stored as it was, not folded out of the log. That is the
// answer to "what happens when an event is added later": nothing. A later
// event cannot change an earlier payload, so a park record read today replays
// exactly as it would have replayed the moment it was written — which a
// transcript rebuilt by re-reading the run's events could not promise.

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// EventParked is the run-log event a parked agent writes: the whole of its
// position, at the moment it stopped. The value is stored verbatim and is
// therefore a persistence contract — add types, never rename one.
const EventParked runstore.EventType = "AGENT_PARKED"

// ParkRecord is EventParked's payload.
//
// It is the conversation and the accounting, and it is deliberately BOTH: a
// resume that restored the conversation but not the step count would hand the
// agent a fresh ceiling every time somebody approved something, which is a way
// around the one bound ADR 0015 gives an agent loop.
type ParkRecord struct {
	// Subject is the agent's own principal, never a name it supplied.
	Subject string `json:"subject"`
	// Actions are the at-most-once actions this gate is being asked about,
	// in call order. There is more than one when the model asked for several
	// in a single batch, and one decision answers the batch — which is what
	// the gate's prompt says.
	Actions []string `json:"actions"`
	// ToolCallIDs are the SDK's ids for those calls. They are what the
	// resumed loop matches a person's decision against, so a decision cannot
	// land on a call it was not made about.
	ToolCallIDs []string `json:"tool_call_ids"`
	// Transcript is the conversation up to and including the unanswered
	// assistant message carrying those calls. It is what the model sees on
	// resume, and it is why the resumed loop never asks for anything it has
	// already done: every earlier call is in here already answered.
	Transcript json.RawMessage `json:"transcript"`
	// StepsUsed is how much of MaxSteps was spent before parking.
	StepsUsed int `json:"steps_used"`
	// Usage is what the model has cost so far, summed across every segment
	// of this loop. Nothing enforces a token ceiling on an agent step today;
	// this is here so the run log answers "what did the parked agent spend"
	// without a ceiling having to exist first.
	Usage Usage `json:"usage"`
}

// Usage is the token accounting carried across a park.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// MarshalPark encodes a park record.
func MarshalPark(p ParkRecord) ([]byte, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("agent: recording where the loop parked: %w", err)
	}
	return b, nil
}

// UnmarshalPark decodes an EventParked payload.
func UnmarshalPark(b []byte) (ParkRecord, error) {
	var p ParkRecord
	if err := json.Unmarshal(b, &p); err != nil {
		return p, fmt.Errorf("agent: reading %s: %w", EventParked, err)
	}
	return p, nil
}

// Messages decodes the parked conversation.
func (p ParkRecord) Messages() ([]provider.Message, error) {
	return decodeTranscript(p.Transcript)
}

// --- the transcript codec --------------------------------------------------

// provider.ContentPart is an interface, so a transcript needs a tagged
// encoding to survive a round trip through the log. An UNKNOWN tag is refused
// rather than dropped: a part silently missing from a resumed conversation is
// a model answering a question it can no longer see, and a provider rejecting
// a malformed message is the better failure.
const (
	partText       = "text"
	partReasoning  = "reasoning"
	partToolCall   = "tool_call"
	partToolResult = "tool_result"
	partImage      = "image"
	partFile       = "file"
	partSource     = "source"
)

// ErrTranscript is what a parked conversation that could not be written down,
// or could not be read back, is refused with.
var ErrTranscript = errors.New("agent: the parked conversation")

type wireMessage struct {
	Role  provider.Role `json:"role"`
	Parts []wirePart    `json:"parts"`
}

type wirePart struct {
	Kind string `json:"kind"`

	Text      string `json:"text,omitempty"`
	Redacted  bool   `json:"redacted,omitempty"`
	Signature string `json:"signature,omitempty"`

	ID         string          `json:"id,omitempty"`
	Name       string          `json:"name,omitempty"`
	Args       json.RawMessage `json:"args,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	IsError    bool            `json:"is_error,omitempty"`

	Data      []byte `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
	MediaType string `json:"media_type,omitempty"`
	Filename  string `json:"filename,omitempty"`
	FileID    string `json:"file_id,omitempty"`
	Title     string `json:"title,omitempty"`
}

func encodeTranscript(messages []provider.Message) (json.RawMessage, error) {
	wire := make([]wireMessage, 0, len(messages))
	for _, m := range messages {
		parts := make([]wirePart, 0, len(m.Content))
		for _, c := range m.Content {
			p, err := encodePart(c)
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
		}
		wire = append(wire, wireMessage{Role: m.Role, Parts: parts})
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%w could not be recorded: %w", ErrTranscript, err)
	}
	return b, nil
}

func encodePart(c provider.ContentPart) (wirePart, error) {
	switch p := c.(type) {
	case provider.TextPart:
		return wirePart{Kind: partText, Text: p.Text}, nil
	case provider.ReasoningPart:
		return wirePart{
			Kind: partReasoning, Text: p.Text, Redacted: p.Redacted, Signature: p.Signature,
		}, nil
	case provider.ToolCallPart:
		return wirePart{Kind: partToolCall, ID: p.ID, Name: p.Name, Args: p.Args}, nil
	case provider.ToolResultPart:
		result, err := json.Marshal(p.Result)
		if err != nil {
			return wirePart{}, fmt.Errorf("%w holds a tool result for %q that is not JSON: %w",
				ErrTranscript, p.Name, err)
		}
		return wirePart{
			Kind: partToolResult, ToolCallID: p.ToolCallID, Name: p.Name,
			Result: result, IsError: p.IsError,
		}, nil
	case provider.ImagePart:
		return wirePart{Kind: partImage, Data: p.Data, URL: p.URL, MediaType: p.MediaType}, nil
	case provider.FilePart:
		return wirePart{
			Kind: partFile, Data: p.Data, MediaType: p.MediaType,
			Filename: p.Filename, FileID: p.FileID, URL: p.URL,
		}, nil
	case provider.SourcePart:
		return wirePart{Kind: partSource, ID: p.ID, URL: p.URL, Title: p.Title}, nil
	default:
		return wirePart{}, fmt.Errorf("%w holds a %T, which this build cannot record; "+
			"a parked agent whose conversation cannot be written down cannot be resumed",
			ErrTranscript, c)
	}
}

func decodeTranscript(raw json.RawMessage) ([]provider.Message, error) {
	var wire []wireMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("%w could not be read back: %w", ErrTranscript, err)
	}
	messages := make([]provider.Message, 0, len(wire))
	for _, m := range wire {
		content := make([]provider.ContentPart, 0, len(m.Parts))
		for _, p := range m.Parts {
			part, err := decodePart(p)
			if err != nil {
				return nil, err
			}
			content = append(content, part)
		}
		messages = append(messages, provider.Message{Role: m.Role, Content: content})
	}
	return messages, nil
}

func decodePart(p wirePart) (provider.ContentPart, error) {
	switch p.Kind {
	case partText:
		return provider.TextPart{Text: p.Text}, nil
	case partReasoning:
		return provider.ReasoningPart{
			Text: p.Text, Redacted: p.Redacted, Signature: p.Signature,
		}, nil
	case partToolCall:
		return provider.ToolCallPart{ID: p.ID, Name: p.Name, Args: p.Args}, nil
	case partToolResult:
		var result any
		if len(p.Result) > 0 {
			if err := json.Unmarshal(p.Result, &result); err != nil {
				return nil, fmt.Errorf("%w holds an unreadable result for %q: %w",
					ErrTranscript, p.Name, err)
			}
		}
		return provider.ToolResultPart{
			ToolCallID: p.ToolCallID, Name: p.Name, Result: result, IsError: p.IsError,
		}, nil
	case partImage:
		return provider.ImagePart{Data: p.Data, URL: p.URL, MediaType: p.MediaType}, nil
	case partFile:
		return provider.FilePart{
			Data: p.Data, MediaType: p.MediaType, Filename: p.Filename,
			FileID: p.FileID, URL: p.URL,
		}, nil
	case partSource:
		return provider.SourcePart{ID: p.ID, URL: p.URL, Title: p.Title}, nil
	default:
		return nil, fmt.Errorf("%w holds a part of unknown kind %q; "+
			"dropping it would resume a model into a conversation it cannot have had",
			ErrTranscript, p.Kind)
	}
}
