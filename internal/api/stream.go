package api

// Two streams a browser can hold open, and nothing else in this file.
//
// WHY SSE AND NOT A WEBSOCKET. Both would carry the bytes. Only one of them
// survives the middle: a corporate proxy that buffers or kills an Upgrade
// leaves a WebSocket client with no signal it can act on, whereas
// text/event-stream is an ordinary chunked GET. More importantly SSE has a
// resume protocol already — the browser reconnects on its own and replays its
// last id in Last-Event-ID — and a stream that silently reconnects at the
// BEGINNING duplicates a run's history, while one that reconnects at the END
// loses whatever happened while it was away. Both are worse than a visible
// failure, so the resume is implemented here rather than assumed.
//
// WHY TWO COPIES OF A LOG. docs/wire-contract.md: the LogChunk stream on
// job.logs.<run>.<step> is the LIVE copy — ephemeral, best-effort, droppable —
// and the object the engine writes under JobStatus.log_key is the
// AUTHORITATIVE one. A viewer that only ever tails the subject shows a
// partial log to anyone who arrives late and never says so. So this endpoint
// tails the subject while the step is running and then SWITCHES: on the step's
// terminal event it announces a new source and re-sends the stored object in
// full, which is also exactly what a reader who arrives after the fact gets.
//
// These are plain net/http handlers rather than Connect RPCs because
// EventSource speaks neither Connect's streaming envelope nor gRPC's. They are
// NOT a second door: authentication runs through Server.principal, the same
// function every RPC in this package calls, so the tenant comes from the
// credential and from nothing else. A handler here that read a tenant out of
// the path would be the privileged corner ADR 0013 forbids.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
)

const (
	// StreamWriteTimeout bounds one write to a client. A browser tab that is
	// backgrounded, throttled or simply gone stops reading its socket, and
	// with no deadline the write blocks in the kernel forever, holding a
	// goroutine, a connection and a bus subscription for a viewer that will
	// never come back. Exceeding it ends THAT stream and nothing else.
	StreamWriteTimeout = 15 * time.Second

	// LiveLogBuffer is how many live chunks are held for one viewer before
	// chunks start being dropped. The live copy is explicitly best-effort
	// (docs/wire-contract.md), so a slow client loses chunks rather than
	// slowing the engine that produces them — but it is TOLD, with a gap
	// event, and the authoritative copy it receives at the end is complete.
	LiveLogBuffer = 256
)

// LiveLogs is the ephemeral log subject, narrowed to the one thing a viewer
// needs. bus.Bus satisfies it; nothing here may publish.
type LiveLogs interface {
	SubscribeEphemeral(ctx context.Context, subject string, fn func([]byte)) (func(), error)
}

// LogArchive is the authoritative log, narrowed to reading it.
// blobstore.Store satisfies it.
//
// The tenant is a parameter rather than something the key encodes: the store
// scopes structurally, so one tenant's key cannot name another's object even
// if it is guessed exactly.
type LogArchive interface {
	Read(ctx context.Context, tenantID, key string) (io.ReadCloser, error)
}

// sseEvent is one run transition on the wire.
//
// Payload is JSON where the stored payload is JSON (every scheduler and loop
// payload is, deliberately, so a person reading a stuck run can read it), the
// protojson form of a JobStatus where it is that, and base64 where it is
// neither. A viewer never has to know which.
type sseEvent struct {
	RunID      string          `json:"runId"`
	StepID     string          `json:"stepId,omitempty"`
	Attempt    uint32          `json:"attempt,omitempty"`
	Sequence   uint64          `json:"sequence"`
	Type       string          `json:"type"`
	AtUnixNano int64           `json:"atUnixNano"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

// logFrame is one piece of a log on the wire.
type logFrame struct {
	Seq    uint64 `json:"seq,omitempty"`
	Stream string `json:"stream,omitempty"`
	Text   string `json:"text"`
}

// sourceFrame announces which of the two copies the frames after it come
// from. A viewer RESETS on it: the stored copy replaces whatever the live one
// showed, because the live one may have gaps and the stored one may not.
type sourceFrame struct {
	Source string `json:"source"`
	Reason string `json:"reason,omitempty"`
}

// The two log sources, as the wire spells them.
const (
	sourceLive   = "live"
	sourceStored = "stored"
	sourceNone   = "none"
)

// registerStreams mounts the two SSE endpoints. It is called by Handler, and
// the routes exist whether or not the sources behind them are configured: a
// 501 that says which dependency is missing beats a 404 that suggests the
// endpoint was never built.
func (s *Server) registerStreams(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/runs/{run}/events", s.streamRunEvents)
	mux.HandleFunc("GET /v1/runs/{run}/steps/{step}/logs", s.streamStepLogs)
}

// streamRunEvents is GET /v1/runs/{id}/events: every run and step transition,
// from the beginning of the run, then following until the run ends.
//
// It ENDS when the run does. A stream that stayed open after RUN_COMPLETED
// would hold a connection per finished run for as long as the tab lived, and
// a viewer waiting for an event that can never arrive cannot tell that from a
// stalled one.
func (s *Server) streamRunEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p, ok := s.streamPrincipal(w, r)
	if !ok {
		return
	}
	if s.runs == nil {
		http.Error(w, "this server was built without a run store", http.StatusNotImplemented)
		return
	}
	runID := r.PathValue("run")

	// The first read happens BEFORE any byte of the body, because it is the
	// only chance to answer with a status code. A run of another tenant is
	// indistinguishable here from one that does not exist: Replay is
	// tenant-scoped, so the answer is an empty log either way, and a caller
	// who could tell them apart would learn which runs exist elsewhere.
	events, err := s.runs.Replay(ctx, p.TenantID, runID)
	if err != nil {
		http.Error(w, "replay run", http.StatusInternalServerError)
		return
	}
	if len(events) == 0 {
		http.Error(w, "no such run", http.StatusNotFound)
		return
	}

	// Last-Event-ID is the sequence the client last SAW. Resuming means
	// everything after it and nothing before it: replaying from zero would
	// duplicate a run's whole history in the viewer, and jumping to the live
	// tail would silently lose whatever happened during the disconnection.
	after := lastEventID(r)

	sse := newSSE(w)
	sse.open()
	for {
		for _, e := range events {
			if e.Sequence <= after {
				continue
			}
			if err := sse.send(strconv.FormatUint(e.Sequence, 10), string(e.Type), wireSSEEvent(e)); err != nil {
				return
			}
			after = e.Sequence
			if terminal(e.Type) {
				_ = sse.send("", "end", map[string]string{"runId": runID})
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.poll):
		}
		events, err = s.runs.Replay(ctx, p.TenantID, runID)
		if err != nil {
			return
		}
	}
}

// streamStepLogs is GET /v1/runs/{id}/steps/{step}/logs.
//
// While the step runs this is the live subject; when it finishes — or if it
// had already finished before anyone connected — it is the stored object, in
// full, announced as such.
func (s *Server) streamStepLogs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	p, ok := s.streamPrincipal(w, r)
	if !ok {
		return
	}
	if s.runs == nil {
		http.Error(w, "this server was built without a run store", http.StatusNotImplemented)
		return
	}
	runID, stepID := r.PathValue("run"), r.PathValue("step")
	if stepID == "" {
		http.Error(w, "a step is required", http.StatusBadRequest)
		return
	}

	// Same tenant check, same reason, and it is the ONLY thing standing
	// between a caller and a bus subject that carries no tenant of its own:
	// job.logs.<run>.<step> is keyed by run id, so the right to subscribe to
	// it is established here, from the run log, before anything is opened.
	events, err := s.runs.Replay(ctx, p.TenantID, runID)
	if err != nil {
		http.Error(w, "replay run", http.StatusInternalServerError)
		return
	}
	if len(events) == 0 {
		http.Error(w, "no such run", http.StatusNotFound)
		return
	}

	sse := newSSE(w)
	sse.open()

	if key, done := logKeyFor(events, stepID); done {
		// Late arrival: there is no live copy left, and pretending otherwise
		// would show an empty log for a step that produced plenty.
		s.sendStored(ctx, sse, p.TenantID, key)
		return
	}
	// Resume: a reconnecting viewer says which live chunk it last saw, and
	// gets the ones after it rather than the whole tail again.
	s.tailLive(ctx, sse, p, runID, stepID, lastEventID(r))
}

// tailLive follows the ephemeral subject until the step reaches a terminal
// event, then hands over to the stored object.
func (s *Server) tailLive(
	ctx context.Context, sse *sseWriter, p identity.Principal, runID, stepID string, after uint64,
) {
	if s.live == nil {
		_ = sse.send("", "source", sourceFrame{Source: sourceNone,
			Reason: "this server was built without a live log subject"})
		return
	}

	// A BOUNDED buffer, and the bound is the whole point. The subscription
	// callback runs on the bus client's delivery goroutine: blocking it would
	// stall every other subscriber of this process behind one slow browser.
	// So a full buffer drops the chunk and counts it — which the wire contract
	// permits for this copy — and the viewer is told with a gap event rather
	// than being left to infer it from a jump in seq.
	chunks := make(chan *dholev1.LogChunk, LiveLogBuffer)
	dropped := 0
	unsubscribe, err := s.live.SubscribeEphemeral(ctx, bus.SubjectLogs(runID, stepID), func(data []byte) {
		chunk := &dholev1.LogChunk{}
		if err := proto.Unmarshal(data, chunk); err != nil {
			return
		}
		select {
		case chunks <- chunk:
		default:
			dropped++
		}
	})
	if err != nil {
		_ = sse.send("", "source", sourceFrame{Source: sourceNone, Reason: "subscribing to the live log failed"})
		return
	}
	defer unsubscribe()

	if err := sse.send("", "source", sourceFrame{Source: sourceLive}); err != nil {
		return
	}
	poll := time.NewTicker(s.poll)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case chunk := <-chunks:
			if after != 0 && chunk.GetSeq() <= after {
				continue
			}
			after = chunk.GetSeq()
			if dropped > 0 {
				if err := sse.send("", "gap", map[string]int{"dropped": dropped}); err != nil {
					return
				}
				dropped = 0
			}
			if err := sse.send(strconv.FormatUint(chunk.GetSeq(), 10), "chunk", logFrame{
				Seq:    chunk.GetSeq(),
				Stream: strings.TrimPrefix(chunk.GetStream().String(), "STREAM_"),
				Text:   string(chunk.GetData()),
			}); err != nil {
				return
			}
		case <-poll.C:
			events, err := s.runs.Replay(ctx, p.TenantID, runID)
			if err != nil {
				return
			}
			key, done := logKeyFor(events, stepID)
			if !done {
				continue
			}
			// The handover. The live copy stops here whatever is still in
			// the buffer: it was best-effort, and what replaces it is the
			// copy that is not.
			unsubscribe()
			s.sendStored(ctx, sse, p.TenantID, key)
			return
		}
	}
}

// sendStored announces the authoritative copy and sends it whole.
func (s *Server) sendStored(ctx context.Context, sse *sseWriter, tenantID, key string) {
	if s.archive == nil || key == "" {
		reason := "this step recorded no authoritative log"
		if s.archive == nil {
			reason = "this server was built without an object store"
		}
		_ = sse.send("", "source", sourceFrame{Source: sourceNone, Reason: reason})
		_ = sse.send("", "end", sourceFrame{Source: sourceNone})
		return
	}
	body, err := s.archive.Read(ctx, tenantID, key)
	if err != nil {
		reason := "the authoritative log could not be read"
		if errors.Is(err, blobstore.ErrNotFound) {
			reason = "the authoritative log is not in the object store"
		}
		_ = sse.send("", "source", sourceFrame{Source: sourceNone, Reason: reason})
		_ = sse.send("", "end", sourceFrame{Source: sourceNone})
		return
	}
	defer func() { _ = body.Close() }()

	if err := sse.send("", "source", sourceFrame{Source: sourceStored}); err != nil {
		return
	}
	// Read and forward in bounded pieces rather than reading the whole object
	// into memory: a step is allowed to produce a log far larger than the
	// control plane's heap.
	buf := make([]byte, 32*1024)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if sendErr := sse.send("", "log", logFrame{Text: string(buf[:n])}); sendErr != nil {
				return
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = sse.send("", "end", sourceFrame{Source: sourceStored, Reason: "the log ended early: " + err.Error()})
			return
		}
	}
	_ = sse.send("", "end", sourceFrame{Source: sourceStored})
}

// logKeyFor finds the authoritative log key of one step, and whether that step
// has finished at all. The key comes off the recorded JobStatus, which is what
// the engine promised to have written before it published (wire contract).
func logKeyFor(events []runstore.Event, stepID string) (string, bool) {
	key, done := "", false
	for _, e := range events {
		if e.StepID != stepID {
			continue
		}
		if e.Type != runstore.StepSucceeded && e.Type != runstore.StepFailed {
			continue
		}
		done = true
		status := &dholev1.JobStatus{}
		if err := proto.Unmarshal(e.Payload, status); err == nil && status.GetLogKey() != "" {
			key = status.GetLogKey()
		}
	}
	return key, done
}

// wireSSEEvent is one stored event as a viewer reads it.
func wireSSEEvent(e runstore.Event) sseEvent {
	return sseEvent{
		RunID:      e.RunID,
		StepID:     e.StepID,
		Attempt:    e.Attempt,
		Sequence:   e.Sequence,
		Type:       string(e.Type),
		AtUnixNano: e.At.UTC().UnixNano(),
		Payload:    ssePayload(e.Payload),
	}
}

// ssePayload turns a stored payload into something a browser can read.
func ssePayload(payload []byte) json.RawMessage {
	if len(payload) == 0 {
		return nil
	}
	if json.Valid(payload) {
		return payload
	}
	status := &dholev1.JobStatus{}
	if err := proto.Unmarshal(payload, status); err == nil {
		if encoded, err := protojson.Marshal(status); err == nil {
			return encoded
		}
	}
	encoded, err := json.Marshal(base64.StdEncoding.EncodeToString(payload))
	if err != nil {
		return nil
	}
	return encoded
}

// streamPrincipal authenticates one streaming request through the SAME path
// every RPC uses, and answers with an HTTP status rather than a Connect code.
func (s *Server) streamPrincipal(w http.ResponseWriter, r *http.Request) (identity.Principal, bool) {
	p, err := s.principal(r.Context(), r.Header)
	if err == nil {
		return p, true
	}
	status := http.StatusInternalServerError
	switch connect.CodeOf(err) {
	case connect.CodeUnauthenticated:
		status = http.StatusUnauthorized
	case connect.CodePermissionDenied:
		status = http.StatusForbidden
	case connect.CodeNotFound:
		status = http.StatusNotFound
	}
	// The message is the refusal's own, which never distinguishes an unknown
	// subject from a bad secret.
	http.Error(w, connect.CodeOf(err).String(), status)
	return identity.Principal{}, false
}

// lastEventID reads the resume position off the request.
func lastEventID(r *http.Request) uint64 {
	raw := r.Header.Get("Last-Event-ID")
	if raw == "" {
		// Not every client is an EventSource. A fetch-based reader — which is
		// what the web app uses, because EventSource cannot set an
		// Authorization header — resumes with the same value in a query
		// parameter.
		raw = r.URL.Query().Get("last_event_id")
	}
	return lastEventIDFromString(raw)
}

func lastEventIDFromString(raw string) uint64 {
	n, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		// An id this server did not mint resumes from the beginning. The
		// alternative — treating it as "the latest" — turns a corrupted id
		// into silently skipped history.
		return 0
	}
	return n
}

// sseWriter writes text/event-stream frames, flushing each one and refusing to
// block on a client that has stopped reading.
type sseWriter struct {
	w        http.ResponseWriter
	rc       *http.ResponseController
	lastSeen string
}

func newSSE(w http.ResponseWriter) *sseWriter {
	return &sseWriter{w: w, rc: http.NewResponseController(w)}
}

// open writes the response headers. Nothing may set a status after it.
func (s *sseWriter) open() {
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	// Nginx and friends buffer a proxied response by default, which turns a
	// live tail into a batch delivered at the end.
	h.Set("X-Accel-Buffering", "no")
	s.w.WriteHeader(http.StatusOK)
	s.flush()
}

// send writes one frame. An error means this client is gone or too slow, and
// the caller must stop: the stream is not resumable in place.
func (s *sseWriter) send(id, event string, data any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("api: encoding %s frame: %w", event, err)
	}
	var b strings.Builder
	if id != "" {
		b.WriteString("id: " + id + "\n")
		s.lastSeen = id
	}
	b.WriteString("event: " + event + "\n")
	// A payload containing a newline would otherwise end the frame early.
	for _, line := range strings.Split(string(encoded), "\n") {
		b.WriteString("data: " + line + "\n")
	}
	b.WriteString("\n")

	// The deadline is what keeps a vanished client from pinning a goroutine
	// and a subscription forever. A ResponseWriter that cannot take one (a
	// recorder in a test) simply has none, which is the safe direction.
	if err := s.rc.SetWriteDeadline(time.Now().Add(StreamWriteTimeout)); err != nil &&
		!errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := io.WriteString(s.w, b.String()); err != nil {
		return err
	}
	s.flush()
	return nil
}

func (s *sseWriter) flush() {
	if err := s.rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return
	}
}
