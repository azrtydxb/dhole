package api_test

// The two SSE endpoints the run view is built on.
//
// Every case here is written so that ONE property fails alone. The one that
// took the most care is the log switch-over: a test whose live copy and stored
// copy contain the same bytes cannot tell a server that switched from one that
// never did, and would pass against an implementation that only ever tails the
// ephemeral subject. So the two copies are deliberately DIFFERENT here, and
// the assertion is on the stored text, which the live subject never carried.

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

const (
	streamRunID  = "run_stream"
	streamStepID = "build"
	// The two copies of the log, and they differ on purpose. "authoritative"
	// appears ONLY in the object store, so an assertion on it cannot be
	// satisfied by the live subject.
	liveText   = "live chunk that may be dropped\n"
	storedText = "authoritative line one\nauthoritative line two\n"
)

// fakeLive is the ephemeral log subject. It records every subject anyone
// subscribed to, which is how the tenant cases prove that a refused caller
// never reached the bus at all.
type fakeLive struct {
	mu        sync.Mutex
	subjects  []string
	fn        func([]byte)
	ready     chan struct{}
	closed    int
	readyOnce sync.Once
}

func newFakeLive() *fakeLive { return &fakeLive{ready: make(chan struct{})} }

func (f *fakeLive) SubscribeEphemeral(
	_ context.Context, subject string, fn func([]byte),
) (func(), error) {
	f.mu.Lock()
	f.subjects = append(f.subjects, subject)
	f.fn = fn
	f.mu.Unlock()
	f.readyOnce.Do(func() { close(f.ready) })
	return func() {
		f.mu.Lock()
		f.closed++
		f.mu.Unlock()
	}, nil
}

// publish delivers one chunk the way the bus client would: on ITS goroutine,
// which is the goroutine a slow viewer must never be able to block.
func (f *fakeLive) publish(t *testing.T, seq uint64, text string) {
	t.Helper()
	f.mu.Lock()
	fn := f.fn
	f.mu.Unlock()
	require.NotNil(t, fn, "nothing subscribed to the live subject")
	data, err := proto.Marshal(&dholev1.LogChunk{
		RunId: streamRunID, StepId: streamStepID, Seq: seq,
		Data: []byte(text), Stream: dholev1.Stream_STREAM_STDOUT,
	})
	require.NoError(t, err)
	fn(data)
}

func (f *fakeLive) subscribedSubjects() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.subjects...)
}

// fakeArchive is the object store, keyed by tenant AND key, so a cross-tenant
// read misses rather than being caught by something above it.
type fakeArchive struct {
	mu      sync.Mutex
	objects map[string]string
}

func newFakeArchive() *fakeArchive { return &fakeArchive{objects: map[string]string{}} }

func (f *fakeArchive) put(tenantID, key, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[tenantID+"/"+key] = body
}

func (f *fakeArchive) Read(_ context.Context, tenantID, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[tenantID+"/"+key]
	if !ok {
		return nil, blobstore.ErrNotFound
	}
	return io.NopCloser(strings.NewReader(body)), nil
}

// streamHarness is one API server, its run log and its two log copies.
type streamHarness struct {
	url     string
	client  *http.Client
	runs    runstore.Store
	live    *fakeLive
	archive *fakeArchive
}

func newStreamHarness(t *testing.T) *streamHarness {
	t.Helper()
	runs, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	live, archive := newFakeLive(), newFakeArchive()
	srv, err := api.NewServer(api.Config{
		Definitions:  newRecordingDefs(),
		Auth:         fakeAuth{},
		Runs:         runs,
		LiveLogs:     live,
		LogArchive:   archive,
		PollInterval: 5 * time.Millisecond,
	})
	require.NoError(t, err)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	return &streamHarness{
		url: httpSrv.URL, client: httpSrv.Client(),
		runs: runs, live: live, archive: archive,
	}
}

func (h *streamHarness) append(t *testing.T, e runstore.Event) {
	t.Helper()
	e.RunID = streamRunID
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	require.NoError(t, h.runs.Append(context.Background(), tenantA, e))
}

// get opens a stream. The caller closes the body.
func (h *streamHarness) get(t *testing.T, path, token string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, h.url+path, nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := h.client.Do(req)
	require.NoError(t, err)
	return res
}

// frame is one parsed SSE frame.
type frame struct {
	id    string
	event string
	data  string
}

// readFrames parses frames off a live stream until fn says to stop or the
// stream ends. It is a reader, not a buffer: a case that has to see the
// switch-over happen cannot wait for the whole body.
func readFrames(t *testing.T, body io.Reader, stop func(frame) bool) []frame {
	t.Helper()
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var frames []frame
	var current frame
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "id:"):
			current.id = strings.TrimSpace(strings.TrimPrefix(line, "id:"))
		case strings.HasPrefix(line, "event:"):
			current.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			current.data += strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		case line == "":
			if current.event == "" {
				continue
			}
			frames = append(frames, current)
			if stop != nil && stop(current) {
				return frames
			}
			current = frame{}
		}
	}
	return frames
}

func eventTypes(frames []frame) []string {
	out := make([]string, 0, len(frames))
	for _, f := range frames {
		out = append(out, f.event)
	}
	return out
}

// seedRun writes a small but realistic run: created, one step dispatched from
// the cache with a reason, one step succeeded.
func (h *streamHarness) seedRun(t *testing.T) {
	t.Helper()
	created, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: "pipe", RevisionID: "rev",
	})
	require.NoError(t, err)
	h.append(t, runstore.Event{Type: runstore.RunCreated, Payload: created})

	dispatched, err := scheduler.MarshalDispatched(scheduler.Dispatched{
		Attempt: 1, Cacheable: false,
		CacheIneligibleReason: "effect class is undeclared, so the step has not promised it is pure",
	})
	require.NoError(t, err)
	h.append(t, runstore.Event{StepID: streamStepID, Attempt: 1,
		Type: runstore.StepDispatched, Payload: dispatched})
}

// succeed closes the step with a recorded JobStatus naming the stored log.
func (h *streamHarness) succeed(t *testing.T, logKey string) {
	t.Helper()
	status, err := proto.Marshal(&dholev1.JobStatus{
		RunId: streamRunID, StepId: streamStepID, Attempt: 1,
		Phase: dholev1.Phase_PHASE_SUCCEEDED, LogKey: logKey,
	})
	require.NoError(t, err)
	h.append(t, runstore.Event{StepID: streamStepID, Attempt: 1,
		Type: runstore.StepSucceeded, Payload: status})
}

func TestRunEventStreamReplaysTheWholeRunAndEndsWithIt(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)
	h.succeed(t, "logs/build")
	h.append(t, runstore.Event{Type: runstore.RunCompleted})

	res := h.get(t, "/v1/runs/"+streamRunID+"/events", tokenAlice, nil)
	defer func() { _ = res.Body.Close() }()
	require.Equal(t, http.StatusOK, res.StatusCode)
	require.Equal(t, "text/event-stream", res.Header.Get("Content-Type"))

	// io.ReadAll returning at all is the proof that the stream TERMINATED: a
	// server holding the connection open after RUN_COMPLETED hangs here.
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	frames := readFrames(t, strings.NewReader(string(body)), nil)

	require.Equal(t, []string{"RUN_CREATED", "STEP_DISPATCHED", "STEP_SUCCEEDED", "RUN_COMPLETED", "end"},
		eventTypes(frames))
	// The dispatch's cache verdict reaches the viewer as readable JSON, which
	// is what lets the run view show the reason without retyping it.
	require.Contains(t, frames[1].data, "effect class is undeclared, so the step has not promised it is pure")
	// A JobStatus payload is protobuf in the store and JSON on the wire.
	require.Contains(t, frames[2].data, "logs/build")
}

func TestRunEventStreamResumesAfterLastEventIDWithoutRepeatingOrSkipping(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)
	h.succeed(t, "logs/build")
	h.append(t, runstore.Event{Type: runstore.RunCompleted})

	all := h.get(t, "/v1/runs/"+streamRunID+"/events", tokenAlice, nil)
	first, err := io.ReadAll(all.Body)
	require.NoError(t, err)
	require.NoError(t, all.Body.Close())
	frames := readFrames(t, strings.NewReader(string(first)), nil)
	require.Len(t, frames, 5)

	// Resume as an EventSource does: the id of the second event.
	resumeFrom := frames[1].id
	require.NotEmpty(t, resumeFrom)

	res := h.get(t, "/v1/runs/"+streamRunID+"/events", tokenAlice,
		map[string]string{"Last-Event-ID": resumeFrom})
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	resumed := readFrames(t, strings.NewReader(string(body)), nil)

	// Nothing before the resume point comes back...
	require.Equal(t, []string{"STEP_SUCCEEDED", "RUN_COMPLETED", "end"}, eventTypes(resumed))
	// ...and nothing after it is missing either: the run's last event is here.
	require.Equal(t, frames[3].id, resumed[1].id)
}

func TestRunEventStreamIsRefusedWithoutACredentialAndToAnotherTenant(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)
	h.append(t, runstore.Event{Type: runstore.RunCompleted})

	bare := h.get(t, "/v1/runs/"+streamRunID+"/events", "", nil)
	require.NoError(t, bare.Body.Close())
	require.Equal(t, http.StatusUnauthorized, bare.StatusCode,
		"a stream with no Authorization header must be refused, like every other call")

	bad := h.get(t, "/v1/runs/"+streamRunID+"/events", "tok-nobody", nil)
	require.NoError(t, bad.Body.Close())
	require.Equal(t, http.StatusUnauthorized, bad.StatusCode)

	// tenant-b authenticates perfectly well and still sees nothing: not
	// forbidden, NOT FOUND, because a caller who can tell those apart learns
	// which runs exist elsewhere.
	foreign := h.get(t, "/v1/runs/"+streamRunID+"/events", tokenBob, nil)
	require.NoError(t, foreign.Body.Close())
	require.Equal(t, http.StatusNotFound, foreign.StatusCode)
}

func TestStepLogStreamIsRefusedToAnotherTenantAndNeverReachesTheBus(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)

	foreign := h.get(t, "/v1/runs/"+streamRunID+"/steps/"+streamStepID+"/logs", tokenBob, nil)
	require.NoError(t, foreign.Body.Close())
	require.Equal(t, http.StatusNotFound, foreign.StatusCode)

	bare := h.get(t, "/v1/runs/"+streamRunID+"/steps/"+streamStepID+"/logs", "", nil)
	require.NoError(t, bare.Body.Close())
	require.Equal(t, http.StatusUnauthorized, bare.StatusCode)

	// The subject job.logs.<run>.<step> carries no tenant of its own. The
	// only thing keeping another tenant off it is the run-log check above, so
	// a refused caller must not have subscribed at all.
	require.Empty(t, h.live.subscribedSubjects(),
		"a refused caller must never reach the live log subject")
}

func TestStepLogStreamSwitchesFromTheLiveSubjectToTheStoredObject(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)
	// The stored copy holds text the live subject NEVER carries. That is what
	// makes this test able to tell the two apart.
	h.archive.put(tenantA, "logs/build", storedText)

	res := h.get(t, "/v1/runs/"+streamRunID+"/steps/"+streamStepID+"/logs", tokenAlice, nil)
	defer func() { _ = res.Body.Close() }()
	require.Equal(t, http.StatusOK, res.StatusCode)

	select {
	case <-h.live.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream never subscribed to the live log subject")
	}
	require.Equal(t, []string{bus.SubjectLogs(streamRunID, streamStepID)}, h.live.subscribedSubjects())
	h.live.publish(t, 1, liveText)

	// Finish the step a moment later; the stream has to notice on its own.
	go func() {
		time.Sleep(50 * time.Millisecond)
		h.succeed(t, "logs/build")
	}()

	frames := readFrames(t, res.Body, func(f frame) bool { return f.event == "end" })
	types := eventTypes(frames)
	require.Equal(t, "source", types[0])
	require.Contains(t, frames[0].data, `"source":"live"`)
	require.Contains(t, types, "chunk", "the live tail must actually deliver while the step runs")

	// The switch, and then the authoritative copy in full.
	var switched bool
	var stored string
	for _, f := range frames {
		if f.event == "source" && strings.Contains(f.data, `"source":"stored"`) {
			switched = true
			continue
		}
		if switched && f.event == "log" {
			stored += f.data
		}
	}
	require.True(t, switched,
		"the stream must announce the authoritative copy when the step completes; "+
			"a viewer left on the live subject shows a best-effort log as if it were the whole one")
	require.Contains(t, stored, "authoritative line one")
	require.Contains(t, stored, "authoritative line two")
	require.Equal(t, "end", types[len(types)-1], "the stream must end rather than stay open")
}

func TestStepLogStreamServesTheStoredObjectToAReaderWhoArrivesLate(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)
	h.succeed(t, "logs/build")
	h.append(t, runstore.Event{Type: runstore.RunCompleted})
	h.archive.put(tenantA, "logs/build", storedText)

	res := h.get(t, "/v1/runs/"+streamRunID+"/steps/"+streamStepID+"/logs", tokenAlice, nil)
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	frames := readFrames(t, strings.NewReader(string(body)), nil)

	require.Contains(t, frames[0].data, `"source":"stored"`,
		"a step that has finished has no live copy left, and pretending otherwise shows an empty log")
	require.NotContains(t, string(body), "live chunk")
	require.Contains(t, string(body), "authoritative line two")
	require.Empty(t, h.live.subscribedSubjects(),
		"there is nothing to tail for a finished step")
	require.Equal(t, "end", frames[len(frames)-1].event)
}

func TestStepLogStreamSaysSoWhenTheAuthoritativeCopyIsMissing(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)
	h.succeed(t, "logs/build") // ...but nothing was ever written under it.

	res := h.get(t, "/v1/runs/"+streamRunID+"/steps/"+streamStepID+"/logs", tokenAlice, nil)
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), `"source":"none"`)
	require.Contains(t, string(body), "not in the object store")
}

func TestStepLogStreamDropsChunksRatherThanBlockingTheDeliveryGoroutine(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)
	h.archive.put(tenantA, "logs/build", storedText)

	res := h.get(t, "/v1/runs/"+streamRunID+"/steps/"+streamStepID+"/logs", tokenAlice, nil)
	defer func() { _ = res.Body.Close() }()
	select {
	case <-h.live.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream never subscribed")
	}

	// Nobody is reading this response yet. Publish far more than the stream
	// will hold: the bus's delivery goroutine must come back regardless,
	// because blocking it would stall every other subscriber in the process
	// behind one slow browser.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for seq := uint64(1); seq <= 8*api.LiveLogBuffer; seq++ {
			h.live.publish(t, seq, strings.Repeat("x", 512)+"\n")
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("publishing blocked: a viewer that stopped reading is holding up the bus")
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		h.succeed(t, "logs/build")
	}()
	frames := readFrames(t, res.Body, func(f frame) bool { return f.event == "end" })

	// What a client that fell behind gets: fewer chunks than were published,
	// a gap event saying how many went missing, and then the COMPLETE
	// authoritative copy anyway.
	var gaps, chunks int
	for _, f := range frames {
		switch f.event {
		case "gap":
			gaps++
		case "chunk":
			chunks++
		}
	}
	require.Less(t, chunks, 8*api.LiveLogBuffer, "a bounded stream must have dropped something")
	require.Positive(t, gaps, "a viewer that lost live chunks has to be told, not left to guess")
	require.Contains(t, eventTypes(frames), "end")
}

func TestStepLogStreamResumesLiveChunksAfterLastEventID(t *testing.T) {
	t.Parallel()
	h := newStreamHarness(t)
	h.seedRun(t)
	h.archive.put(tenantA, "logs/build", storedText)

	res := h.get(t, "/v1/runs/"+streamRunID+"/steps/"+streamStepID+"/logs", tokenAlice,
		map[string]string{"Last-Event-ID": "7"})
	defer func() { _ = res.Body.Close() }()
	select {
	case <-h.live.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream never subscribed")
	}
	h.live.publish(t, 5, "already seen\n")
	h.live.publish(t, 7, "also already seen\n")
	h.live.publish(t, 8, "new since the disconnection\n")

	go func() {
		time.Sleep(100 * time.Millisecond)
		h.succeed(t, "logs/build")
	}()
	frames := readFrames(t, res.Body, func(f frame) bool { return f.event == "end" })

	var live string
	for _, f := range frames {
		if f.event == "chunk" {
			live += f.data
		}
	}
	require.NotContains(t, live, "already seen",
		"a resumed stream must not replay chunks the viewer already has")
	require.Contains(t, live, "new since the disconnection",
		"a resumed stream must not skip what happened while it was away")
}
