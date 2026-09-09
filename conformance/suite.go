// Package conformance is the executable half of docs/wire-contract.md.
//
// The contract claims an engine can be written in any language. That claim is
// only worth something if somebody who has read the document — and nothing
// else — can write an engine and be told, precisely, where it does not comply.
// This package is that test: it stands up an embedded NATS, plays the control
// plane, launches an engine COMMAND, and runs one case per obligation.
//
// Two rules shape everything here.
//
// The suite talks to the engine over the bus and nowhere else. It never
// imports the engine under test, never shares a helper with it, and never
// looks inside its process. Anything Dhole's own Go engine does implicitly —
// a shared constant, a type both halves happen to use — has to be either in
// the contract or in a case's stated convention, or a stranger's engine
// cannot pass.
//
// Every failure names what was expected, what arrived, and how long the suite
// waited. "Case X failed" tells an engine author nothing; "expected a
// JobStatus with phase PHASE_FAILED and an error containing 'unsupported
// protocol', got no message within 30s" tells them what to fix.
package conformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/wire"
)

// Status is a case's outcome.
type Status string

const (
	// StatusPassed means the engine met the obligation.
	StatusPassed Status = "PASS"
	// StatusFailed means it did not, and Detail says how.
	StatusFailed Status = "FAIL"
	// StatusSkipped means the case did not run — never that it succeeded.
	StatusSkipped Status = "SKIP"
)

// Config is one conformance run.
type Config struct {
	// Engine is the argv of the engine under test: the command the suite
	// launches and drives over the bus. Required.
	Engine []string
	// Dir is the working directory for that command. Empty means the caller's.
	Dir string
	// Env is added to the engine's environment, on top of the suite's own
	// variables. Each entry is "KEY=VALUE".
	Env []string
	// Tier is the trust tier the engine runs in and work is dispatched to.
	// Empty means "trusted".
	Tier string
	// EngineID is the identity the engine registers under and the last token
	// of its control subject. Empty means "conformance-engine".
	EngineID string
	// Only, when set, restricts the run to these case names. A restricted run
	// is not a conformance pass and the report says so.
	Only []string
	// CaseTimeout bounds a case that does not set its own. Empty means 30s.
	CaseTimeout time.Duration
	// Logf receives progress and everything the engine writes to its stdout
	// and stderr. Nil discards it.
	Logf func(format string, args ...any)
}

// Result is one case's outcome.
type Result struct {
	// Name identifies the case, and is what Config.Only matches.
	Name string
	// Obligation is the rule from docs/wire-contract.md this case enforces,
	// including any convention the suite had to invent because the contract
	// does not state one.
	Obligation string
	Status     Status
	// Detail is why. For a failure it names what was expected, what arrived,
	// and how long the suite waited.
	Detail   string
	Duration time.Duration
}

// Report is the whole run.
type Report struct {
	// Engine is the command that was tested.
	Engine string
	// Partial is true when Only restricted the run: a partial report is not a
	// conformance pass however many cases it contains.
	Partial  bool
	Results  []Result
	Passed   int
	Failed   int
	Skipped  int
	Duration time.Duration
}

// String renders the report for a terminal, failures first in detail.
func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "engine: %s\n", r.Engine)
	for _, res := range r.Results {
		fmt.Fprintf(&b, "%-4s %-34s %8s", res.Status, res.Name, res.Duration.Round(time.Millisecond))
		if res.Detail != "" {
			fmt.Fprintf(&b, "\n       %s", indent(res.Detail))
			if res.Status == StatusFailed {
				fmt.Fprintf(&b, "\n       obligation: %s", indent(res.Obligation))
			}
		}
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "%d passed, %d failed, %d skipped in %s",
		r.Passed, r.Failed, r.Skipped, r.Duration.Round(time.Millisecond))
	if r.Partial {
		b.WriteString(" (PARTIAL RUN — not a conformance pass)")
	}
	return b.String()
}

func indent(s string) string {
	return strings.ReplaceAll(strings.TrimSpace(s), "\n", "\n       ")
}

// Run executes the suite against cfg.Engine and returns what it found.
//
// It returns an error only when the HARNESS could not run — no bus, no
// process. An engine that fails every case is a Report with failures in it,
// not an error: the caller asked what the engine does, and the answer is in
// the report.
func Run(ctx context.Context, cfg Config) (Report, error) {
	if len(cfg.Engine) == 0 {
		return Report{}, errors.New("conformance: Config.Engine is required — the suite has nothing to test")
	}
	if cfg.Tier == "" {
		cfg.Tier = "trusted"
	}
	if cfg.EngineID == "" {
		cfg.EngineID = "conformance-engine"
	}
	if cfg.CaseTimeout == 0 {
		cfg.CaseTimeout = caseTimeout
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}

	selected, err := selectCases(cfg.Only)
	if err != nil {
		return Report{}, err
	}

	root, err := os.MkdirTemp("", "dhole-conformance-")
	if err != nil {
		return Report{}, fmt.Errorf("conformance: workspace: %w", err)
	}
	defer func() { _ = os.RemoveAll(root) }()

	h, err := startHarness(ctx, cfg, root)
	if err != nil {
		return Report{}, err
	}
	defer h.close()

	if err := h.startEngine(ctx); err != nil {
		return Report{}, err
	}

	report := Report{
		Engine:  strings.Join(cfg.Engine, " "),
		Partial: len(cfg.Only) > 0,
	}
	started := time.Now()
	skipRest := ""
	for _, c := range selected {
		if skipRest != "" {
			report.Results = append(report.Results, Result{
				Name: c.name, Obligation: c.obligation, Status: StatusSkipped, Detail: skipRest,
			})
			report.Skipped++
			continue
		}
		res := h.runCase(ctx, c, cfg.CaseTimeout)
		report.Results = append(report.Results, res)
		switch res.Status {
		case StatusPassed:
			report.Passed++
		case StatusFailed:
			report.Failed++
			// Nothing after registration can mean anything if the engine never
			// announced itself: the remaining cases would all time out and
			// bury the one failure that explains them.
			if c.name == "registration" {
				skipRest = "not run: the engine never registered, so every later case would only time out"
			}
		case StatusSkipped:
			report.Skipped++
		}
		cfg.Logf("conformance: %s %s (%s)", res.Status, res.Name, res.Duration.Round(time.Millisecond))
	}
	report.Duration = time.Since(started)
	return report, nil
}

// selectCases applies Config.Only, refusing a name that matches no case rather
// than silently running fewer cases than the caller asked for.
func selectCases(only []string) ([]kase, error) {
	all := cases()
	if len(only) == 0 {
		return all, nil
	}
	wanted := map[string]bool{}
	for _, name := range only {
		wanted[name] = true
	}
	selected := make([]kase, 0, len(only))
	for _, c := range all {
		if wanted[c.name] {
			selected = append(selected, c)
			delete(wanted, c.name)
		}
	}
	for name := range wanted {
		names := make([]string, 0, len(all))
		for _, c := range all {
			names = append(names, c.name)
		}
		return nil, fmt.Errorf("conformance: no case named %q; the suite has: %s", name, strings.Join(names, ", "))
	}
	return selected, nil
}

// harness is the control plane the engine under test talks to.
type harness struct {
	cfg      Config
	tier     string
	engineID string
	blobDir  string
	logf     func(string, ...any)

	plane *bus.NATS
	raw   *nats.Conn
	nats  *bus.Embedded
	stop  []func()

	cmd     *exec.Cmd
	engineC context.CancelFunc

	mu         sync.Mutex
	statuses   map[string][]*statusWatch
	logs       map[string][]*dholev1.LogChunk
	regs       []*dholev1.EngineRegistration
	unframed   []string
	beats      []*dholev1.EngineHeartbeat
	beatCursor int
	secrets    map[string]string
	redeemed   map[string]int
	seq        int
}

// secretSubjectName is where the suite serves secret redemption. The contract
// says a handle is redeemed and does not say how, so this is the harness's
// own convention, handed to the engine as DHOLE_SECRET_SUBJECT.
const secretSubjectName = "conformance.secret.redeem" // #nosec G101 -- a subject name, not a credential.

func startHarness(ctx context.Context, cfg Config, root string) (*harness, error) {
	h := &harness{
		cfg:      cfg,
		tier:     cfg.Tier,
		engineID: cfg.EngineID,
		blobDir:  filepath.Join(root, "blobs"),
		logf:     cfg.Logf,
		statuses: map[string][]*statusWatch{},
		logs:     map[string][]*dholev1.LogChunk{},
		secrets:  map[string]string{},
		redeemed: map[string]int{},
	}
	if err := os.MkdirAll(h.blobDir, 0o750); err != nil {
		return nil, fmt.Errorf("conformance: blob directory: %w", err)
	}

	embedded, err := bus.StartEmbedded(filepath.Join(root, "nats"))
	if err != nil {
		return nil, fmt.Errorf("conformance: embedded nats: %w", err)
	}
	h.nats = embedded

	plane, err := bus.Connect(ctx, embedded.URL())
	if err != nil {
		embedded.Close()
		return nil, fmt.Errorf("conformance: plane connection: %w", err)
	}
	h.plane = plane

	// The dispatch stream is a work queue, as the contract says: exactly one
	// engine receives each dispatch and an unacknowledged one is redelivered.
	if err := plane.EnsureWorkQueue(ctx, engine.DispatchStream, []string{"job.dispatch.>"}); err != nil {
		h.close()
		return nil, fmt.Errorf("conformance: dispatch stream: %w", err)
	}

	raw, err := nats.Connect(embedded.URL())
	if err != nil {
		h.close()
		return nil, fmt.Errorf("conformance: raw connection: %w", err)
	}
	h.raw = raw

	if err := h.subscribe(ctx); err != nil {
		h.close()
		return nil, err
	}
	return h, nil
}

// subscribe puts the plane's ear on every engine-to-plane subject BEFORE the
// engine starts. A registration published into an empty room is lost, and the
// suite would blame the engine for it.
func (h *harness) subscribe(ctx context.Context) error {
	statuses, err := h.plane.SubscribeEphemeral(ctx, "job.status.>", h.onStatus)
	if err != nil {
		return fmt.Errorf("conformance: subscribing to job.status.>: %w", err)
	}
	h.stop = append(h.stop, statuses)

	logs, err := h.plane.SubscribeEphemeral(ctx, "job.logs.>", h.onLog)
	if err != nil {
		return fmt.Errorf("conformance: subscribing to job.logs.>: %w", err)
	}
	h.stop = append(h.stop, logs)

	regs, err := h.plane.SubscribeEphemeral(ctx, bus.SubjectEngineRegistration(), h.onRegistration)
	if err != nil {
		return fmt.Errorf("conformance: subscribing to %s: %w", bus.SubjectEngineRegistration(), err)
	}
	h.stop = append(h.stop, regs)

	beats, err := h.plane.SubscribeEphemeral(ctx, "engine.heartbeat.>", h.onHeartbeat)
	if err != nil {
		return fmt.Errorf("conformance: subscribing to engine.heartbeat.>: %w", err)
	}
	h.stop = append(h.stop, beats)

	// Secret redemption, on the raw connection: the reply is a bare value, not
	// a protobuf message, because the contract defines no message for it.
	sub, err := h.raw.Subscribe(secretSubjectName, func(m *nats.Msg) {
		_ = m.Respond(h.redeem(string(m.Data)))
	})
	if err != nil {
		return fmt.Errorf("conformance: serving %s: %w", secretSubjectName, err)
	}
	h.stop = append(h.stop, func() { _ = sub.Unsubscribe() })
	return nil
}

func (h *harness) onStatus(data []byte) {
	status := &dholev1.JobStatus{}
	if err := proto.Unmarshal(data, status); err != nil {
		h.logf("conformance: undecodable JobStatus (%d bytes): %v", len(data), err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, w := range h.statuses[jobKey(status.GetRunId(), status.GetStepId())] {
		select {
		case w.ch <- status:
		default:
		}
	}
}

func (h *harness) onLog(data []byte) {
	chunk := &dholev1.LogChunk{}
	if err := proto.Unmarshal(data, chunk); err != nil {
		h.logf("conformance: undecodable LogChunk (%d bytes): %v", len(data), err)
		return
	}
	key := jobKey(chunk.GetRunId(), chunk.GetStepId())
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs[key] = append(h.logs[key], chunk)
}

// onRegistration and onHeartbeat both decode the FRAME. A body-less frame is
// an engine publishing a bare payload — the compatibility path the contract
// keeps open for engines one version behind, and explicitly not what an engine
// written now may do. It is recorded so the registration case can say exactly
// that rather than reporting a missing registration.
func (h *harness) onRegistration(data []byte) {
	msg, err := wire.DecodeEngineMessage(data)
	if err != nil {
		h.logf("conformance: undecodable EngineMessage on engine.registration: %v", err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if reg := msg.GetRegistration(); reg != nil {
		h.regs = append(h.regs, reg)
		return
	}
	h.unframed = append(h.unframed, bus.SubjectEngineRegistration())
}

func (h *harness) onHeartbeat(data []byte) {
	msg, err := wire.DecodeEngineMessage(data)
	if err != nil {
		h.logf("conformance: undecodable EngineMessage on engine.heartbeat: %v", err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if beat := msg.GetHeartbeat(); beat != nil {
		h.beats = append(h.beats, beat)
		return
	}
	h.unframed = append(h.unframed, "engine.heartbeat")
}

// redeem answers one redemption request. Handles are single-use by contract,
// so a second redemption of the same handle is refused — and the count is kept
// so the secret case can say whether the engine redeemed at all.
func (h *harness) redeem(handle string) []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	value, ok := h.secrets[handle]
	if !ok {
		return []byte("ERR unknown handle")
	}
	h.redeemed[handle]++
	if h.redeemed[handle] > 1 {
		return []byte("ERR handle already redeemed")
	}
	return []byte(value)
}

// startEngine launches the command under test with the environment the suite
// documents. Everything it needs to reach the bus and the object store is
// here; nothing is inherited from the suite's own process.
func (h *harness) startEngine(ctx context.Context) error {
	engineCtx, cancel := context.WithCancel(ctx)
	h.engineC = cancel

	// The command is not user input reaching a server: it is the argument of
	// a developer tool, named on the command line by whoever runs the suite,
	// and launching it is the entire point of the package. There is nothing to
	// sanitise -- an engine binary IS an arbitrary program.
	// #nosec G204 -- see above.
	// nosemgrep: dangerous-exec-command
	cmd := exec.CommandContext(engineCtx, h.cfg.Engine[0], h.cfg.Engine[1:]...)
	cmd.Dir = h.cfg.Dir
	cmd.Env = append(os.Environ(),
		"DHOLE_NATS_URL="+h.nats.URL(),
		"DHOLE_ENGINE_ID="+h.engineID,
		"DHOLE_ENGINE_TIER="+h.tier,
		"DHOLE_BLOB_DIR="+h.blobDir,
		"DHOLE_DISPATCH_STREAM="+engine.DispatchStream,
		"DHOLE_SECRET_SUBJECT="+secretSubjectName,
		"DHOLE_ENGINE_SLOTS=2",
	)
	cmd.Env = append(cmd.Env, h.cfg.Env...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return fmt.Errorf("conformance: engine stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return fmt.Errorf("conformance: engine stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("conformance: starting engine %q: %w", strings.Join(h.cfg.Engine, " "), err)
	}
	h.cmd = cmd
	go h.drain("engine stdout", stdout)
	go h.drain("engine stderr", stderr)
	return nil
}

func (h *harness) drain(what string, r io.Reader) {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			for _, line := range strings.Split(strings.TrimRight(string(buf[:n]), "\n"), "\n") {
				h.logf("conformance: %s: %s", what, line)
			}
		}
		if err != nil {
			return
		}
	}
}

func (h *harness) close() {
	if h.engineC != nil {
		h.engineC()
	}
	if h.cmd != nil {
		_ = h.cmd.Wait()
	}
	for _, stop := range h.stop {
		stop()
	}
	if h.raw != nil {
		h.raw.Close()
	}
	if h.plane != nil {
		h.plane.Close()
	}
	if h.nats != nil {
		h.nats.Close()
	}
}

// runCase runs one case under its own deadline and turns whatever it returns
// into a Result. A case that panics is a failure of the SUITE, and is reported
// as one rather than taking the whole run down.
func (h *harness) runCase(ctx context.Context, c kase, fallback time.Duration) (res Result) {
	timeout := c.timeout
	if timeout == 0 {
		timeout = fallback
	}
	res = Result{Name: c.name, Obligation: c.obligation, Status: StatusPassed}
	started := time.Now()
	defer func() {
		res.Duration = time.Since(started)
		if r := recover(); r != nil {
			res.Status = StatusFailed
			res.Detail = fmt.Sprintf("the suite panicked running this case: %v", r)
		}
	}()

	caseCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	h.resetHeartbeatCursor()
	h.logf("conformance: running %s", c.name)
	if err := c.run(caseCtx, h); err != nil {
		res.Status = StatusFailed
		res.Detail = err.Error()
		if h.exited() {
			res.Detail += "\n(the engine process has exited — see its stdout and stderr above)"
		}
	}
	return res
}

func (h *harness) exited() bool {
	return h.cmd != nil && h.cmd.ProcessState != nil
}

// ---- what the cases use -----------------------------------------------------

func jobKey(runID, stepID string) string { return runID + "/" + stepID }

func (h *harness) registrationSubject() string { return bus.SubjectEngineRegistration() }
func (h *harness) controlSubject() string      { return bus.SubjectEngineControl(h.engineID) }
func (h *harness) secretSubject() string       { return secretSubjectName }
func (h *harness) logsSubject(d *dholev1.JobDispatch) string {
	return bus.SubjectLogs(d.GetRunId(), d.GetStepId())
}

// newDispatchPrefix is the object-store prefix a case's step writes beneath.
func (h *harness) newDispatchPrefix(name string) string {
	h.mu.Lock()
	h.seq++
	n := h.seq
	h.mu.Unlock()
	return fmt.Sprintf("runs/conf-%s-%d", name, n)
}

// newDispatch builds a self-contained dispatch, as the contract's first rule
// requires: everything the engine needs is in it or reachable from it.
func (h *harness) newDispatch(name string) *dholev1.JobDispatch {
	prefix := h.newDispatchPrefix(name)
	runID := strings.TrimPrefix(prefix, "runs/")
	return &dholev1.JobDispatch{
		RunId:      runID,
		StepId:     name,
		Attempt:    1,
		FenceToken: fmt.Sprintf("conformance.fence.%s.1", runID),
		Step: &dholev1.Step{
			Id:          name,
			Name:        name,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
		},
		OutputPrefix:    prefix,
		ProtocolVersion: wire.ProtocolVersion,
		Tenant:          &dholev1.Tenant{Id: "conformance"},
	}
}

// watchStatus starts collecting the statuses for this dispatch. It is called
// BEFORE the dispatch is published, so a fast engine cannot answer into a room
// nobody is listening in.
func (h *harness) watchStatus(d *dholev1.JobDispatch) *statusWatch {
	w := &statusWatch{h: h, d: d, ch: make(chan *dholev1.JobStatus, 64)}
	key := jobKey(d.GetRunId(), d.GetStepId())
	h.mu.Lock()
	h.statuses[key] = append(h.statuses[key], w)
	h.mu.Unlock()
	return w
}

// dispatch publishes the job on the subject its tier and capability set name.
func (h *harness) dispatch(ctx context.Context, d *dholev1.JobDispatch) error {
	subject := bus.SubjectDispatch(h.tier, engine.CapsHash(d.GetStep().GetCapabilities()))
	if err := h.plane.Publish(ctx, subject, d); err != nil {
		return fmt.Errorf("publishing the dispatch on %s: %w", subject, err)
	}
	h.logf("conformance: dispatched run %s step %s on %s", d.GetRunId(), d.GetStepId(), subject)
	return nil
}

// cancel publishes EngineControl{Cancel} on the engine's control subject, with
// the fence the caller chose — which is the whole point of the fence case.
func (h *harness) cancel(ctx context.Context, d *dholev1.JobDispatch, fence string) error {
	control := &dholev1.EngineControl{Kind: &dholev1.EngineControl_Cancel{Cancel: &dholev1.Cancel{
		RunId:      d.GetRunId(),
		StepId:     d.GetStepId(),
		Attempt:    d.GetAttempt(),
		FenceToken: fence,
	}}}
	if err := h.plane.Publish(ctx, h.controlSubject(), control); err != nil {
		return fmt.Errorf("publishing EngineControl{Cancel} on %s: %w", h.controlSubject(), err)
	}
	return nil
}

// statusWatch is one case's view of one job's statuses.
type statusWatch struct {
	h    *harness
	d    *dholev1.JobDispatch
	ch   chan *dholev1.JobStatus
	seen []*dholev1.JobStatus
}

// await waits for the first status matching pred. Statuses that do not match
// are remembered, so a timeout can say what DID arrive.
func (w *statusWatch) await(ctx context.Context, pred func(*dholev1.JobStatus) bool, want string) (*dholev1.JobStatus, error) {
	started := time.Now()
	for {
		select {
		case s := <-w.ch:
			w.seen = append(w.seen, s)
			if pred(s) {
				return s, nil
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("expected %s for run %s step %s attempt %d on %s, got %s after %s",
				want, w.d.GetRunId(), w.d.GetStepId(), w.d.GetAttempt(),
				bus.SubjectStatus(w.d.GetRunId(), w.d.GetStepId()),
				describeStatuses(w.seen), time.Since(started).Round(time.Millisecond))
		}
	}
}

func describeStatuses(seen []*dholev1.JobStatus) string {
	if len(seen) == 0 {
		return "no JobStatus at all"
	}
	parts := make([]string, 0, len(seen))
	for _, s := range seen {
		part := s.GetPhase().String()
		if s.GetError() != "" {
			part += fmt.Sprintf("(error %q)", s.GetError())
		}
		parts = append(parts, part)
	}
	return "only " + strings.Join(parts, ", ")
}

// checkFence is the rule that makes duplicate delivery safe: the fence travels
// back unchanged, with the run, step and attempt it belongs to.
func (h *harness) checkFence(d *dholev1.JobDispatch, s *dholev1.JobStatus) error {
	if s.GetFenceToken() != d.GetFenceToken() {
		return fmt.Errorf("the JobStatus echoes fence_token %q, expected %q unchanged; the plane discards a status "+
			"whose fence is not the current lease, so an altered or omitted fence loses the result entirely",
			s.GetFenceToken(), d.GetFenceToken())
	}
	if s.GetRunId() != d.GetRunId() || s.GetStepId() != d.GetStepId() {
		return fmt.Errorf("the JobStatus reports run %q step %q, expected run %q step %q",
			s.GetRunId(), s.GetStepId(), d.GetRunId(), d.GetStepId())
	}
	if s.GetAttempt() != d.GetAttempt() {
		return fmt.Errorf("the JobStatus reports attempt %d, expected %d; a retry's result must not be filed "+
			"against the attempt before it", s.GetAttempt(), d.GetAttempt())
	}
	return nil
}

func (h *harness) logChunks(d *dholev1.JobDispatch) []*dholev1.LogChunk {
	h.mu.Lock()
	defer h.mu.Unlock()
	src := h.logs[jobKey(d.GetRunId(), d.GetStepId())]
	out := make([]*dholev1.LogChunk, len(src))
	copy(out, src)
	return out
}

// awaitLogContains waits until the live log carries want, which is how a case
// knows the step is genuinely RUNNING before it cancels it. An engine that
// reports ACCEPTED and never starts anything would otherwise pass by luck.
func (h *harness) awaitLogContains(ctx context.Context, d *dholev1.JobDispatch, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		var seen int
		for _, c := range h.logChunks(d) {
			seen += len(c.GetData())
			if strings.Contains(string(c.GetData()), want) {
				return nil
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("waited %s for the live log of run %s step %s on %s to contain %q; %d bytes of "+
				"LogChunk arrived. Chunks are best-effort, but a step that has produced nothing at all has "+
				"probably not been started", timeout, d.GetRunId(), d.GetStepId(), h.logsSubject(d), want, seen)
		}
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// awaitRegistration waits for the engine to announce itself, and refuses a
// bare payload: an engine written now must frame.
func (h *harness) awaitRegistration(ctx context.Context) (*dholev1.EngineRegistration, error) {
	deadline := time.Now().Add(15 * time.Second)
	for {
		h.mu.Lock()
		regs, unframed := h.regs, h.unframed
		h.mu.Unlock()
		if len(regs) > 0 {
			return regs[0], nil
		}
		if len(unframed) > 0 {
			return nil, fmt.Errorf("a message arrived on %s whose EngineMessage frame has NO body: that is the "+
				"compatibility path for an engine one version behind, which publishes a bare payload. Publish "+
				"EngineMessage{registration = <EngineRegistration>} (field 100) instead",
				bus.SubjectEngineRegistration())
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("no EngineMessage{registration} arrived on %s within %s of the engine starting; "+
				"an engine announces itself on start and re-announces every %s",
				bus.SubjectEngineRegistration(), 15*time.Second, engine.RegistrationInterval)
		}
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (h *harness) resetHeartbeatCursor() {
	h.mu.Lock()
	h.beatCursor = len(h.beats)
	h.mu.Unlock()
}

// nextHeartbeat returns the next heartbeat published after the cursor, which
// the runner resets before each case so an old beat cannot satisfy a new one.
func (h *harness) nextHeartbeat(ctx context.Context, timeout time.Duration) (*dholev1.EngineHeartbeat, error) {
	deadline := time.Now().Add(timeout)
	for {
		h.mu.Lock()
		if h.beatCursor < len(h.beats) {
			beat := h.beats[h.beatCursor]
			h.beatCursor++
			h.mu.Unlock()
			return beat, nil
		}
		unframed := len(h.unframed)
		h.mu.Unlock()
		if unframed > 0 {
			return nil, fmt.Errorf("a message on engine.heartbeat.%s arrived with an EngineMessage frame that has "+
				"no body; publish EngineMessage{heartbeat = <EngineHeartbeat>} (field 101). A bare payload is the "+
				"compatibility path for an engine one version behind", h.engineID)
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, fmt.Errorf("no EngineMessage{heartbeat} on %s within %s (the contract's interval is %s)",
				bus.SubjectEngineHeartbeat(h.engineID), timeout, engine.HeartbeatInterval)
		}
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// awaitHeartbeatWithout waits for a heartbeat that no longer declares this
// job, which is how a finished job stops holding its lease.
func (h *harness) awaitHeartbeatWithout(ctx context.Context, d *dholev1.JobDispatch, timeout time.Duration) (bool, error) {
	h.mu.Lock()
	from := len(h.beats)
	h.mu.Unlock()
	deadline := time.Now().Add(timeout)
	for {
		h.mu.Lock()
		beats := h.beats[min(from, len(h.beats)):]
		h.mu.Unlock()
		for _, beat := range beats {
			if findInFlight(beat, d) == nil {
				return true, nil
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			if len(beats) == 0 {
				return false, fmt.Errorf("no heartbeat at all on %s in the %s after the terminal status; an engine "+
					"that stops beating is presumed dead and its work re-dispatched",
					bus.SubjectEngineHeartbeat(h.engineID), timeout)
			}
			return false, nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// issueSecret registers a value the engine may redeem once, and returns the
// handle that stands for it. The VALUE never goes on the wire to the engine —
// that is the property the secret case exists to check.
func (h *harness) issueSecret(value string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	handle := fmt.Sprintf("conformance-handle-%d", h.seq)
	h.secrets[handle] = value
	return handle
}

func (h *harness) redemptions(handle string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.redeemed[handle]
}

// readBlob reads an object by the key a JobStatus named. The store is a
// directory (the harness's convention, since the contract defines no object
// store protocol); a key that escapes it is a failure, not a read.
func (h *harness) readBlob(key string) ([]byte, error) {
	path, err := h.resolve(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path) // #nosec G304 -- path is confined to the harness's own temp directory by resolve.
	if err != nil {
		return nil, fmt.Errorf("no object at key %q under the store the engine was given as DHOLE_BLOB_DIR: %w",
			key, err)
	}
	return data, nil
}

func (h *harness) writeBlob(key string, data []byte) error {
	path, err := h.resolve(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func (h *harness) resolve(key string) (string, error) {
	if key == "" {
		return "", errors.New("the object key is empty")
	}
	path := filepath.Join(h.blobDir, filepath.Clean("/"+key))
	if !strings.HasPrefix(path, h.blobDir+string(os.PathSeparator)) {
		return "", fmt.Errorf("object key %q escapes the store", key)
	}
	return path, nil
}

// readOutput reads what an OutputRef points at: the key it names, or the
// content-addressed layout when it names only a digest.
func (h *harness) readOutput(out *dholev1.OutputRef) ([]byte, error) {
	if key := out.GetKey(); key != "" {
		return h.readBlob(key)
	}
	if hex := out.GetDigest().GetHex(); hex != "" {
		algo := out.GetDigest().GetAlgo()
		if algo == "" {
			algo = "sha256"
		}
		return h.readBlob("cas/" + algo + "/" + hex)
	}
	return nil, fmt.Errorf("the OutputRef for port %q names neither a key nor a digest, so the bytes it reports "+
		"cannot be found by anybody", out.GetPort())
}
