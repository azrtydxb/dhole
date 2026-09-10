package engine

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
)

// running tracks the jobs this engine is executing so a control message can
// reach one.
//
// It is keyed by (run, step, attempt) — the same key the registry client holds
// an in-flight job under — because that triple is what a Cancel names, and
// because an engine may be running two attempts of two different runs of the
// same step id.
type running struct {
	mu   sync.Mutex
	jobs map[string]cancellable
}

// cancellable is one running job: how to stop it, and the fence it holds.
type cancellable struct {
	cancel context.CancelFunc
	fence  string
}

func newRunning() *running { return &running{jobs: map[string]cancellable{}} }

func (r *running) add(key, fence string, cancel context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[key] = cancellable{cancel: cancel, fence: fence}
}

func (r *running) remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.jobs, key)
}

// stop cancels the named job and reports whether it was there to cancel.
//
// A fence that does not match is REFUSED rather than ignored quietly: a
// control message quoting a stale fence is addressed to an attempt this engine
// no longer holds, and acting on it would kill the attempt that superseded it
// — the exact duplicate-execution failure the fence exists to prevent, run
// backwards.
func (r *running) stop(key, fence string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[key]
	if !ok || (fence != "" && job.fence != fence) {
		return false
	}
	job.cancel()
	return true
}

// watchControl subscribes to this engine's one inbound subject and acts on
// what arrives there.
//
// Engines are outbound-only otherwise: they dial the bus and nothing dials
// them, so `engine.control.<engine-id>` is the whole of the inbound path
// (docs/wire-contract.md). Until this existed the control plane could publish
// a Cancel and nothing on the far side was listening — the run would be closed
// in its log while the sandbox kept running, which is a cancellation that
// frees no capacity and tells the operator otherwise.
//
// Ephemeral, not durable: a control message is only meaningful to an engine
// that is running the job right now, and one redelivered to an engine that has
// restarted names an attempt it does not hold.
func (a *Agent) watchControl(ctx context.Context) (func(), error) {
	// A deadline for the SUBSCRIBE, not for the subscription: the bus
	// confirms a subscription with a round trip and refuses a context that
	// could wait forever, while the agent's own context has no deadline
	// because an engine runs until it is stopped.
	bind, cancel := context.WithTimeout(ctx, bindTimeout)
	defer cancel()
	return a.cfg.Bus.SubscribeEphemeral(bind, bus.SubjectEngineControl(a.cfg.EngineID),
		func(raw []byte) {
			control := &dholev1.EngineControl{}
			if err := proto.Unmarshal(raw, control); err != nil {
				return
			}
			// Cancel only, for now. Drain and Attach are declared by the
			// contract and served by nothing, and an engine that silently
			// swallowed them would be indistinguishable from one that acted.
			cancel := control.GetCancel()
			if cancel == nil {
				return
			}
			a.jobs.stop(
				jobKey(cancel.GetRunId(), cancel.GetStepId(), cancel.GetAttempt()),
				cancel.GetFenceToken())
		})
}
