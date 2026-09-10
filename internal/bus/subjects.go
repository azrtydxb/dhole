package bus

// The subject table from docs/wire-contract.md, and nothing else. That document
// is the contract every engine — in any language — is written against, so these
// builders exist to keep one spelling of each subject in the Go tree. If a
// builder here and the document ever disagree, the document wins.
//
//	job.dispatch.<tier>.<caps>     plane  -> engine   JobDispatch
//	job.status.<run>.<step>        engine -> plane    JobStatus
//	job.logs.<run>.<step>          engine -> viewers  LogChunk
//	engine.control.<engine-id>     plane  -> engine   EngineControl
//	engine.heartbeat.<engine-id>   engine -> plane    EngineHeartbeat
//	engine.registration            engine -> plane    EngineRegistration
//	secret.redeem                  engine -> plane    handle -> value (raw)

// SubjectDispatch carries one JobDispatch to the tier and capability set it was
// scheduled for. It is a work queue: exactly one engine receives each dispatch.
func SubjectDispatch(tier, capsHash string) string {
	return "job.dispatch." + tier + "." + capsHash
}

// SubjectDispatchWildcard matches every capability set within one tier. An
// engine's credentials permit subscribing to its own tier's wildcard only.
func SubjectDispatchWildcard(tier string) string {
	return "job.dispatch." + tier + ".*"
}

// SubjectStatus carries JobStatus from an engine back to the plane. Durable:
// the control plane must not miss one.
func SubjectStatus(runID, stepID string) string {
	return "job.status." + runID + "." + stepID
}

// SubjectLogs carries live LogChunks to whoever is watching. Ephemeral and
// best-effort — the authoritative log is the object the engine writes.
func SubjectLogs(runID, stepID string) string {
	return "job.logs." + runID + "." + stepID
}

// SubjectEngineControl is the one inbound path to an engine, delivered over the
// connection the engine itself dialled.
func SubjectEngineControl(engineID string) string {
	return "engine.control." + engineID
}

// SubjectEngineHeartbeat carries an engine's periodic proof of life and the
// list of what it holds.
func SubjectEngineHeartbeat(engineID string) string {
	return "engine.heartbeat." + engineID
}

// SubjectEngineRegistration is where an engine announces itself. It is not
// per-engine: the plane listens on one subject for all of them.
func SubjectEngineRegistration() string {
	return "engine.registration"
}

// SubjectSecretRedeem is where an engine exchanges a SecretRef handle for the
// value behind it: a raw request carrying the handle, a raw reply carrying the
// value, or one beginning "ERR " to refuse. The control plane serves it.
//
// It is not under job.* because it is not addressed to a run: one step's
// dispatch may carry handles issued for several bindings, and the responder
// answers by handle alone. It is not under engine.* either — nothing about it
// is per-engine, and putting it there would have engines subscribing to their
// siblings' redemptions under the existing engine.> permission.
func SubjectSecretRedeem() string {
	return "secret.redeem"
}
