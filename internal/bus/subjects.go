package bus

// The subject table from docs/wire-contract.md, and nothing else. That document
// is the contract every engine — in any language — is written against, so these
// builders exist to keep one spelling of each subject in the Go tree. If a
// builder here and the document ever disagree, the document wins.
//
//	job.dispatch.<tier>.<caps>         plane  -> engine   JobDispatch (any kind)
//	job.dispatch.<tier>.<caps>.<kind>  plane  -> engine   JobDispatch (one kind)
//	job.status.<run>.<step>            engine -> plane    JobStatus
//	job.logs.<run>.<step>              engine -> viewers  LogChunk
//	engine.control.<engine-id>         plane  -> engine   EngineControl
//	engine.heartbeat.<engine-id>       engine -> plane    EngineHeartbeat
//	engine.registration                engine -> plane    EngineRegistration
//	secret.redeem                      engine -> plane    handle -> value (raw)

// SubjectDispatch carries one JobDispatch to the tier and capability set it was
// scheduled for. It is a work queue: exactly one engine receives each dispatch.
func SubjectDispatch(tier, capsHash string) string {
	return "job.dispatch." + tier + "." + capsHash
}

// SubjectDispatchKind carries one JobDispatch to a single executor KIND within
// a tier and capability set. It exists because Match only decides whether a
// step CAN be placed: on kw a step naming engine_type vm, in a tier holding a
// vm-backed and a kubernetes-backed engine, ran on the kubernetes one, because
// both pulled the same SubjectDispatch work queue and whichever grabbed it
// first ran it. The kind therefore has to be in the route, not checked on
// receipt — a dispatch refused after delivery has already left the queue, and
// its redelivery is a race rather than a route.
//
// Keeping SubjectDispatch as the "any kind" subject is what makes this
// backward compatible: an engine built before this token existed subscribes
// only there, so it keeps taking unrestricted work and can never be handed
// kind-targeted work it would not know to honour.
//
// kind is one subject token: it comes from executor.Kind(), and a kind
// carrying a dot, a wildcard or a space would widen or split the route rather
// than name it.
func SubjectDispatchKind(tier, capsHash, kind string) string {
	return SubjectDispatch(tier, capsHash) + "." + kind
}

// SubjectDispatchWildcard matches everything dispatched within one tier —
// every capability set and every executor kind. An engine's credentials permit
// subscribing to its own tier's wildcard only.
//
// It is `>` rather than `*` because `*` matches exactly ONE token, and a
// kind-targeted dispatch has one token more. Under `*` an engine's own tier's
// kind subjects were outside its permissions, so the consumer it needs most
// was refused by the server. The tier token is still fixed, so this widens
// what an engine may pull WITHIN its tier and nothing about which tier.
func SubjectDispatchWildcard(tier string) string {
	return "job.dispatch." + tier + ".>"
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
