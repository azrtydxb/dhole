package executor

import (
	"fmt"
	"strings"
)

// WithDeclaredEnvironment returns e reporting id as its environment identity.
//
// It exists for the operator whose environment IS reproducible but whose
// backend cannot prove it. A process executor runs against whatever its host
// carries and so reports no stable identity (ADR 0021), which is the honest
// answer for a laptop and the wrong one for a fleet of engines built from one
// immutable image: those hosts really are identical, and nothing in the
// executor can see that.
//
// This is a DECLARATION, not a discovery, and the difference is the whole
// risk. An operator who declares one identity across hosts that are not
// actually identical gets one host's results served as another's — silently,
// because that is exactly what a cache hit looks like. So it is opt-in, it is
// never inferred, and the value should be something that changes when the
// environment does: the image digest the hosts were built from, not "prod".
//
// The backend keeps every other behaviour, including its capabilities: naming
// an environment says what the steps run IN, never what they are allowed to do.
func WithDeclaredEnvironment(e Executor, id string) (Executor, error) {
	if e == nil {
		return nil, fmt.Errorf("executor: no backend to declare an environment for")
	}
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("executor: a declared environment identity may not be blank")
	}
	return &declaredEnv{Executor: e, id: id}, nil
}

type declaredEnv struct {
	Executor
	id string
}

// EnvironmentIdentity reports what the operator declared, ignoring what the
// backend would have said — including ErrNoStableIdentity, which is the case
// this exists for.
func (d *declaredEnv) EnvironmentIdentity() (string, error) { return d.id, nil }
