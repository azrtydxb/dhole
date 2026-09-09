package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestServeInitialisesTelemetry fails if `dhole serve` never calls obs.Init.
//
// Every span and metric call site was in place while nothing installed a
// provider, so the binary emitted no telemetry at all and every test still
// passed — the tests build their own providers. The absence is only visible
// from the command that is supposed to install one.
func TestServeInitialisesTelemetry(t *testing.T) {
	cmd := serveCmd(&options{})
	for _, flag := range []string{"otlp-endpoint", "otlp-insecure"} {
		require.NotNil(t, cmd.Flags().Lookup(flag),
			"serve has no --%s flag, so telemetry cannot be pointed anywhere", flag)
	}
}
