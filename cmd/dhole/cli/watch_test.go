package cli_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"

	"github.com/azrtydxb/dhole/cmd/dhole/cli"
)

// slowRunStub is a run that takes longer than the CLI's --timeout: one event
// at once, the run's end only after a pause.
type slowRunStub struct {
	dholev1connect.UnimplementedPipelineServiceHandler
	pause time.Duration
}

func (s *slowRunStub) WatchRun(
	ctx context.Context, _ *connect.Request[dholev1.WatchRunRequest],
	stream *connect.ServerStream[dholev1.WatchRunResponse],
) error {
	if err := stream.Send(&dholev1.WatchRunResponse{Sequence: 1, Type: "RUN_CREATED"}); err != nil {
		return err
	}
	select {
	case <-time.After(s.pause):
	case <-ctx.Done():
		return ctx.Err()
	}
	return stream.Send(&dholev1.WatchRunResponse{Sequence: 2, Type: "RUN_COMPLETED"})
}

// TestFollowingARunOutlivesTheCallTimeout: --timeout bounds how long the
// server may take to answer, not how long a run may take. On kw an eleven
// minute pipeline cut `dhole run watch` off after the first minute with
// "deadline_exceeded", which reads like the run failed.
func TestFollowingARunOutlivesTheCallTimeout(t *testing.T) {
	url := startStubAPI(t, &slowRunStub{pause: 1500 * time.Millisecond})

	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	code := cli.Main([]string{
		"--server", url, "--token", goodToken, "--timeout", "500ms", "run", "watch", "run_1",
	}, out, errOut)
	require.Zero(t, code, "a run longer than --timeout was cut off: %s", errOut)
	require.Contains(t, out.String(), "RUN_COMPLETED")
}
