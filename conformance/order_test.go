package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
)

// An engine publishes a step's LogChunks and THEN its terminal status, on one
// connection, in that order. The harness must see them in that order too: a
// case that reads the live log the moment the terminal status arrives is
// asking "did any chunk come before this?", and the answer has to be the
// engine's, not the scheduler's.
//
// It was the scheduler's. Statuses and logs were two subscriptions, and the
// NATS client delivers each subscription on its own goroutine, so the status
// could be handled before a chunk published ahead of it. On a fast Linux
// runner a one-line step lost that race every night, and dispatch-and-success
// failed the reference engine with "no LogChunk arrived" for a chunk that had.
func TestTheHarnessSeesAChunkPublishedBeforeTheStatusThatFollowsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h, err := startHarness(ctx, Config{Tier: "trusted", EngineID: "order-probe", Logf: t.Logf}, t.TempDir())
	require.NoError(t, err)
	defer h.close()

	const attempts = 300
	for i := range attempts {
		d := h.newDispatch(fmt.Sprintf("order-%d", i))
		watch := h.watchStatus(d)

		chunk, err := proto.Marshal(&dholev1.LogChunk{
			RunId: d.GetRunId(), StepId: d.GetStepId(), Seq: 1, Data: []byte("hello\n"), Attempt: 1,
		})
		require.NoError(t, err)
		status, err := proto.Marshal(&dholev1.JobStatus{
			RunId: d.GetRunId(), StepId: d.GetStepId(), Attempt: 1,
			Phase: dholev1.Phase_PHASE_SUCCEEDED, FenceToken: d.GetFenceToken(),
		})
		require.NoError(t, err)
		// Back to back on one connection, exactly as an engine sends them.
		require.NoError(t, h.raw.Publish(bus.SubjectLogs(d.GetRunId(), d.GetStepId()), chunk))
		require.NoError(t, h.raw.Publish(bus.SubjectStatus(d.GetRunId(), d.GetStepId()), status))
		require.NoError(t, h.raw.Flush())

		_, err = watch.await(ctx, terminal, "a terminal JobStatus")
		require.NoError(t, err)
		require.NotEmpty(t, h.logChunks(d),
			"attempt %d: the terminal status was handled before the chunk published ahead of it", i)
	}
}
