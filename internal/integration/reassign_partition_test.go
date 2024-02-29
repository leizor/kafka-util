package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/leizor/kafka-util/internal/cmds/ongoing"
	"github.com/leizor/kafka-util/internal/cmds/reassign"
	"github.com/leizor/kafka-util/internal/cmds/vars"
)

func TestReassignPartitionAndWait(t *testing.T) {
	kc, err := StartKafkaCluster(context.Background(), 3, nil)
	t.Cleanup(func() {
		// Cleanup needs to be called even if there was an error.
		require.NoError(t, kc.Cleanup(context.Background()))
	})
	require.NoError(t, err)

	err = kc.CreateTopicWithTraffic("foobar", [][]int{{0, 1}, {1, 0}})
	require.NoError(t, err)

	vars.Timeout = 1 * time.Minute
	require.NoError(t, reassign.Partition(kc.GetClient(), "foobar", 0, []int{2, 0}))
	require.NoError(t, ongoing.Wait(kc.GetClient(), nil))

	assertReplicaAssignments(t, kc.GetClient(), "foobar", [][]int{{2, 0}, {1, 0}})
}
