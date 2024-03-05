package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/leizor/kafka-util/internal/cmds/stage"
	"github.com/leizor/kafka-util/internal/cmds/vars"
)

func TestReconcileUntilDone(t *testing.T) {
	kc, err := StartKafkaCluster(context.Background(), 4, nil)
	t.Cleanup(func() {
		// Cleanup needs to be called even if there was an error.
		require.NoError(t, kc.Cleanup(context.Background()))
	})
	require.NoError(t, err)

	err = kc.CreateTopicWithTraffic("foobar", [][]int{{0, 1}})
	require.NoError(t, err)

	vars.Timeout = 1 * time.Minute
	require.NoError(t, stage.ReconcileUntilDone(kc.GetClient(), &stage.Reassignments{
		Version: 1,
		Partitions: []stage.Reassignment{
			{
				Topic:     "foobar",
				Partition: 0,
				Replicas:  []int{2, 3},
			},
		},
	}, 1, false))

	assertReplicaAssignments(t, kc.GetClient(), "foobar", [][]int{{2, 3}})
}
