package integration

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"

	cmd "github.com/leizor/kafka-util/internal/cmds/reassign-partition"
	"github.com/leizor/kafka-util/internal/cmds/vars"
)

func TestReassignPartition(t *testing.T) {
	kc, err := StartKafkaCluster(context.Background(), 3)
	t.Cleanup(func() {
		// Cleanup needs to be called even if there was an error.
		require.NoError(t, kc.Cleanup(context.Background()))
	})
	require.NoError(t, err)

	err = kc.CreateTopicWithTraffic("foobar", [][]int{{0, 1}, {1, 0}})
	require.NoError(t, err)

	time.Sleep(1 * time.Minute) // Let the partitions accumulate a bit of data first.

	vars.BootstrapServer = kc.GetBootstrapServer()
	require.NoError(t, cmd.ReassignPartition("foobar", 0, "2,0"))

	assertReplicaAssignments(t, kc.GetClient(), "foobar", [][]int{{2, 0}, {1, 0}})
}

func assertReplicaAssignments(t *testing.T, client *kafka.Client, topicName string, expected [][]int) {
	t.Helper()

	resp, err := client.Metadata(context.Background(), &kafka.MetadataRequest{Topics: []string{topicName}})
	require.NoError(t, err)

	require.Len(t, resp.Topics, 1)
	require.Equal(t, topicName, resp.Topics[0].Name)

	actual := make([][]int, len(resp.Topics[0].Partitions))
	for _, p := range resp.Topics[0].Partitions {
		replicas := make([]int, len(p.Replicas))
		for i, b := range p.Replicas {
			replicas[i] = b.ID
		}

		actual[p.ID] = replicas
	}

	require.Equal(t, expected, actual)
}
