package reassign_partition

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/pkg/cmds/vars"
)

var (
	topic     string
	partition int
	replicas  string
)

var ReassignPartitionCmd = &cobra.Command{
	Use: "reassign-partition",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := &kafka.Client{Addr: kafka.TCP(vars.BootstrapServer)}

		brokerIDs, err := getBrokerIDs(replicas)
		if err != nil {
			return fmt.Errorf("problem parsing replica set: %w", err)
		}
		req := kafka.AlterPartitionReassignmentsRequest{
			Topic: topic,
			Assignments: []kafka.AlterPartitionReassignmentsRequestAssignment{
				{
					PartitionID: partition,
					BrokerIDs:   brokerIDs,
				},
			},
			Timeout: 60 * time.Second,
		}

		resp, err := client.AlterPartitionReassignments(context.Background(), &req)
		if err != nil {
			return fmt.Errorf("problem executing partition reassignments: %w", err)
		}
		if err = checkRespErrors(resp); err != nil {
			return fmt.Errorf("partition reassignment response contained errors: %w", err)
		}

		waitForReassignment(client, topic, partition, brokerIDs)

		log.Info().Msg("done")

		return nil
	},
}

func init() {
	ReassignPartitionCmd.Flags().StringVarP(&topic, "topic", "t", "", "The topic to reassign")
	ReassignPartitionCmd.Flags().IntVarP(&partition, "partition", "p", 0, "The partition to reassign")
	ReassignPartitionCmd.Flags().StringVarP(&replicas, "replicas", "r", "", "The replica set to assign the topic-partition to (ex: 1,2,3)")

	for _, flag := range []string{"topic", "partition", "replicas"} {
		err := ReassignPartitionCmd.MarkFlagRequired(flag)
		if err != nil {
			panic(err)
		}
	}
}

func getBrokerIDs(replicas string) (res []int, err error) {
	for _, b := range strings.Split(replicas, ",") {
		brokerID, err := strconv.Atoi(b)
		if err != nil {
			return nil, err
		}

		res = append(res, brokerID)
	}

	return res, nil
}

// checkRespErrors inventories any errors in a *kafka.AlterPartitionReassignmentsResponse and returns them
// in a single consolidated error.
func checkRespErrors(resp *kafka.AlterPartitionReassignmentsResponse) error {
	var errs []string

	if resp.Error != nil {
		errs = append(errs, resp.Error.Error())
	}
	for _, res := range resp.PartitionResults {
		if res.Error != nil {
			errs = append(errs, fmt.Sprintf("[partition %d] %s", res.PartitionID, res.Error))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	} else {
		return nil
	}
}

func waitForReassignment(client *kafka.Client, topic string, partition int, replicas []int) {
	req := kafka.MetadataRequest{
		Topics: []string{topic},
	}

	totalWaitTimeSec := 0

	for {
		resp, err := client.Metadata(context.Background(), &req)
		if err != nil {
			log.Warn().Err(err).Msg("problem retrieving metadata")
		}

		for _, respTopic := range resp.Topics {
			if respTopic.Name == topic {
				for _, respPartition := range respTopic.Partitions {
					if respPartition.ID == partition {
						if assertReplicasInISR(replicas, respPartition.Isr) {
							return
						}
					}
				}
			}
		}

		time.Sleep(30 * time.Second)

		totalWaitTimeSec += 30
		log.Info().Int("seconds", totalWaitTimeSec).Msg("waiting for reassignment to complete")
	}
}

func assertReplicasInISR(replicas []int, isr []kafka.Broker) bool {
	if len(replicas) != len(isr) {
		return false
	}
	for _, brokerID := range replicas {
		if !isInISR(brokerID, isr) {
			return false
		}
	}
	return true
}

func isInISR(brokerID int, isr []kafka.Broker) bool {
	for _, b := range isr {
		if b.ID == brokerID {
			return true
		}
	}
	return false
}
