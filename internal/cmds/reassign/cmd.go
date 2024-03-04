package reassign

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/internal/cmds/vars"
	"github.com/leizor/kafka-util/internal/errorcheck"
)

var (
	topic     string
	partition int
	replicas  string
)

var Cmd = &cobra.Command{
	Use:   "reassign",
	Short: "Execute a single partition reassignment",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := &kafka.Client{Addr: kafka.TCP(vars.BootstrapServer)}

		brokerIDs, err := getBrokerIDs(replicas)
		if err != nil {
			return fmt.Errorf("problem parsing replica set: %w", err)
		}

		err = Partition(client, topic, partition, brokerIDs)
		if err != nil {
			return err
		}

		fmt.Println("Reassignment executed.")
		return nil
	},
}

func init() {
	Cmd.Flags().StringVarP(&topic, "topic", "t", "", "topic to reassign (required)")
	Cmd.Flags().IntVarP(&partition, "partition", "p", 0, "partition to reassign (required)")
	Cmd.Flags().StringVarP(&replicas, "replicas", "r", "", "replica set to assign the topic-partition to (ex: 1,2,3) (required)")

	for _, flag := range []string{"topic", "partition", "replicas"} {
		err := Cmd.MarkFlagRequired(flag)
		if err != nil {
			panic(err)
		}
	}
}

func Partition(client *kafka.Client, topic string, partition int, brokerIDs []int) error {
	req := kafka.AlterPartitionReassignmentsRequest{
		Topic: topic,
		Assignments: []kafka.AlterPartitionReassignmentsRequestAssignment{
			{
				PartitionID: partition,
				BrokerIDs:   brokerIDs,
			},
		},
		Timeout: vars.Timeout,
	}

	ctx, cancel := context.WithTimeout(context.Background(), vars.Timeout)
	defer cancel()

	resp, err := client.AlterPartitionReassignments(ctx, &req)
	if err != nil {
		return fmt.Errorf("problem executing partition reassignments: %w", err)
	}
	if err = errorcheck.CheckRespErrors(resp); err != nil {
		return fmt.Errorf("partition reassignment response contained errors: %w", err)
	}

	// Wait until either the reassignment has started or it is done. This makes
	// the `reassign` subcommand chainable with the `ongoing --wait` subcommand.
	err = waitForReassignmentStartOrFinish(client, topic, partition, brokerIDs)
	if err != nil {
		return fmt.Errorf("problem waiting for reassignment to start: %w", err)
	}

	return nil
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

var errWaitCondUnsatisfied = errors.New("wait-cond-unsatisfied")

func waitForReassignmentStartOrFinish(client *kafka.Client, topic string, partition int, replicas []int) error {
	return backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), vars.Timeout)
		defer cancel()

		resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		if err != nil {
			return backoff.Permanent(err)
		}

		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if p.Topic == topic && p.ID == partition {
					replicaSet := make([]int, len(p.Replicas))
					for i, b := range p.Replicas {
						replicaSet[i] = b.ID
					}

					if len(replicaSet) > len(replicas) {
						// The new replica set includes the new broker; the
						// reassignment has started.
						return nil
					}

					// If all replicas are in the replica set then we know the
					// reassignment has either started or already finished.
					for _, b := range replicas {
						if !slices.Contains(replicaSet, b) {
							return errWaitCondUnsatisfied
						}
					}
				}
			}
		}

		return nil
	}, backoff.NewExponentialBackOff())
}
