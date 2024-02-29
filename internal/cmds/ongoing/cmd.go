package ongoing

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/internal/cmds/vars"
	"github.com/leizor/kafka-util/internal/errorcheck"
)

var (
	cancel bool
	wait   bool
)

var Cmd = &cobra.Command{
	Use: "ongoing",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := &kafka.Client{Addr: kafka.TCP(vars.BootstrapServer)}

		if cancel {
			ongoing, err := listOngoing(client)
			if err != nil {
				return fmt.Errorf("problem listing ongoing reassignments: %w", err)
			}

			err = cancelReassignments(client, ongoing)
			if err != nil {
				return fmt.Errorf("problem canceling partition reassignments: %w", err)
			}

			fmt.Println("Ongoing reassignments cancelled:")
			fmt.Println()
			printOngoing(ongoing)
			return nil
		}

		if wait {
			var totalWait time.Duration
			err := Wait(client, func(err error, wait time.Duration) {
				totalWait += wait
				fmt.Printf("Waited %s for ongoing reassignments to complete...\n", totalWait.Truncate(time.Second).String())
			})
			if err != nil {
				return fmt.Errorf("problem waiting for reassignments: %w", err)
			}

			fmt.Println("No ongoing reassignments.")
			return nil
		}

		ongoing, err := listOngoing(client)
		if err != nil {
			return fmt.Errorf("problem listing ongoing reassignments: %w", err)
		}
		printOngoing(ongoing)

		return nil
	},
}

func init() {
	Cmd.Flags().BoolVarP(&cancel, "cancel", "c", false, "Cancel any ongoing reassignments.")
	Cmd.Flags().BoolVarP(&wait, "wait", "w", false, "Wait for any ongoing reassignments to complete.")
}

type reassignment struct {
	topic     string
	partition int
	replicas  []int
	adding    []int
	removing  []int
}

func (r reassignment) toCancelAssignment() kafka.AlterPartitionReassignmentsRequestAssignment {
	return kafka.AlterPartitionReassignmentsRequestAssignment{
		Topic:       r.topic,
		PartitionID: r.partition,
		BrokerIDs:   nil,
	}
}

func (r reassignment) String() string {
	return fmt.Sprintf("%s-%d; replicas: %v, adding: %v, removing: %v", r.topic, r.partition, r.replicas, r.adding, r.removing)
}

func listOngoing(client *kafka.Client) ([]reassignment, error) {
	ctx, cancel := context.WithTimeout(context.Background(), vars.Timeout)
	defer cancel()

	resp, err := client.ListPartitionReassignments(ctx, &kafka.ListPartitionReassignmentsRequest{})
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, err
	}

	var ongoing []reassignment
	for topic, t := range resp.Topics {
		for _, p := range t.Partitions {
			ongoing = append(ongoing, reassignment{
				topic:     topic,
				partition: p.PartitionIndex,
				replicas:  p.Replicas,
				adding:    p.AddingReplicas,
				removing:  p.RemovingReplicas,
			})
		}
	}

	return ongoing, nil
}

func cancelReassignments(client *kafka.Client, reassignments []reassignment) error {
	toCancel := make([]kafka.AlterPartitionReassignmentsRequestAssignment, len(reassignments))
	for i, r := range reassignments {
		toCancel[i] = r.toCancelAssignment()
	}

	ctx, cancel := context.WithTimeout(context.Background(), vars.Timeout)
	defer cancel()

	resp, err := client.AlterPartitionReassignments(ctx, &kafka.AlterPartitionReassignmentsRequest{
		Assignments: toCancel,
		Timeout:     vars.Timeout,
	})
	if err != nil {
		return err
	}
	if err = errorcheck.CheckRespErrors(resp); err != nil {
		return fmt.Errorf("partition reassignment response contained errors: %w", err)
	}

	return nil
}

func printOngoing(ongoing []reassignment) {
	slices.SortFunc(ongoing, func(a, b reassignment) int {
		if a.topic != b.topic {
			return cmp.Compare(a.topic, b.topic)
		}
		return cmp.Compare(a.partition, b.partition)
	})

	for _, r := range ongoing {
		fmt.Println(r.String())
	}
}

var errOngoing = errors.New("ongoing")

func Wait(client *kafka.Client, notify func(error, time.Duration)) error {
	check := func() error {
		ongoing, err := listOngoing(client)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("problem listing ongoing reassignments: %w", err))
		}

		if len(ongoing) > 0 {
			return errOngoing
		}

		return nil
	}

	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 0

	return backoff.RetryNotify(check, b, notify)
}
