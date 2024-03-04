package stage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/internal/cmds/vars"
	"github.com/leizor/kafka-util/internal/errorcheck"
)

var (
	reassignmentsFilepath string
	maxMovesPerBroker     int
	dryRun                bool

	long = `Execute a series of partition reassignments.

The partition reassignments is given as a JSON file in the same format that the
'kafka-reassign-partitions.sh' utility expects:

{
  "partitions": [
    {
      "topic": "foo",
      "partition": 1,
      "replicas": [1,2,3],
      "log_dirs": ["dir1", "dir2", "dir3"]
    }
  ],
  "version": 1
}

The '--max-moves-per-broker' flag specifies the maximum number of simultaneous 
inter-broker replica movements allowed per broker. A broker is considered part
of a replica movement if it is either the source or destination broker of the
replica or is the lead replica of the partition.
`
)

var Cmd = &cobra.Command{
	Use:   "stage",
	Short: "Execute a series of partition reassignments",
	Long:  long,
	RunE: func(cmd *cobra.Command, args []string) error {
		rs, err := readReassignments(reassignmentsFilepath)
		if err != nil {
			return fmt.Errorf("problem reading reassignments json file: %w", err)
		}

		if rs.Version != 1 {
			return fmt.Errorf("unsupported version '%d'", rs.Version)
		}

		client := &kafka.Client{Addr: kafka.TCP(vars.BootstrapServer)}

		err = ReconcileUntilDone(client, rs, maxMovesPerBroker)
		if err != nil {
			return err
		}

		fmt.Println("Reassignments complete.")
		return nil
	},
}

func init() {
	Cmd.Flags().StringVarP(&reassignmentsFilepath, "reassignment-json-file", "f", "", "json file with the reassignment configuration (required)")
	Cmd.Flags().IntVarP(&maxMovesPerBroker, "max-moves-per-broker", "m", 1, "max simultaneous inter-broker replica movements")

	err := Cmd.MarkFlagRequired("reassignment-json-file")
	if err != nil {
		panic(err)
	}
}

func ReconcileUntilDone(client *kafka.Client, rs *Reassignments, maxMovesPerBroker int) error {

	for i := 0; ; i++ {
		state, err := getCurrentState(client, rs)
		if err != nil {
			return fmt.Errorf("problem retrieving cluster state: %w", err)
		}

		var cur []Reassignment
		for _, r := range rs.Partitions {
			for _, s := range r.getReassignmentSteps(state.assignments, maxMovesPerBroker) {
				if state.maybeApplyReassignment(s, maxMovesPerBroker) {
					cur = append(cur, s)
				}
			}
		}

		if len(cur) == 0 {
			// Either no more reassignments are possible given the
			// constraints or current state matches the target state;
			// either way, we are done.
			break
		}

		fmt.Printf("Applying reassignments (phase %d):\n", i+1)
		for _, r := range cur {
			fmt.Println(r.String())
		}
		if err = applyReassignments(client, cur); err != nil {
			return fmt.Errorf("problem applying reassignments: %w", err)
		}

		fmt.Println()
	}

	return nil
}

// applyReassignments applies the given reassignments and does not return until
// the reassignments are complete.
func applyReassignments(client *kafka.Client, rs []Reassignment) error {
	assignments := make([]kafka.AlterPartitionReassignmentsRequestAssignment, len(rs))
	for i, r := range rs {
		assignments[i] = kafka.AlterPartitionReassignmentsRequestAssignment{
			Topic:       r.Topic,
			PartitionID: r.Partition,
			BrokerIDs:   r.Replicas,
		}
	}
	req := kafka.AlterPartitionReassignmentsRequest{
		Assignments: assignments,
		Timeout:     vars.Timeout,
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

	err = waitForReassignmentsFinish(client, rs)
	if err != nil {
		return fmt.Errorf("problem waiting for reassignments to finish: %w", err)
	}

	return nil
}

var errReassignmentsIncomplete = errors.New("reassignments-incomplete")

func waitForReassignmentsFinish(client *kafka.Client, rs []Reassignment) error {
	var topics []string
	byTP := make(map[string]Reassignment)
	for _, r := range rs {
		if !slices.Contains(topics, r.Topic) {
			topics = append(topics, r.Topic)
		}
		byTP[topicPartition(r.Topic, r.Partition)] = r
	}

	check := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), vars.Timeout)
		defer cancel()

		resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
		if err != nil {
			return backoff.Permanent(err)
		}

		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				tp := topicPartition(p.Topic, p.ID)
				if r, ok := byTP[tp]; ok {
					replicaSet := make([]int, len(p.Replicas))
					for i, b := range p.Replicas {
						replicaSet[i] = b.ID
					}

					if !slices.Equal(r.Replicas, replicaSet) {
						return errReassignmentsIncomplete
					}
				}
			}
		}

		return nil
	}

	infBackoff := backoff.NewExponentialBackOff()
	infBackoff.MaxElapsedTime = 0

	var totalWait time.Duration
	notify := func(err error, wait time.Duration) {
		totalWait += wait
		fmt.Printf("Waited %s for reassignment phase to complete...\n", totalWait.Truncate(time.Second).String())
	}

	return backoff.RetryNotify(check, infBackoff, notify)
}

type state struct {
	// brokers holds the number of inter-broker replica movements per broker.
	brokers     map[int]int
	assignments map[string][]int
}

// maybeApplyReassignment determines if the proposed reassignment satisfies
// maxMovesPerBroker and if applicable updates the internal state to reflect
// the expected post-reassignment state.
func (s state) maybeApplyReassignment(r Reassignment, maxMovesPerBroker int) bool {
	delta := make(map[int]bool)

	// Add destination brokers.
	for _, b := range r.adding {
		delta[b] = true
	}
	// Add source brokers.
	for _, b := range r.removing {
		delta[b] = true
	}

	tp := topicPartition(r.Topic, r.Partition)

	// Add lead replica of the partition.
	leadReplica := s.assignments[tp][0]
	delta[leadReplica] = true

	for b, numMoves := range s.brokers {
		if delta[b] {
			numMoves += 1
		}
		if numMoves > maxMovesPerBroker {
			return false
		}
	}

	// The reassignment is accepted; apply it to the internal state.
	for b := range delta {
		s.brokers[b]++
	}
	s.assignments[tp] = r.Replicas

	return true
}

func getCurrentState(client *kafka.Client, rs *Reassignments) (state, error) {
	var topics []string
	for _, r := range rs.Partitions {
		if !slices.Contains(topics, r.Topic) {
			topics = append(topics, r.Topic)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), vars.Timeout)
	defer cancel()

	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return state{}, fmt.Errorf("problem retrieving metadata: %w", err)
	}

	brokers := make(map[int]int)
	for _, b := range resp.Brokers {
		brokers[b.ID] = 0
	}

	assignments := make(map[string][]int)
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			replicas := make([]int, len(p.Replicas))
			for i, b := range p.Replicas {
				replicas[i] = b.ID
			}

			tp := topicPartition(p.Topic, p.ID)
			assignments[tp] = replicas
		}
	}

	return state{brokers: brokers, assignments: assignments}, nil
}

func topicPartition(topic string, partition int) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

type Reassignments struct {
	Partitions []Reassignment `json:"partitions"`
	Version    int            `json:"version"`

	lookup map[string]map[int][]int
}

type Reassignment struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	Replicas  []int    `json:"replicas"`
	LogDirs   []string `json:"log_dirs,omitempty"`

	adding   []int
	removing []int
}

func (r Reassignment) String() string {
	return fmt.Sprintf("%s-%d; replicas: %v, adding: %v, removing: %v", r.Topic, r.Partition, r.Replicas, r.adding, r.removing)
}

// getReassignmentSteps breaks an assignment into smaller steps depending on
// what maxMovesPerBroker is provided.
func (r Reassignment) getReassignmentSteps(assignments map[string][]int, maxMovesPerBroker int) []Reassignment {
	current := assignments[topicPartition(r.Topic, r.Partition)]
	l := len(r.Replicas)
	if l < len(current) {
		l = len(current)
	}

	type replicaMvmt struct {
		idx int
		add int
		rmv int
	}

	var steps [][]replicaMvmt
	for i := 0; i < l; i++ {
		rm := replicaMvmt{idx: i, add: -1, rmv: -1}

		if i < len(r.Replicas) {
			rm.add = r.Replicas[i]
		}
		if i < len(current) {
			rm.rmv = current[i]
		}

		if rm.add == rm.rmv {
			// Not a replica movement.
			continue
		}

		if steps == nil || (maxMovesPerBroker > 0 && len(steps[len(steps)-1]) >= maxMovesPerBroker) {
			steps = append(steps, make([]replicaMvmt, 0))
		}
		steps[len(steps)-1] = append(steps[len(steps)-1], rm)
	}

	rsSteps := make([]Reassignment, len(steps))
	prv := current
	for i, s := range steps {
		mid := make([]int, len(prv))
		for j, b := range prv {
			mid[j] = b
		}

		var adding, removing []int
		for _, m := range s {
			if m.add > -1 && m.rmv > -1 {
				// Replacement.
				mid[m.idx] = m.add
				adding = append(adding, m.add)
				removing = append(removing, m.rmv)
			} else if m.add > -1 {
				// Increasing RF.
				mid = append(mid, m.add)
				adding = append(adding, m.add)
			} else if m.rmv > -1 {
				// Decreasing RF.
				mid = slices.DeleteFunc(mid, func(b int) bool {
					return b == m.rmv
				})
				removing = append(removing, m.rmv)
			}
		}
		rsSteps[i] = Reassignment{
			Topic:     r.Topic,
			Partition: r.Partition,
			Replicas:  mid,
			adding:    adding,
			removing:  removing,
		}

		prv = mid
	}

	return rsSteps
}

func readReassignments(filepath string) (*Reassignments, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("problem opening file: %w", err)
	}

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("problem reading file: %w", err)
	}

	var r Reassignments
	if err = json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("problem unmarshalling json: %w", err)
	}

	return &r, nil
}
