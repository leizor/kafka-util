package topics

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/internal/cmds/vars"
)

var (
	topics      []string
	generate    bool
	leaderDist  bool
	replicaDist bool
)

var Cmd = &cobra.Command{
	Use:   "topics",
	Short: "Describe topics",
	RunE: func(cmd *cobra.Command, args []string) error {
		if generate {
			err := printTopicsToMoveJSONFile(topics)
			if err != nil {
				return fmt.Errorf("problem generating topics-to-move json file: %w", err)
			}
			return nil
		}

		client := &kafka.Client{Addr: kafka.TCP(vars.BootstrapServer)}
		err := printDescribeTopics(client, topics)
		if err != nil {
			return fmt.Errorf("problem describing topics: %w", err)
		}

		return nil
	},
}

func init() {
	Cmd.Flags().StringArrayVarP(&topics, "topic", "t", []string{}, "target topics")
	Cmd.Flags().BoolVarP(&generate, "generate", "g", false, "generate a topics-to-move json file")
	Cmd.Flags().BoolVarP(&leaderDist, "leader-distribution", "l", false, "show partition leadership distribution per topic")
	Cmd.Flags().BoolVarP(&replicaDist, "replica-distribution", "r", false, "show partition replica distribution per topic")
}

type topicObj struct {
	Topic string `json:"topic"`
}

type topicsToMove struct {
	Version int        `json:"version"`
	Topics  []topicObj `json:"topics"`
}

func printTopicsToMoveJSONFile(topics []string) error {
	topicObjs := make([]topicObj, len(topics))
	for i, t := range topics {
		topicObjs[i] = topicObj{Topic: t}
	}

	out := topicsToMove{Version: 1, Topics: topicObjs}
	b, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("problem marshaling topics obj: %w", err)
	}

	fmt.Println(string(b))
	return nil
}

func printDescribeTopics(client *kafka.Client, topics []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), vars.Timeout)
	defer cancel()

	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return fmt.Errorf("problem retrieving metadata: %w", err)
	}

	if leaderDist {
		for _, t := range resp.Topics {
			fmt.Printf("Topic: %s\n", t.Name)

			dist := make(map[int]int)
			for _, p := range t.Partitions {
				dist[p.Replicas[0].ID]++
			}
			printDist(dist)
		}
	}

	if replicaDist {
		for _, t := range resp.Topics {
			fmt.Printf("Topic: %s\n", t.Name)

			dist := make(map[int]int)
			for _, p := range t.Partitions {
				for _, b := range p.Replicas {
					dist[b.ID]++
				}
			}
			printDist(dist)
		}
	}

	if !leaderDist && !replicaDist {
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				fmt.Printf(
					"%s-%d; leader: %v, replicas: %v, isr: %v\n",
					p.Topic, p.ID, p.Leader.ID, toBrokersString(p.Replicas), toBrokersString(p.Isr),
				)
			}
		}
	}

	return nil
}

func toBrokersString(brokers []kafka.Broker) string {
	out := make([]string, len(brokers))
	for i, b := range brokers {
		out[i] = strconv.Itoa(b.ID)
	}
	return strings.Join(out, ",")
}

func printDist(dist map[int]int) {
	keys := make([]int, len(dist))
	maxV := -1

	{
		i := 0
		for k, v := range dist {
			keys[i] = k
			i++
			if v > maxV {
				maxV = v
			}
		}
	}

	slices.Sort(keys)

	bar := func(v int) string {
		out := ""
		for i := 0; i < maxV; i++ {
			if i < v {
				out += "="
			} else {
				out += " "
			}
		}
		return out
	}

	for _, k := range keys {
		v := dist[k]
		fmt.Printf("%d: [%s] %d\n", k, bar(v), v)
	}
}
