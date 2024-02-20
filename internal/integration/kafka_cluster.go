package integration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/orlangure/gnomock"
	"github.com/segmentio/kafka-go"
)

const (
	clusterID  = "7IPMyg2iQ0K4oIC1SOpPHw"
	nwName     = "kafka-cluster"
	kafkaImage = "bitnami/kafka:3.6"
)

type KafkaCluster struct {
	networkID string
	brokers   []*gnomock.Container
	client    *kafka.Client
	stop      chan interface{}
}

// StartKafkaCluster starts a Kafka cluster with the prescribed number of
// brokers and returns it.
//
// Note that there can be containers and networks that need to be cleaned up even
// if this returns an error.
func StartKafkaCluster(ctx context.Context, numBrokers int) (*KafkaCluster, error) {
	kc := &KafkaCluster{
		brokers: make([]*gnomock.Container, numBrokers),
		stop:    make(chan interface{}),
	}

	nwID, err := gnomock.StartNetwork(ctx, nwName)
	if err != nil {
		return kc, err
	}

	kc.networkID = nwID
	fmt.Printf("Network '%s' with id '%s' created.\n", nwName, nwID)

	quorumVoters := make([]string, numBrokers)
	for i := 0; i < numBrokers; i++ {
		quorumVoters[i] = fmt.Sprintf("%d@kafka-%d:9093", i, i)
	}

	for i := 0; i < numBrokers; i++ {
		c, err := startBroker(i, nwID, quorumVoters)
		if err != nil {
			return kc, err
		}

		kc.brokers[i] = c
		fmt.Printf("Broker %d started.\n", i)
	}

	kc.client = &kafka.Client{Addr: kafka.TCP(kc.GetBootstrapServer())}

	// Wait for cluster to be ready before returning.
	for {
		resp, err := kc.client.Metadata(ctx, &kafka.MetadataRequest{})
		if err != nil {
			continue
		}
		if len(resp.Brokers) == numBrokers {
			break
		}
		time.Sleep(10 * time.Second)
	}

	return kc, nil
}

func startBroker(nodeID int, nwID string, quorumVoters []string) (*gnomock.Container, error) {
	ports := make(gnomock.NamedPorts)
	ports["plaintext"] = gnomock.TCP(9092)
	ports["controller"] = gnomock.TCP(9093)
	ports["external"] = withHostPort(9094, 9094+nodeID+1)

	return gnomock.StartCustom(kafkaImage, ports,
		gnomock.WithContainerName(fmt.Sprintf("kafka-%d", nodeID)),
		gnomock.WithEnv(fmt.Sprintf("KAFKA_CFG_NODE_ID=%d", nodeID)),
		gnomock.WithEnv("KAFKA_CFG_PROCESS_ROLES=controller,broker"),
		gnomock.WithEnv("KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094"),
		gnomock.WithEnv(fmt.Sprintf("KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://:9092,EXTERNAL://localhost:%d", ports["external"].HostPort)),
		gnomock.WithEnv("KAFKA_CFG_LISTENER_NAMES=PLAINTEXT,EXTERNAL"),
		gnomock.WithEnv("KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER"),
		gnomock.WithEnv("KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT"),
		gnomock.WithEnv("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT"),
		gnomock.WithEnv(fmt.Sprintf("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=%s", strings.Join(quorumVoters, ","))),
		gnomock.WithEnv("KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR=3"),
		gnomock.WithEnv("KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3"),
		gnomock.WithEnv("KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR=2"),
		gnomock.WithEnv(fmt.Sprintf("KAFKA_KRAFT_CLUSTER_ID=%s", clusterID)),
		gnomock.WithNetworkID(nwID),
	)
}

func withHostPort(port, hostPort int) gnomock.Port {
	return gnomock.Port{
		Protocol: "tcp",
		Port:     port,
		HostPort: hostPort,
	}
}

func (kc *KafkaCluster) GetBootstrapServer() string {
	return kc.brokers[0].Address("external")
}

func (kc *KafkaCluster) GetClient() *kafka.Client {
	return kc.client
}

func (kc *KafkaCluster) CreateTopicWithTraffic(topicName string, replicaAssignments [][]int) error {
	resp, err := kc.client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:              topicName,
				NumPartitions:      -1,
				ReplicationFactor:  -1,
				ReplicaAssignments: toReplicaAssignments(replicaAssignments),
			},
		},
	})
	if err != nil {
		return err
	}
	for _, err := range resp.Errors {
		if err != nil {
			return err
		}
	}

	go func() {
		p := &kafka.Writer{
			Addr:     kafka.TCP(kc.GetBootstrapServer()),
			Topic:    topicName,
			Balancer: &kafka.RoundRobin{},
		}
		defer func() { _ = p.Close() }()

		msgNum := 0
		for {
			select {
			case <-kc.stop:
				return
			default:
				val := make([]byte, 128)
				for i := range val {
					val[i] = 0xff
				}

				_ = p.WriteMessages(context.Background(), kafka.Message{
					Key:   []byte("key"),
					Value: []byte(fmt.Sprintf("%s %d", val, msgNum)),
				})
				msgNum++
			}
		}
	}()

	fmt.Printf("Topic '%s' created.\n", topicName)

	return nil
}

func (kc *KafkaCluster) Cleanup(ctx context.Context) error {
	// Stop any producers.
	kc.stop <- struct{}{}

	// Stop the brokers.
	if err := gnomock.Stop(kc.brokers...); err != nil {
		return err
	}

	// Stop the network.
	if err := gnomock.StopNetwork(ctx, kc.networkID); err != nil {
		return err
	}

	return nil
}

func toReplicaAssignments(in [][]int) []kafka.ReplicaAssignment {
	out := make([]kafka.ReplicaAssignment, len(in))

	for i, a := range in {
		out[i] = kafka.ReplicaAssignment{
			Partition: i,
			Replicas:  a,
		}
	}

	return out
}
