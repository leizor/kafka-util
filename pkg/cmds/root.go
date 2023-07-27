package cmds

import (
	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/pkg/cmds/reassign-partition"
	"github.com/leizor/kafka-util/pkg/cmds/vars"
)

const version = "0.01"

var RootCmd = &cobra.Command{Use: "kafka-util", Version: version}

func init() {
	RootCmd.PersistentFlags().StringVar(&vars.BootstrapServer, "bootstrap-server", "", "The Kafka server to connect to")
	err := RootCmd.MarkPersistentFlagRequired("bootstrap-server")
	if err != nil {
		panic(err)
	}

	RootCmd.AddCommand(reassign_partition.ReassignPartitionCmd)
}
