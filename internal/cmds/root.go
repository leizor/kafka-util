package cmds

import (
	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/internal/cmds/count-urps"
	"github.com/leizor/kafka-util/internal/cmds/reassign-partition"
	"github.com/leizor/kafka-util/internal/cmds/vars"
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
	RootCmd.AddCommand(count_urps.CountURPsCommand)
}
