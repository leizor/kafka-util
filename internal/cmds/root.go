package cmds

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/internal/cmds/ongoing"
	"github.com/leizor/kafka-util/internal/cmds/reassign"
	"github.com/leizor/kafka-util/internal/cmds/vars"
)

const version = "0.01"

var RootCmd = &cobra.Command{Use: "kafka-util", Version: version}

func init() {
	RootCmd.PersistentFlags().StringVar(&vars.BootstrapServer, "bootstrap-server", "", "The Kafka server to connect to")
	if err := RootCmd.MarkPersistentFlagRequired("bootstrap-server"); err != nil {
		panic(err)
	}

	RootCmd.PersistentFlags().DurationVar(&vars.Timeout, "timeout", time.Minute, "The amount of time to wait for a request to Kafka to complete.")

	RootCmd.AddCommand(reassign.Cmd)
	RootCmd.AddCommand(ongoing.Cmd)
}
