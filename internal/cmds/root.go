package cmds

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/leizor/kafka-util/internal/cmds/ongoing"
	"github.com/leizor/kafka-util/internal/cmds/reassign"
	"github.com/leizor/kafka-util/internal/cmds/vars"
)

const (
	version               = "0.01"
	bootstrapServerEnvVar = "KAFKA_BOOTSTRAP_SERVER"
)

var RootCmd = &cobra.Command{
	Use:     "kafka-util",
	Version: version,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if vars.BootstrapServer != "" {
			// vars.BootstrapServer already set via flag.
			return nil
		}

		var ok bool
		if vars.BootstrapServer, ok = os.LookupEnv(bootstrapServerEnvVar); ok {
			// vars.BootstrapServer set via env var.
			return nil
		}

		return fmt.Errorf("missing bootstrap server; must be provided via the '--bootstrap-server' flag or the '%s' environment variable", bootstrapServerEnvVar)
	},
}

func init() {
	RootCmd.PersistentFlags().StringVar(&vars.BootstrapServer, "bootstrap-server", "", fmt.Sprintf("kafka bootstrap server, may also be set via the '%s' env var (required)", bootstrapServerEnvVar))
	RootCmd.PersistentFlags().DurationVar(&vars.Timeout, "timeout", time.Minute, "amount of time to wait for a request to Kafka to complete")

	RootCmd.AddCommand(reassign.Cmd)
	RootCmd.AddCommand(ongoing.Cmd)
}
