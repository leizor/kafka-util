package main

import (
	"os"

	"github.com/leizor/kafka-util/internal/cmds"
)

func main() {
	err := cmds.RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
