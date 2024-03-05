package errorcheck

import (
	"errors"
	"fmt"
	"strings"

	"github.com/segmentio/kafka-go"
)

// CheckRespErrors inventories any errors in a *kafka.AlterPartitionReassignmentsResponse and returns them
// in a single consolidated error.
func CheckRespErrors(resp *kafka.AlterPartitionReassignmentsResponse) error {
	var errs []string

	if resp.Error != nil {
		errs = append(errs, resp.Error.Error())
	}
	for _, res := range resp.PartitionResults {
		if res.Error != nil && resp.Error != nil && resp.Error.Error() != res.Error.Error() {
			errs = append(errs, fmt.Sprintf("[partition %d] %s", res.PartitionID, res.Error))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, ", "))
	} else {
		return nil
	}
}
