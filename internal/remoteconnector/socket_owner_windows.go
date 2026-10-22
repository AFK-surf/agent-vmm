package remoteconnector

import (
	"fmt"
	"os"
)

func checkSocketOwner(_ os.FileInfo, label string) error {
	return fmt.Errorf("%s requires a Unix host", label)
}
