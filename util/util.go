package util

import (
	"fmt"
	"strings"

	"github.com/cyverse-de/data-usage-api/config"
)

func FixUsername(username string, configuration *config.Config) string {
	if !strings.HasSuffix(username, configuration.UserSuffix) {
		return fmt.Sprintf("%s@%s", username, configuration.UserSuffix)
	}
	return username
}
