package util

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cyverse-de/data-usage-api/config"
)

func FixUsername(username string, configuration *config.Config) string {
	re, _ := regexp.Compile(`@.*$`)
	return fmt.Sprintf("%s@%s", re.ReplaceAllString(username, ""), strings.Trim(configuration.UserSuffix, "@"))
}
