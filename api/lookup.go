package api

import (
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cyverse-de/data-usage-api/db"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/cyverse-de/data-usage-api/util"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

func (a *App) UserCurrentUsageHandler(c echo.Context) error {
	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return logging.ErrorResponse{Message: "No username provided", ErrorCode: "400", HTTPStatusCode: http.StatusBadRequest}
	}
	user = util.FixUsername(user, a.configuration)

	dedb := db.NewDE(a.dedb, a.configuration)

	res, err := dedb.UserCurrentDataUsage(context, user)

	if err == sql.ErrNoRows {
		log.Tracef("Enqueuing update message for %s", user)
		err = a.amqp.PublishContext(context, fmt.Sprintf("index.usage.data.user.%s", strings.TrimSuffix(user, "@"+a.configuration.UserSuffix)), []byte{})
		if err != nil {
			log.Error(errors.Wrap(err, "Failed enqueuing update message"))
		}
		return logging.ErrorResponse{Message: "No data usage information found for user", ErrorCode: "404", HTTPStatusCode: http.StatusNotFound}
	} else if err != nil {
		e := errors.Wrap(err, "Failed fetching current usage")
		log.Error(e)
		return logging.ErrorResponse{Message: e.Error(), ErrorCode: "500", HTTPStatusCode: http.StatusInternalServerError}
	}

	// if the user's usage information is older than the refresh interval, asynchronously update it
	if res.Time.Add(*a.configuration.RefreshInterval).Before(time.Now()) {
		// enqueue async update
		log.Tracef("Enqueuing update message for %s", user)
		err = a.amqp.PublishContext(context, fmt.Sprintf("index.usage.data.user.%s", strings.TrimSuffix(user, "@"+a.configuration.UserSuffix)), []byte{})
		if err != nil {
			log.Error(errors.Wrap(err, "Failed enqueuing update message"))
		}
	}

	return c.JSON(http.StatusOK, res)
}
