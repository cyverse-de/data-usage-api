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

	// Get user info from the DE database. Used below to fill out some fields
	// in the response.
	db := db.NewDE(a.dedb, a.configuration)
	userInfo, err := db.GetUserInfo(context, user)
	if err != nil {
		return logging.ErrorResponse{Message: err.Error(), ErrorCode: "400", HTTPStatusCode: http.StatusBadRequest}
	}

	// Get the current usage as recorded in QMS.
	res, err := a.nc.UserCurrentDataUsage(context, a.configuration, user)

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

	// QMS response contains user info from QMS, which does not necessarily
	// match the user info in the DE. Callers are expecting DE user info here,
	// not QMS user info.
	res.UserID = userInfo.ID
	res.Username = userInfo.Username

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

func (a *App) UserDataOverageHandler(c echo.Context) error {
	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return logging.ErrorResponse{Message: "No username provided", ErrorCode: "400", HTTPStatusCode: http.StatusBadRequest}
	}
	user = util.FixUsername(user, a.configuration)

	overages, err := a.nc.AllResourceOveragesForUser(context, a.configuration, user)
	if err != nil {
		e := errors.Wrap(err, "failed getting all resource overages")
		log.Error(e)
		return logging.ErrorResponse{Message: e.Error(), ErrorCode: "500", HTTPStatusCode: http.StatusInternalServerError}
	}

	hasDataOverage := false

	for _, overage := range overages.Overages {
		if overage.ResourceName == "data.size" {
			hasDataOverage = true
		}
	}

	return c.JSON(http.StatusOK, map[string]bool{"has_data_overage": hasDataOverage})
}
