package api

import (
	"database/sql"
	"net/http"

	"github.com/cyverse-de/data-usage-api/db"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

func (a *App) UserCurrentUsageHandler(c echo.Context) error {
	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("No username provided"))
	}
	user = a.FixUsername(user)

	log.Debugf("username: %s", user)

	dedb := db.NewDE(a.dedb, a.schema)

	res, err := dedb.UserCurrentDataUsage(context, user)

	if err == sql.ErrNoRows {
		return logging.ErrorResponse{Message: "No data usage information found for user", ErrorCode: "404", HTTPStatusCode: http.StatusNotFound}
	} else if err != nil {
		e := errors.Wrap(err, "Failed fetching current usage")
		log.Error(e)
		return logging.ErrorResponse{Message: e.Error(), ErrorCode: "500", HTTPStatusCode: http.StatusInternalServerError}
	}

	return c.JSON(http.StatusOK, res)
}
