package api

import (
	"database/sql"
	"net/http"

	"github.com/cyverse-de/data-usage-api/db"
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
		return echo.NewHTTPError(http.StatusNotFound, errors.New("No data usage information found for user"))
	} else if err != nil {
		e := errors.Wrap(err, "Failed fetching current usage")
		log.Error(e)
		return echo.NewHTTPError(http.StatusInternalServerError, e)
	}

	return c.JSON(http.StatusOK, res)
}
