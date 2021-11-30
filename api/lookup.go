package api

import (
	"errors"
	"net/http"

	"github.com/cyverse-de/data-usage-api/db"
	"github.com/labstack/echo/v4"
)

func (a *App) UserCurrentUsageHandler(c echo.Context) error {
	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("No username provided"))
	}
	user = a.FixUsername(user)

	log.Debugf("username: %s", user)

	dedb := db.NewDE(a.dedb, a.schema)

	userid, err := dedb.UserID(c.Request().Context(), user)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("No user fuond for username"))
	}

	return c.JSON(http.StatusOK, map[string]interface{}{"username": user, "userid": userid})
}
