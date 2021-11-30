package api

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"
)

func (a *App) UserCurrentUsageHandler(c echo.Context) error {
	user := c.Param("username")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("No username provided"))
	}
	user = a.FixUsername(user)

	log.Debugf("username: %s", user)

	return c.JSON(http.StatusOK, map[string]string{"username": user})
}
