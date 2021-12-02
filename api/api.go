package api

import (
	"fmt"
	"strings"

	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

// nolint - for now
var log = logging.Log.WithFields(logrus.Fields{"package": "api"})

type App struct {
	dedb              *sqlx.DB
	schema            string
	icat              *sqlx.DB
	router            *echo.Echo
	userSuffix        string
	zone              string
	rootResourceNames []string
}

func New(dedb *sqlx.DB, schema string, icat *sqlx.DB, userSuffix, zone string, rootResourceNames []string) *App {
	return &App{
		dedb:              dedb,
		schema:            schema,
		icat:              icat,
		router:            echo.New(),
		userSuffix:        userSuffix,
		zone:              zone,
		rootResourceNames: rootResourceNames,
	}
}

func (a *App) FixUsername(username string) string {
	if !strings.HasSuffix(username, a.userSuffix) {
		return fmt.Sprintf("%s@%s", username, a.userSuffix)
	}
	return username
}

func (a *App) Router() *echo.Echo {
	a.router.HTTPErrorHandler = logging.HTTPErrorHandler
	a.router.GET("/", a.GreetingHandler).Name = "greeting"

	userdata := a.router.Group("/:username/data")
	userdata.GET("/current", a.UserCurrentUsageHandler)
	userdata.POST("/update", a.UpdateUserCurrentUsageHandler)

	return a.router
}
