package api

import (
	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/cyverse-de/messaging"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
)

// nolint - for now
var log = logging.Log.WithFields(logrus.Fields{"package": "api"})

type App struct {
	dedb          *sqlx.DB
	icat          *sqlx.DB
	router        *echo.Echo
	amqp          *messaging.Client
	configuration *config.Config
}

func New(dedb *sqlx.DB, icat *sqlx.DB, amqp *messaging.Client, configuration *config.Config) *App {
	return &App{
		dedb:          dedb,
		icat:          icat,
		router:        echo.New(),
		amqp:          amqp,
		configuration: configuration,
	}
}

func (a *App) Router() *echo.Echo {
	a.router.HTTPErrorHandler = logging.HTTPErrorHandler
	a.router.GET("/", a.GreetingHandler).Name = "greeting"

	userdata := a.router.Group("/:username/data")
	userdata.GET("/current", a.UserCurrentUsageHandler)
	userdata.POST("/update", a.UpdateUserCurrentUsageHandler)

	return a.router
}
