package api

import (
	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/cyverse-de/data-usage-api/natsconn"
	"github.com/cyverse-de/messaging/v9"
	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
)

// nolint - for now
var log = logging.Log.WithFields(logrus.Fields{"package": "api"})

type App struct {
	dedb          *sqlx.DB
	icat          *sqlx.DB
	router        *echo.Echo
	amqp          *messaging.Client
	nc            *natsconn.Connector
	configuration *config.Config
}

func New(dedb *sqlx.DB, icat *sqlx.DB, amqp *messaging.Client, nc *natsconn.Connector, configuration *config.Config) *App {
	return &App{
		dedb:          dedb,
		icat:          icat,
		router:        echo.New(),
		amqp:          amqp,
		nc:            nc,
		configuration: configuration,
	}
}

func (a *App) Router() *echo.Echo {
	a.router.Use(otelecho.Middleware("data-usage-api"))

	a.router.HTTPErrorHandler = logging.HTTPErrorHandler
	a.router.GET("/", a.GreetingHandler).Name = "greeting"

	userdata := a.router.Group("/:username/data")
	userdata.GET("/current", a.UserCurrentUsageHandler)
	userdata.POST("/update", a.UpdateUserCurrentUsageHandler)
	userdata.GET("/overage", a.UserDataOverageHandler)

	return a.router
}
