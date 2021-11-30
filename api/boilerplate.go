package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (a *App) GreetingHandler(context echo.Context) error {
	return context.String(http.StatusOK, "Hello from data-usage-api.")
}
