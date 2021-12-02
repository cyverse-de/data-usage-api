package api

import (
	"net/http"

	"github.com/cyverse-de/data-usage-api/db"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

func (a *App) UpdateUserCurrentUsageHandler(c echo.Context) error {
	context := c.Request().Context()

	user := c.Param("username")
	if user == "" {
		return logging.ErrorResponse{Message: "No username provided", ErrorCode: "400", HTTPStatusCode: http.StatusBadRequest}
	}
	user = a.FixUsername(user)

	dbs, rb, commit, err := db.NewBothTx(context, a.dedb, a.schema, a.icat, a.userSuffix, a.zone, a.rootResourceNames)
	if err != nil {
		e := errors.Wrap(err, "Failed setting up database")
		log.Error(e)
		return logging.ErrorResponse{Message: e.Error(), ErrorCode: "500", HTTPStatusCode: http.StatusInternalServerError}
	}
	defer rb()

	res, err := dbs.UpdateUserDataUsage(context, user)
	if err != nil {
		e := errors.Wrap(err, "Failed updating usage information")
		log.Error(e)
		return logging.ErrorResponse{Message: e.Error(), ErrorCode: "500", HTTPStatusCode: http.StatusInternalServerError}

	}
	err = commit()
	if err != nil {
		e := errors.Wrap(err, "Failed updating usage information")
		log.Error(e)
		return logging.ErrorResponse{Message: e.Error(), ErrorCode: "500", HTTPStatusCode: http.StatusInternalServerError}
	}

	return c.JSON(http.StatusOK, res)
}
