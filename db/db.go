package db

import (
	"context"
	"database/sql"

	"github.com/Masterminds/squirrel"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "db"})

const otelName = "github.com/cyverse-de/data-usage-api/db"

var psql squirrel.StatementBuilderType = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

type DatabaseAccessor interface {
	QueryRowxContext(context.Context, string, ...interface{}) *sqlx.Row
	QueryxContext(context.Context, string, ...interface{}) (*sqlx.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	GetContext(context.Context, interface{}, string, ...interface{}) error
	SelectContext(context.Context, interface{}, string, ...interface{}) error
}

type DatabaseTxAccessor interface {
	DatabaseAccessor
	BeginTxx(context.Context, *sql.TxOptions) (*sqlx.Tx, error)
	Stats() sql.DBStats
}

func logStats(name string, db DatabaseTxAccessor) {
	stats := db.Stats()

	log.Infof("%s stats: %d/%d open; %d/%d used/idle", name, stats.OpenConnections, stats.MaxOpenConnections, stats.InUse, stats.Idle)
}
