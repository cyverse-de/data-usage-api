package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "db"})

var psql squirrel.StatementBuilderType = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

type UserDataUsage struct {
	ID           string    `db:"id" json:"id"`
	UserID       string    `db:"user_id" json:"user_id"`
	Username     string    `db:"username" json:"username"`
	Total        int64     `db:"total" json:"total"`
	Time         time.Time `db:"time" json:"time"`
	LastModified time.Time `db:"last_modified" json:"last_modified"`
}

type DatabaseAccessor interface {
	QueryRowxContext(context.Context, string, ...interface{}) *sqlx.Row
	QueryxContext(context.Context, string, ...interface{}) (*sqlx.Rows, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

type DEDatabase struct {
	db     DatabaseAccessor
	schema string
}

func NewDE(db DatabaseAccessor, schema string) *DEDatabase {
	return &DEDatabase{db: db, schema: schema}
}

func (d *DEDatabase) Table(name string) string {
	return fmt.Sprintf("%s.%s", d.schema, name)
}

func (d *DEDatabase) Username(context context.Context, userID string) (string, error) {
	var username string

	sql, args, err := psql.Select("username").From(d.Table("users")).Where("id = ?", userID).ToSql()
	if err != nil {
		return "", err
	}

	err = d.db.QueryRowxContext(context, sql, args...).Scan(&username)
	if err != nil {
		return "", err
	}

	return username, nil
}

func (d *DEDatabase) UserID(context context.Context, username string) (string, error) {
	var userID string

	sql, args, err := psql.Select("id").From(d.Table("users")).Where("username = ?", username).ToSql()
	if err != nil {
		return "", err
	}

	err = d.db.QueryRowxContext(context, sql, args...).Scan(&userID)
	if err != nil {
		return "", err
	}

	return userID, nil
}
