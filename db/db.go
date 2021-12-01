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

func (d *DEDatabase) UserCurrentDataUsage(context context.Context, username string) (*UserDataUsage, error) {
	var usage UserDataUsage

	log.Tracef("Getting data usage for %s", username)

	sql, args, err := psql.Select("d.id", "d.total", "d.user_id", "u.username", "d.time AT TIME ZONE (select current_setting('TIMEZONE')) AS time", "d.last_modified AT TIME ZONE (select current_setting('TIMEZONE')) AS last_modified").
		From(fmt.Sprintf("%s d", d.Table("user_data_usage"))).
		Join(fmt.Sprintf("%s u ON d.user_id = u.id", d.Table("users"))).
		Where("u.username = ?", username).
		OrderBy("d.time DESC").
		Limit(1).
		ToSql()
	if err != nil {
		return nil, err
	}

	err = d.db.QueryRowxContext(context, sql, args...).StructScan(&usage)
	if err != nil {
		return nil, err
	}

	return &usage, err
}
