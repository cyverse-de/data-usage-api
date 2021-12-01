package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
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
	GetContext(context.Context, interface{}, string, ...interface{}) error
}

type DEDatabase struct {
	db     DatabaseAccessor
	schema string
}

func NewDE(db DatabaseAccessor, schema string) *DEDatabase {
	return &DEDatabase{db: db, schema: schema}
}

func (d *DEDatabase) Table(name, alias string) string {
	return fmt.Sprintf("%s.%s AS %s", d.schema, name, alias)
}

func (d *DEDatabase) baseUserUsageSelect() squirrel.SelectBuilder {
	return psql.Select("d.id", "d.total", "d.user_id", "u.username", "d.time AT TIME ZONE (select current_setting('TIMEZONE')) AS time", "d.last_modified AT TIME ZONE (select current_setting('TIMEZONE')) AS last_modified").
		From(d.Table("user_data_usage", "d")).
		Join(fmt.Sprintf("%s ON (d.user_id = u.id)", d.Table("users", "u")))
}

func (d *DEDatabase) doUserUsage(context context.Context, query squirrel.Sqlizer) (*UserDataUsage, error) {
	var usage UserDataUsage

	sql, args, err := query.ToSql()

	if err != nil {
		return nil, errors.Wrap(err, "Error formatting SQL query")
	}

	log.Tracef("doUserUsage SQL: %s, %+v", sql, args)

	err = d.db.GetContext(context, &usage, sql, args...)
	if err != nil {
		return nil, errors.Wrap(err, "Error running query")
	}

	return &usage, err
}

func (d *DEDatabase) UserCurrentDataUsage(context context.Context, username string) (*UserDataUsage, error) {
	log.Tracef("Getting data usage for %s", username)

	query := d.baseUserUsageSelect().
		Where("u.username = ?", username).
		OrderBy("d.time DESC").
		Limit(1)

	return d.doUserUsage(context, query)
}

func (d *DEDatabase) AddUserDataUsage(context context.Context, username string, total int64, time time.Time) (*UserDataUsage, error) {
	log.Tracef("Inserting for %s: %d at %s", username, total, time)

	query := psql.Insert(d.Table("user_data_usage", "d")).
		Columns("total", "time", "user_id").
		Select(psql.Select().
			Column("? AS total", total).
			Column("? AT TIME ZONE (SELECT current_setting('TIMEZONE')) AS time", time).
			Column("u.id").
			From(d.Table("users", "u")).
			Where("username = ?", username),
		).
		Suffix("RETURNING d.id, d.total, d.user_id, (SELECT username from users WHERE id = d.user_id) as username, d.time AT TIME ZONE (SELECT current_setting('TIMEZONE')) AS time, d.last_modified AT TIME ZONE (SELECT current_setting('TIMEZONE')) AS last_modified")

	return d.doUserUsage(context, query)
}
