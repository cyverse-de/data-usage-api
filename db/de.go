package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/cyverse-de/data-usage-api/config"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
)

type UserDataUsage struct {
	ID           string    `db:"id" json:"id"`
	UserID       string    `db:"user_id" json:"user_id"`
	Username     string    `db:"username" json:"username"`
	Total        int64     `db:"total" json:"total"`
	Time         time.Time `db:"time" json:"time"`
	LastModified time.Time `db:"last_modified" json:"last_modified"`
}

type UserInfo struct {
	ID       string `db:"id" json:"id"`
	Username string `db:"username" json:"username"`
}

type DEDatabase struct {
	db            DatabaseAccessor
	configuration *config.Config
}

func NewDE(db DatabaseAccessor, config *config.Config) *DEDatabase {
	return &DEDatabase{db: db, configuration: config}
}

func (d *DEDatabase) Table(name, alias string) string {
	return fmt.Sprintf("%s.%s AS %s", d.configuration.DBSchema, name, alias)
}

func (d *DEDatabase) baseUserUsageSelect() squirrel.SelectBuilder {
	return psql.Select("d.id", "d.total", "d.user_id", "u.username", "d.time AT TIME ZONE (select current_setting('TIMEZONE')) AS time", "d.last_modified AT TIME ZONE (select current_setting('TIMEZONE')) AS last_modified").
		From(d.Table("user_data_usage", "d")).
		Join(fmt.Sprintf("%s ON (d.user_id = u.id)", d.Table("users", "u")))
}

func (d *DEDatabase) doUserUsage(context context.Context, query squirrel.Sqlizer) (*UserDataUsage, error) {
	var usage UserDataUsage

	querys, args, err := query.ToSql()

	if err != nil {
		return nil, errors.Wrap(err, "Error formatting SQL query")
	}

	log.Tracef("doUserUsage SQL: %s, %+v", querys, args)

	err = d.db.GetContext(context, &usage, querys, args...)
	if err == sql.ErrNoRows {
		return nil, err
	} else if err != nil {
		return nil, errors.Wrap(err, "Error running query")
	}

	return &usage, err
}

func (d *DEDatabase) UserCurrentDataUsage(context context.Context, username string) (*UserDataUsage, error) {
	log.Tracef("Getting data usage for %s", username)
	ctx, span := otel.Tracer(otelName).Start(context, "UserCurrentDataUsage")
	defer span.End()

	query := d.baseUserUsageSelect().
		Where("u.username = ?", username).
		OrderBy("d.time DESC").
		Limit(1)

	return d.doUserUsage(ctx, query)
}

func (d *DEDatabase) EnsureUsers(context context.Context, users []string) error {
	log.Tracef("Ensuring users %+v", users)
	ctx, span := otel.Tracer(otelName).Start(context, "EnsureUsers")
	defer span.End()

	// Users passed here should already have the user suffix
	for _, user := range users {
		if !strings.Contains(user, "@") {
			return errors.New("Usernames passed to EnsureUsers should already be domain-qualified")
		}
	}

	query := psql.Insert(d.Table("users", "u")).
		Columns("username").
		Suffix("ON CONFLICT (username) DO NOTHING")
	for _, user := range users {
		query = query.Values(user)
	}

	qs, args, err := query.ToSql()
	if err != nil {
		return errors.Wrap(err, "Error formatting user insert SQL")
	}

	log.Tracef("EnsureUsers SQL: %s, %+v", qs, args)

	_, err = d.db.ExecContext(ctx, qs, args...)
	if err != nil {
		return errors.Wrap(err, "Error inserting users")
	}
	return nil
}

func (d *DEDatabase) GetUserInfo(context context.Context, username string) (*UserInfo, error) {
	log.Tracef("looking up user info for %s", username)

	ctx, span := otel.Tracer(otelName).Start(context, "GetUserInfo")
	defer span.End()

	query, args, err := psql.
		Select("id", "username").
		From(d.Table("users", "u")).
		Where("username = ?", username).
		ToSql()
	if err != nil {
		return nil, err
	}

	var uis []UserInfo
	err = d.db.SelectContext(ctx, &uis, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "error getting user info")
	}

	if len(uis) < 1 {
		return nil, errors.New("No rows returned for user")
	}

	retval := uis[0]
	return &retval, nil
}
