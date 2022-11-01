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

func (d *DEDatabase) AddUserDataUsage(context context.Context, username string, total int64, time time.Time) (*UserDataUsage, error) {
	log.Tracef("Inserting for %s: %d at %s", username, total, time)
	ctx, span := otel.Tracer(otelName).Start(context, "AddUserDataUsage")
	defer span.End()

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

	return d.doUserUsage(ctx, query)
}

func (d *DEDatabase) AddUserDataUsageBatch(context context.Context, start, end string, usages map[string]int64, time time.Time) ([]*UserDataUsage, error) {
	log.Tracef("Inserting usages: %+v at %s", usages, time)
	ctx, span := otel.Tracer(otelName).Start(context, "AddUserDataUsageBatch")
	defer span.End()

	var placeholders []string
	var startargs []interface{}
	for usr, usg := range usages {
		placeholders = append(placeholders, "(?::text, ?::bigint)")
		startargs = append(startargs, usr, usg)
	}
	nonzero_usages := "new_nonzero_usages (username, usage) AS (VALUES " + strings.Join(placeholders, ",") + ")"

	// Add a 0 for any user whose most recent total is > 0 but who doesn't appear in the batch we got from the ICAT
	new_usages2, nuargs, err := psql.Select().
		Column("us.username").
		Column("?", 0).
		From(d.Table("user_data_usage", "udu")).
		Join(fmt.Sprintf("%s ON (us.id = udu.user_id)", d.Table("users", "us"))).
		LeftJoin("new_nonzero_usages ON users.username = new_nonzero_usages.username").
		Where(squirrel.Gt{"total": 0}).
		Where("us.username BETWEEN ? AND ?", start, end).
		Where("time = (SELECT MAX(time) FROM user_data_usage u2 WHERE u2.user_id = udu.user_id)").
		ToSql()
	startargs = append(startargs, nuargs...)
	new_usages := "new_usages (username, usage) AS (SELECT username, usage from new_nonzero_usages UNION ALL " + new_usages2 + ")"

	startcte := "WITH " + nonzero_usages + ", " + new_usages

	querys, args, err := psql.Insert(d.Table("user_data_usage", "d")).
		Prefix(startcte, startargs...).
		Columns("total", "time", "user_id").
		Select(squirrel.Select().
			Column("new_usages.usage AS total").
			Column("? AT TIME ZONE (SELECT current_setting('TIMEZONE')) AS time", time).
			Column("u.id").
			From(d.Table("users", "u")).
			Join("new_usages ON (new_usages.username = u.username)"),
		).
		Suffix("RETURNING d.id, d.total, d.user_id, (SELECT username from users WHERE id = d.user_id) as username, d.time AT TIME ZONE (SELECT current_setting('TIMEZONE')) AS time, d.last_modified AT TIME ZONE (SELECT current_setting('TIMEZONE')) AS last_modified").
		ToSql()

	if err != nil {
		return nil, errors.Wrap(err, "Error formatting SQL query")
	}

	log.Tracef("AddUserDataUsageBatch SQL: %s, %+v", querys, args)

	var rv []*UserDataUsage

	err = d.db.SelectContext(ctx, &rv, querys, args...)
	if err == sql.ErrNoRows {
		return nil, err
	} else if err != nil {
		return nil, errors.Wrap(err, "Error running query")
	}

	return rv, nil
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
