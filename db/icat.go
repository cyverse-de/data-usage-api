package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/util"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
)

type ICATDatabase struct {
	db            DatabaseAccessor
	configuration *config.Config
}

func NewICAT(db DatabaseAccessor, config *config.Config) *ICATDatabase {
	return &ICATDatabase{db: db, configuration: config}
}

func (i *ICATDatabase) FixUsername(username string) string {
	return util.FixUsername(username, &config.Config{UserSuffix: i.configuration.UserSuffix})
}

func (i *ICATDatabase) UnqualifiedUsername(username string) string {
	return strings.TrimSuffix(username, "@"+i.configuration.UserSuffix)
}

func (i *ICATDatabase) createStorageRootMapping(context context.Context) error {
	ctx, span := otel.Tracer(otelName).Start(context, "createStorageRootMapping")
	defer span.End()

	q := `
CREATE TEMPORARY TABLE storage_root_mapping(storage_id, root_name) ON COMMIT DROP AS
WITH RECURSIVE child_mapping AS (
  SELECT
      resc_id AS id,
      (resc_net != 'EMPTY_RESC_HOST') AS storage,
      resc_name AS root
    FROM r_resc_main
    WHERE resc_parent = '' AND resc_name != 'bundleResc'
  UNION SELECT r.resc_id, r.resc_net != 'EMPTY_RESC_HOST', m.root
    FROM r_resc_main AS r JOIN child_mapping AS m ON m.id::TEXT = r.resc_parent )
SELECT id, root FROM child_mapping WHERE storage`

	_, err := i.db.ExecContext(ctx, q)
	if err != nil {
		return errors.Wrap(err, "Error creating storage_root_mapping temporary table")
	}

	q2 := "CREATE INDEX idx_root_storage_mapping_root ON storage_root_mapping(root_name)"

	_, err = i.db.ExecContext(ctx, q2)
	if err != nil {
		return errors.Wrap(err, "Error creating storage_root_mapping temporary table")
	}

	return nil
}

func (i *ICATDatabase) createUserCollsTable(context context.Context) (string, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "createUserCollsTable")
	defer span.End()

	q := `
CREATE TEMPORARY TABLE user_colls (user_name text, coll_id bigint) ON COMMIT DROP`
	_, err := i.db.ExecContext(ctx, q)
	if err != nil {
		return "", errors.Wrap(err, "Error creating empty user_colls table")
	}

	return "user_colls", nil
}

func (i *ICATDatabase) populateSpecificUserColls(context context.Context, username, table string) error {
	ctx, span := otel.Tracer(otelName).Start(context, "populateSpecificUserColls")
	defer span.End()

	// subfolders of /trash/home need to come first or their contents all get assigned to the subfolder name
	q := fmt.Sprintf(`INSERT INTO %s (user_name, coll_id)
SELECT CASE WHEN coll_name LIKE '/' || $1 || '/home/%%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/home/([^/]+).*', E'\\1')
	    WHEN coll_name LIKE '/' || $1 || '/trash/home/de-irods/%%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/de-irods/([^/]+).*', E'\\1')
	    WHEN coll_name LIKE '/' || $1 || '/trash/home/ipcservices/%%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/ipcservices/([^/]+).*', E'\\1')
            WHEN coll_name LIKE '/' || $1 || '/trash/home/%%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/([^/]+).*', E'\\1')
       END, coll_id
    FROM r_coll_main
   WHERE coll_name LIKE '/' || $1 || '/home/' || $2 || '/%%'
      OR coll_name =    '/' || $1 || '/home/' || $2
      OR coll_name LIKE '/' || $1 || '/trash/home/' || $2 || '/%%'
      OR coll_name =    '/' || $1 || '/trash/home/' || $2
      OR coll_name LIKE '/' || $1 || '/trash/home/de-irods/' || $2 || '/%%'
      OR coll_name =    '/' || $1 || '/trash/home/de-irods/' || $2
      OR coll_name LIKE '/' || $1 || '/trash/home/ipcservices/' || $2 || '/%%'
      OR coll_name =    '/' || $1 || '/trash/home/ipcservices/' || $2
`, table)

	log.Tracef("populateSpecificUserColls SQL: %s, [%s %s]", q, i.configuration.Zone, username)

	_, err := i.db.ExecContext(ctx, q, i.configuration.Zone, username)
	if err != nil {
		return errors.Wrap(err, "Error filling user_colls table for user")
	}
	return nil
}

func (i *ICATDatabase) populateBatchUserColls(context context.Context, start, end, table string) error {
	ctx, span := otel.Tracer(otelName).Start(context, "populateBatchUserColls")
	defer span.End()

	q := fmt.Sprintf(`INSERT INTO %s (user_name, coll_id)
SELECT CASE WHEN coll_name LIKE '/' || $1 || '/home/%%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/home/([^/]+).*', E'\\1')
	    WHEN coll_name LIKE '/' || $1 || '/trash/home/de-irods/%%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/de-irods/([^/]+).*', E'\\1')
	    WHEN coll_name LIKE '/' || $1 || '/trash/home/ipcservices/%%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/ipcservices/([^/]+).*', E'\\1')
            WHEN coll_name LIKE '/' || $1 || '/trash/home/%%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/([^/]+).*', E'\\1')
       END, coll_id
    FROM r_coll_main
   WHERE coll_name BETWEEN '/' || $1 || '/home/' || $2 AND '/' || $1 || '/home/' || $3
      OR coll_name LIKE    '/' || $1 || '/home/' || $3 || '/%%'
      OR coll_name BETWEEN '/' || $1 || '/trash/home/' || $2 AND '/' || $1 || '/trash/home/' || $3
      OR coll_name LIKE    '/' || $1 || '/trash/home/' || $3 || '/%%'
      OR coll_name BETWEEN '/' || $1 || '/trash/home/de-irods/' || $2 AND '/' || $1 || '/trash/home/de-irods/' || $3
      OR coll_name LIKE    '/' || $1 || '/trash/home/de-irods/' || $3 || '/%%'
      OR coll_name BETWEEN '/' || $1 || '/trash/home/ipcservices/' || $2 AND '/' || $1 || '/trash/home/ipcservices/' || $3
      OR coll_name LIKE    '/' || $1 || '/trash/home/ipcservices/' || $3 || '/%%'
`, table)

	log.Tracef("populateSpecificUserColls SQL: %s, [%s %s %s]", q, i.configuration.Zone, start, end)

	_, err := i.db.ExecContext(ctx, q, i.configuration.Zone, start, end)
	if err != nil {
		return errors.Wrap(err, "Error filling user_colls table for batch")
	}
	return nil
}

func (i *ICATDatabase) createSpecificUserColls(context context.Context, username string) (string, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "createSpecificUserColls")
	defer span.End()

	u := i.UnqualifiedUsername(username)
	t, err := i.createUserCollsTable(ctx)
	if err != nil {
		return "", err
	}

	err = i.populateSpecificUserColls(ctx, u, t)
	if err != nil {
		return "", err
	}

	return t, nil
}

func (i *ICATDatabase) createBatchUserColls(context context.Context, start, end string) (string, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "createBatchUserColls")
	defer span.End()

	s := i.UnqualifiedUsername(start)
	e := i.UnqualifiedUsername(end)
	t, err := i.createUserCollsTable(ctx)
	if err != nil {
		return "", err
	}

	err = i.populateBatchUserColls(ctx, s, e, t)
	if err != nil {
		return "", err
	}

	return t, nil
}

func (i *ICATDatabase) resourcesSubselect() (string, []interface{}, error) {
	// use plain squirrel here to retain ?-style args for embedding in the next query
	return squirrel.Select("storage_id").
		From("storage_root_mapping").
		Where(squirrel.Eq{"root_name": i.configuration.RootResourceNames}).
		ToSql()
}

func (i *ICATDatabase) baseUsageQuery(userCollsTable, resourceQuery string, resourceArgs []interface{}) squirrel.SelectBuilder {
	return psql.Select().
		Column("SUM(d.data_size) AS file_volume").
		From("r_user_main AS u").
		Join(fmt.Sprintf("%s AS c ON c.user_name = u.user_name", userCollsTable)).
		Join("r_data_main AS d ON d.coll_id = c.coll_id").
		Where(squirrel.Eq{"u.user_type_name": "rodsuser"}).
		Where(fmt.Sprintf("d.resc_id = ANY(ARRAY(%s))", resourceQuery), resourceArgs...).
		GroupBy("u.user_name")
}

func (i *ICATDatabase) UserCurrentDataUsage(context context.Context, username string) (int64, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "UserCurrentDataUsage")
	defer span.End()

	u := i.UnqualifiedUsername(username)
	// We should have a Tx here, or this will behave badly. Not sure how to ensure that/if it's possible to.

	err := i.createStorageRootMapping(ctx)
	if err != nil {
		return 0, err
	}

	resourceQuery, resourceArgs, err := i.resourcesSubselect()
	if err != nil {
		return 0, err
	}

	userCollsTable, err := i.createSpecificUserColls(ctx, u)
	if err != nil {
		return 0, err
	}

	// should this additionally return a timestamp, or even a semi-complete UserDataUsage object?
	querys, args, err := i.baseUsageQuery(userCollsTable, resourceQuery, resourceArgs).
		Where(squirrel.Eq{"u.user_name": u}).
		Limit(1).
		ToSql()

	if err != nil {
		return 0, errors.Wrap(err, "Error formatting user data usage query")
	}

	log.Tracef("UserCurrentDataUsage SQL: %s, %+v", querys, args)

	var usage int64
	err = i.db.GetContext(ctx, &usage, querys, args...)
	if err == sql.ErrNoRows {
		return 0, err
	} else if err != nil {
		return 0, errors.Wrap(err, "Error running query")
	}

	return usage, nil
}

func (i *ICATDatabase) BatchCurrentDataUsage(context context.Context, start, end string) (map[string]int64, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "BatchCurrentDataUsage")
	defer span.End()

	// Again, this should be a Tx
	rv := make(map[string]int64)
	s := i.UnqualifiedUsername(start)
	e := i.UnqualifiedUsername(end)

	err := i.createStorageRootMapping(ctx)
	if err != nil {
		return rv, err
	}

	resourceQuery, resourceArgs, err := i.resourcesSubselect()
	if err != nil {
		return rv, err
	}

	userCollsTable, err := i.createBatchUserColls(ctx, start, end)
	if err != nil {
		return rv, err
	}

	querys, args, err := i.baseUsageQuery(userCollsTable, resourceQuery, resourceArgs).
		Where("u.user_name BETWEEN ? AND ?", s, e).
		Column("u.user_name AS username").
		ToSql()
	if err != nil {
		return rv, errors.Wrap(err, "Error formatting query")
	}

	log.Tracef("BatchCurrentDataUsage SQL: %s, %+v", querys, args)

	rows, err := i.db.QueryxContext(ctx, querys, args...)
	if err != nil {
		return rv, errors.Wrap(err, "Error fetching usage for users")
	}
	defer rows.Close()
	for rows.Next() {
		var uname string
		var total int64
		err = rows.Scan(&total, &uname)
		if err != nil {
			return rv, err
		}
		log.Tracef("Got %d for %s", total, uname)
		rv[uname] = total
	}
	return rv, nil
}

func (i *ICATDatabase) GetUserBatchBounds(context context.Context, batchSize int) ([][]string, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "GetUserBatchBounds")
	defer span.End()

	var bounds [][]string
	prefix := "WITH users AS (SELECT row_number() OVER (ORDER BY user_name) AS n, user_name FROM r_user_main WHERE user_type_name = 'rodsuser')"
	querys, args, err := psql.Select("n", "user_name").
		From("users").
		Where("n % ? = 0 OR (n - 1) % ? = 0", batchSize, batchSize).
		Prefix(prefix).
		Suffix("UNION ALL SELECT max(n), max(user_name) FROM users").
		ToSql()

	if err != nil {
		return bounds, errors.Wrap(err, "Error formatting SQL query")
	}

	rows, err := i.db.QueryxContext(ctx, querys, args...)
	if err != nil {
		return bounds, errors.Wrap(err, "Error fetching user batch bounds")
	}
	defer rows.Close()

	boundsMap := make(map[int]string)
	maxN := 0
	for rows.Next() {
		var rown int
		var username string
		err = rows.Scan(&rown, &username)
		if err != nil {
			return bounds, err
		}
		boundsMap[rown] = username
		maxN = rown
	}
	log.Tracef("%d %+v", maxN, boundsMap)

	for i := 1; i < maxN; i = i + batchSize {
		upperBound, ok := boundsMap[i+batchSize-1]
		if !ok && i+batchSize > maxN {
			upperBound = boundsMap[maxN]
		}
		bounds = append(bounds, []string{boundsMap[i], upperBound})
	}

	return bounds, nil
}
