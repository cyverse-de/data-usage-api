package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/pkg/errors"
)

type ICATDatabase struct {
	db                DatabaseAccessor
	userSuffix        string
	zone              string
	rootResourceNames []string
}

func NewICAT(db DatabaseAccessor, userSuffix, zone string, rootResourceNames []string) *ICATDatabase {
	return &ICATDatabase{db: db, userSuffix: userSuffix, zone: zone, rootResourceNames: rootResourceNames}
}

func (i *ICATDatabase) UnqualifiedUsername(username string) string {
	return strings.TrimSuffix(username, "@"+i.userSuffix)
}

func (i *ICATDatabase) createStorageRootMapping(context context.Context) error {
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

	_, err := i.db.ExecContext(context, q)
	if err != nil {
		return errors.Wrap(err, "Error creating storage_root_mapping temporary table")
	}

	q2 := "CREATE INDEX idx_root_storage_mapping_root ON storage_root_mapping(root_name)"

	_, err = i.db.ExecContext(context, q2)
	if err != nil {
		return errors.Wrap(err, "Error creating storage_root_mapping temporary table")
	}

	return nil
}

func (i *ICATDatabase) createUserCollsTable(context context.Context) error {
	q := `
CREATE TEMPORARY TABLE user_colls (user_name text, coll_id bigint) ON COMMIT DROP`
	_, err := i.db.ExecContext(context, q)
	if err != nil {
		return errors.Wrap(err, "Error creating empty user_colls table")
	}

	return nil
}

func (i *ICATDatabase) createSpecificUserColls(context context.Context, username string) (string, error) {
	u := i.UnqualifiedUsername(username)
	err := i.createUserCollsTable(context)
	if err != nil {
		return "", err
	}

	q := `INSERT INTO user_colls (user_name, coll_id)
SELECT CASE WHEN coll_name LIKE '/' || $1 || '/home/%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/home/([^/]+).*', E'\\1')
            WHEN coll_name LIKE '/' || $1 || '/trash/home/%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/([^/]+).*', E'\\1')
	    WHEN coll_name LIKE '/' || $1 || '/trash/home/de-irods/%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/de-irods/([^/]+).*', E'\\1')
	    WHEN coll_name LIKE '/' || $1 || '/trash/home/ipcservices/%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/ipcservices/([^/]+).*', E'\\1')
       END, coll_id
    FROM r_coll_main
   WHERE coll_name LIKE '/' || $1 || '/home/' || $2 || '/%'
      OR coll_name =    '/' || $1 || '/home/' || $2
      OR coll_name LIKE '/' || $1 || '/trash/home/' || $2 || '/%'
      OR coll_name =    '/' || $1 || '/trash/home/' || $2
      OR coll_name LIKE '/' || $1 || '/trash/home/de-irods/' || $2 || '/%'
      OR coll_name =    '/' || $1 || '/trash/home/de-irods/' || $2
      OR coll_name LIKE '/' || $1 || '/trash/home/ipcservices/' || $2 || '/%'
      OR coll_name =    '/' || $1 || '/trash/home/ipcservices/' || $2
`

	log.Tracef("createSpecificUserColls SQL: %s, [%s %s]", q, i.zone, u)

	_, err = i.db.ExecContext(context, q, i.zone, u)
	if err != nil {
		return "", errors.Wrap(err, "Error filling user_colls table for user")
	}

	return "user_colls", nil
}

func (i *ICATDatabase) resourcesSubselect() (string, []interface{}, error) {
	// use plain squirrel here to retain ?-style args for embedding in the next query
	return squirrel.Select("storage_id").
		From("storage_root_mapping").
		Where(squirrel.Eq{"root_name": i.rootResourceNames}).
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
	u := i.UnqualifiedUsername(username)
	// We should have a Tx here, or this will behave badly. Not sure how to ensure that/if it's possible to.

	err := i.createStorageRootMapping(context)
	if err != nil {
		return 0, err
	}

	resourceQuery, resourceArgs, err := i.resourcesSubselect()
	if err != nil {
		return 0, err
	}

	userCollsTable, err := i.createSpecificUserColls(context, u)
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
	err = i.db.GetContext(context, &usage, querys, args...)
	if err == sql.ErrNoRows {
		return 0, err
	} else if err != nil {
		return 0, errors.Wrap(err, "Error running query")
	}

	return usage, nil
}

func (i *ICATDatabase) BatchCurrentDataUsage(context context.Context, start, end string) (map[string]int64, error) {
	// Again, this should be a Tx
	rv := make(map[string]int64)

	err := i.createStorageRootMapping(context)
	if err != nil {
		return rv, err
	}

	//resourceQuery, resourceArgs, err := i.resourcesSubselect()
	if err != nil {
		return rv, err
	}

	return rv, nil
}
