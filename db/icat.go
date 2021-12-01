package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type ICATDatabase struct {
	db         DatabaseAccessor
	userSuffix string
	zone       string
}

func rollbackTxLogError(tx *sqlx.Tx) {
	err := tx.Rollback()
	if err != nil && err.Error() != "sql: transaction has already been committed or rolled back" {
		log.Error(errors.Wrap(err, "Error rolling back transaction"))
	}
}

func NewICAT(db DatabaseAccessor, userSuffix, zone string) *ICATDatabase {
	return &ICATDatabase{db: db, userSuffix: userSuffix, zone: zone}
}

func (i *ICATDatabase) UnqualifiedUsername(username string) string {
	return strings.TrimSuffix(username, i.userSuffix)
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

func (i *ICATDatabase) createSpecificUserColls(context context.Context, username string) error {
	u := i.UnqualifiedUsername(username)
	q := `
CREATE TEMPORARY TABLE user_colls (user_name, coll_id) ON COMMIT DROP AS
SELECT CASE WHEN coll_name LIKE '/' || $1 || '/home/%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/home/([^/]+).*', E'\\1')
            WHEN coll_name LIKE '/' || $1 || '/trash/home/%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/([^/]+).*', E'\\1')
	    WHEN coll_name LIKE '/' || $1 || '/trash/home/de-irods/%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/de-irods/([^/]+).*', E'\\1')
	    WHEN coll_name LIKE '/' || $1 || '/trash/home/ipcservices/%' THEN REGEXP_REPLACE(coll_name, '/' || $1 || '/trash/home/ipcservices/([^/]+).*', E'\\1')
       END, coll_id, coll_name
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

	_, err := i.db.ExecContext(context, q, i.zone, u)
	if err != nil {
		return errors.Wrap(err, "Error creating user_colls table for user")
	}

	return nil
}

func (i *ICATDatabase) UserCurrentDataUsage(context context.Context, username string, rootResourceNames []string) (int64, error) {
	// We should have a Tx here, or this will behave badly. Not sure how to ensure that/if it's possible to.

	err := i.createStorageRootMapping(context)
	if err != nil {
		return 0, err
	}

	err = i.createSpecificUserColls(context, username)
	if err != nil {
		return 0, err
	}

	// use plain squirrel here to retain ?-style args for embedding in the next query
	resourceQuery, resourceArgs, err := squirrel.Select("storage_id").
		From("storage_root_mapping").
		//Where(squirrel.Eq{"root_name": []string{"CyVerseRes", "taccCorralRes", "taccRes"}}).
		Where(squirrel.Eq{"root_name": rootResourceNames}).
		ToSql()

	if err != nil {
		return 0, err
	}

	sql, args, err := psql.Select().
		//Column("u.user_name").
		Column("SUM(d.data_size) AS file_volume").
		From("r_user_main AS u").
		Join("user_colls AS c ON c.user_name = u.user_name").
		Join("r_data_main AS d ON d.coll_id = c.coll_id").
		Where(squirrel.Eq{"u.user_type_name": "rodsuser"}).
		Where(fmt.Sprintf("d.resc_id = ANY(ARRAY(%s))", resourceQuery), resourceArgs...).
		Where(squirrel.Eq{"u.user_name": username}).
		GroupBy("u.user_name").
		Limit(1).
		ToSql()

	if err != nil {
		return 0, errors.Wrap(err, "Error formatting user data usage query")
	}

	log.Tracef("UserCurrentDataUsage SQL: %s, %+v", sql, args)

	var usage int64
	err = i.db.GetContext(context, &usage, sql, args...)
	if err != nil {
		return 0, errors.Wrap(err, "Error running query")
	}

	return usage, nil
}

//func UserCurrentDataUsageDB(db *sqlx.DB, context context.Context, username string) (int64, error) {
//	tx, err := db.BeginTxx(context, nil)
//	if err != nil {
//		return 0, errors.Wrap(err, "Error starting transaction")
//	}
//	defer rollbackTxLogError(tx)
//
//	i := NewICAT(tx)
//	return i.UserCurrentDataUsage(context, username)
//}
