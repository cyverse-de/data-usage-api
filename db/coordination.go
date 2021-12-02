package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type BothDatabases struct {
	dedb   *DEDatabase
	icatdb *ICATDatabase
}

func NewBoth(dedb *DEDatabase, icatdb *ICATDatabase) *BothDatabases {
	return &BothDatabases{dedb: dedb, icatdb: icatdb}
}

func NewBothTx(context context.Context, deconn *sqlx.DB, deschema string, icatconn *sqlx.DB, userSuffix, zone string, rootResourceNames []string) (*BothDatabases, func(), func() error, error) {
	detx, err := deconn.BeginTxx(context, nil)
	if err != nil {
		return nil, func() {}, func() error { return err }, errors.Wrap(err, "Error creating DE database transaction")
	}

	icattx, err := icatconn.BeginTxx(context, nil)
	if err != nil {
		return nil, func() {}, func() error { return err }, errors.Wrap(err, "Error creating ICAT transaction")
	}

	dedb := NewDE(detx, deschema)
	icatdb := NewICAT(icattx, userSuffix, zone, rootResourceNames)

	rb := func() {
		detx.Rollback()   // nolint:errcheck
		icattx.Rollback() // nolint:errcheck
	}

	commit := func() error {
		err := detx.Commit()
		if err != nil {
			e := errors.Wrap(err, "Error committing DE database transaction")
			log.Error(e)
			icattx.Rollback() // nolint:errcheck
			return e
		}
		err = icattx.Commit()
		if err != nil {
			// yes, this swallows the error. We don't write to the ICAT in this service anyway though
			log.Error(errors.Wrap(err, "Error committing ICAT transaction"))
		}
		return nil
	}
	return NewBoth(dedb, icatdb), rb, commit, nil
}

func (b *BothDatabases) UpdateUserDataUsage(context context.Context, username string) (*UserDataUsage, error) {
	usagenum, err := b.icatdb.UserCurrentDataUsage(context, username)
	if err == sql.ErrNoRows {
		usagenum = 0
		log.Infof("No usage information was found for user %s. Attempting to add a usage of 0 anyway.", username)
	} else if err != nil {
		return nil, errors.Wrap(err, "Error getting current data usage")
	}

	// if this update shouldn't be added, or should amend a prior reading, do it here or in the method called below
	// or maybe have an async cleanup process that deduplicates readings

	return b.dedb.AddUserDataUsage(context, username, usagenum, time.Now())
}
