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
	logStats("DE", deconn)
	detx, err := deconn.BeginTxx(context, nil)
	if err != nil {
		return nil, func() {}, func() error { return err }, errors.Wrap(err, "Error creating DE database transaction")
	}

	logStats("ICAT", icatconn)
	icattx, err := icatconn.BeginTxx(context, nil)
	if err != nil {
		return nil, func() {}, func() error { return err }, errors.Wrap(err, "Error creating ICAT transaction")
	}

	dedb := NewDE(detx, deschema)
	icatdb := NewICAT(icattx, userSuffix, zone, rootResourceNames)

	rb := func() {
		err := detx.Rollback()
		if err != nil {
			e := errors.Wrap(err, "Error rolling back DE database transaction")
			log.Error(e)
		}
		err = icattx.Rollback()
		if err != nil {
			e := errors.Wrap(err, "Error rolling back ICAT transaction")
			log.Error(e)
		}
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
		log.Infof("No usage information was found for user %s. Attempting to add a usage of 0 anyway", username)
	} else if err != nil {
		return nil, errors.Wrap(err, "Error getting current data usage")
	}

	// if this update shouldn't be added, or should amend a prior reading, do it here or in the method called below
	// or maybe have an async cleanup process that deduplicates readings

	res, err := b.dedb.AddUserDataUsage(context, username, usagenum, time.Now())
	if err == sql.ErrNoRows {
		e := errors.Wrap(err, "No data could be inserted. Perhaps the user doesn't exist in the DE database")
		log.Error(e)
		return nil, e

	}

	return res, err
}

func (b *BothDatabases) UpdateUserDataUsageBatch(context context.Context, start, end string) error {
	// should pass in qualified usernames, icatdb method will strip it as needed
	usages, err := b.icatdb.BatchCurrentDataUsage(context, start, end)
	if err != nil {
		return err
	}

	log.Tracef("usages in batch: %+v", usages)

	var us []string
	usagesFixed := make(map[string]int64)
	for usr, usg := range usages { // keys of usages map
		us = append(us, b.icatdb.FixUsername(usr))
		usagesFixed[b.icatdb.FixUsername(usr)] = usg
	}

	err = b.dedb.EnsureUsers(context, us)
	if err != nil {
		return errors.Wrap(err, "Error ensuring users exist")
	}

	_, err = b.dedb.AddUserDataUsageBatch(context, usagesFixed, time.Now())
	if err != nil {
		return errors.Wrap(err, "Error inserting new usage")
	}

	return nil
}
