package amqp

import (
	"context"
	"strings"

	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/db"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/cyverse-de/data-usage-api/util"
	"github.com/cyverse-de/messaging"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "amqp"})

const SingleUserPrefix = "index.usage.data.user"
const BatchUserPrefix = "index.usage.data.batch.user"

func UpdateUserHandler(del amqp.Delivery, dedb, icat *sqlx.DB, configuration *config.Config) error {
	username := del.RoutingKey[len(SingleUserPrefix)+1:]
	user := util.FixUsername(username, configuration)

	log.Tracef("Recalculating usage for %s asynchronously", user)

	ctx := context.Background()

	dbs, rb, commit, err := db.NewBothTx(ctx, dedb, configuration.DBSchema, icat, configuration.UserSuffix, configuration.Zone, configuration.RootResourceNames)
	if err != nil {
		e := errors.Wrap(err, "Failed setting up database")
		log.Error(e)
		rejectErr := del.Reject(!del.Redelivered)
		if rejectErr != nil {
			log.Error(errors.Wrap(rejectErr, "Failed rejecting failed message"))
		}
		return e
	}
	defer rb()

	// no need to hold onto the response here
	_, err = dbs.UpdateUserDataUsage(ctx, user)
	if err != nil {
		e := errors.Wrap(err, "Failed updating usage information")
		log.Error(e)
		rejectErr := del.Reject(!del.Redelivered)
		if rejectErr != nil {
			log.Error(errors.Wrap(rejectErr, "Failed rejecting failed message"))
		}
		return e
	}
	err = commit()
	if err != nil {
		e := errors.Wrap(err, "Failed updating usage information")
		log.Error(e)
		rejectErr := del.Reject(!del.Redelivered)
		if rejectErr != nil {
			log.Error(errors.Wrap(rejectErr, "Failed rejecting failed message"))
		}
		return e
	}

	return nil
}

func UpdateUserBatchHandler(del amqp.Delivery, dedb, icat *sqlx.DB, configuration *config.Config) error {
	usernames := strings.SplitN(del.RoutingKey[len(BatchUserPrefix)+1:], ".", 2)
	log.Infof("Updating the user batch from %s to %s", usernames[0], usernames[1])
	ctx := context.Background()
	icattx, err := icat.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	i := db.NewICAT(icattx, configuration.UserSuffix, configuration.Zone, configuration.RootResourceNames)
	usages, err := i.BatchCurrentDataUsage(ctx, usernames[0], usernames[1])
	if err != nil {
		return err
	}

	log.Tracef("usages in batch: %+v", usages)
	return nil
}

func SendBatchMessages(del amqp.Delivery, dedb, icat *sqlx.DB, amqpClient *messaging.Client, configuration *config.Config) error {
	return nil
}
