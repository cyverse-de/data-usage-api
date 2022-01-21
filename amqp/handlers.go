package amqp

import (
	"context"
	"fmt"
	"strings"
	"time"

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

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

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

	err = dbs.UpdateUserDataUsageBatch(ctx, usernames[0], usernames[1])
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

func SendBatchMessages(del amqp.Delivery, dedb, icat *sqlx.DB, amqpClient *messaging.Client, configuration *config.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	i := db.NewICAT(icat, configuration.UserSuffix, configuration.Zone, configuration.RootResourceNames)
	batches, err := i.GetUserBatchBounds(ctx, configuration.BatchSize)
	if err != nil {
		return errors.Wrap(err, "Failed getting user batch bounds")
	}
	log.Tracef("batches: %+v", batches)
	var overall_error error
	for _, batch := range batches {
		start := i.UnqualifiedUsername(batch[0])
		end := i.UnqualifiedUsername(batch[1])
		err = amqpClient.Publish(fmt.Sprintf("index.usage.data.batch.user.%s.%s", start, end), []byte{})
		if err != nil {
			log.Error(errors.Wrap(err, fmt.Sprintf("Error publishing message for batch %s - %s", start, end)))
			overall_error = err
			// continue anyway though in case it's specific to this one batch
		}

	}
	return overall_error
}
