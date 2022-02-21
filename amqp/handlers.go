package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
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

type UsageUpdate struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
	Unit      string `json:"unit"`
	Username  string `json:"username"`
	UserID    string `json:"user_id"`
}

func SendUserUsageUpdateMessage(res *db.UserDataUsage, amqpClient *messaging.Client) error {
	update := &UsageUpdate{
		Attribute: "data.size",
		Value:     strconv.FormatInt(res.Total, 10),
		Unit:      "bytes",
		Username:  res.Username,
		UserID:    res.UserID,
	}
	marshalled, err := json.Marshal(update)
	if err != nil {
		e := errors.Wrap(err, "Failed marshalling JSON AMQP message")
		log.Error(e)
		return e
	}

	err = amqpClient.Publish("qms.usages", marshalled)
	if err != nil {
		e := errors.Wrap(err, "Failed sending usage update AMQP message")
		log.Error(e)
		return e
	}

	return nil
}

func UpdateUserHandler(del amqp.Delivery, dedb, icat *sqlx.DB, amqpClient *messaging.Client, configuration *config.Config) error {
	username := del.RoutingKey[len(SingleUserPrefix)+1:]
	user := util.FixUsername(username, configuration)

	log.Tracef("Recalculating usage for %s asynchronously", user)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dbs := db.NewBoth(dedb, icat, configuration)

	res, err := dbs.UpdateUserDataUsage(ctx, user)
	if err != nil {
		e := errors.Wrap(err, "Failed updating usage information")
		log.Error(e)
		rejectErr := del.Reject(!del.Redelivered)
		if rejectErr != nil {
			log.Error(errors.Wrap(rejectErr, "Failed rejecting failed message"))
		}
		return e
	}

	err = SendUserUsageUpdateMessage(res, amqpClient)
	if err != nil {
		return err
	}

	return nil
}

func UpdateUserBatchHandler(del amqp.Delivery, dedb, icat *sqlx.DB, amqpClient *messaging.Client, configuration *config.Config) error {
	usernames := strings.SplitN(del.RoutingKey[len(BatchUserPrefix)+1:], ".", 2)
	log.Infof("Updating the user batch from %s to %s", usernames[0], usernames[1])

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	dbs := db.NewBoth(dedb, icat, configuration)

	res, err := dbs.UpdateUserDataUsageBatch(ctx, usernames[0], usernames[1])
	if err != nil {
		e := errors.Wrap(err, "Failed updating usage information")
		log.Error(e)
		rejectErr := del.Reject(!del.Redelivered)
		if rejectErr != nil {
			log.Error(errors.Wrap(rejectErr, "Failed rejecting failed message"))
		}
		return e
	}

	for _, r := range res {
		err = SendUserUsageUpdateMessage(r, amqpClient)
		if err != nil {
			return err
		}
	}

	return nil
}

func SendBatchMessages(del amqp.Delivery, dedb, icat *sqlx.DB, amqpClient *messaging.Client, configuration *config.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	i := db.NewICAT(icat, configuration)
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
