package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/db"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/cyverse-de/data-usage-api/natsconn"
	"github.com/cyverse-de/data-usage-api/util"
	"github.com/cyverse-de/messaging/v9"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
)

var otelName = "github.com/cyverse-de/data-usage-api/amqp"
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

type Updater struct {
	amqpClient *messaging.Client
}

func NewUpdater(c *messaging.Client) *Updater {
	return &Updater{
		amqpClient: c,
	}
}

type UsageUpdateMessenger interface {
	SendUserUsageUpdateMessage(context.Context, *db.UserDataUsage) error
}

func (u *Updater) SendUserUsageUpdateMessage(ctx context.Context, res *db.UserDataUsage) error {
	ctx, span := otel.Tracer(otelName).Start(ctx, "SendUserUsageUpdateMessage")
	defer span.End()

	log.Tracef("Sending user usage update message for %v", res)
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

	err = u.amqpClient.PublishContext(ctx, "qms.usages", marshalled)
	if err != nil {
		e := errors.Wrap(err, "Failed sending usage update AMQP message")
		log.Error(e)
		return e
	}
	log.Trace("Done sending user usage update message")

	return nil
}

func UpdateUserHandler(ctx context.Context, del amqp.Delivery, dedb, icat *sqlx.DB, nc *natsconn.Connector, configuration *config.Config) error {
	username := del.RoutingKey[len(SingleUserPrefix)+1:]
	user := util.FixUsername(username, configuration)

	log.Tracef("Recalculating usage for %s asynchronously", user)

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	ctx, span := otel.Tracer(otelName).Start(ctx, "UpdateUserHandler")
	defer span.End()

	dbs := db.NewBoth(dedb, icat, configuration, nc)

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

	err = nc.SendUserUsageUpdateMessage(ctx, res.Username, float64(res.Total))
	if err != nil {
		return err
	}

	return nil
}

func UpdateUserBatchHandler(ctx context.Context, del amqp.Delivery, dedb, icat *sqlx.DB, nc *natsconn.Connector, configuration *config.Config) error {
	usernames := strings.SplitN(del.RoutingKey[len(BatchUserPrefix)+1:], ".", 2)
	log.Infof("Updating the user batch from %s to %s", usernames[0], usernames[1])

	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	ctx, span := otel.Tracer(otelName).Start(ctx, "UpdateUserBatchHandler")
	defer span.End()

	dbs := db.NewBoth(dedb, icat, configuration, nc)

	_, err := dbs.UpdateUserDataUsageBatch(ctx, usernames[0], usernames[1])
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

func SendBatchMessages(ctx context.Context, del amqp.Delivery, dedb, icat *sqlx.DB, amqpClient *messaging.Client, configuration *config.Config) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	ctx, span := otel.Tracer(otelName).Start(ctx, "SendBatchMessages")
	defer span.End()

	i := db.NewICAT(icat, configuration)

	// Randomly add a number between -2 and 2
	// i.e. in [0,5) minus 2
	// This makes the bounds different each run, which will help us catch 0
	// values that might otherwise repeatedly fall between batch bounds
	boundModifier := rand.Intn(5) - 2

	batches, err := i.GetUserBatchBounds(ctx, configuration.BatchSize+boundModifier)
	if err != nil {
		return errors.Wrap(err, "Failed getting user batch bounds")
	}
	log.Tracef("batches: %+v", batches)
	var overallError error
	for _, batch := range batches {
		start := i.UnqualifiedUsername(batch[0])
		end := i.UnqualifiedUsername(batch[1])
		err = amqpClient.PublishContext(ctx, fmt.Sprintf("index.usage.data.batch.user.%s.%s", start, end), []byte{})
		if err != nil {
			log.Error(errors.Wrap(err, fmt.Sprintf("Error publishing message for batch %s - %s", start, end)))
			overallError = err
			// continue anyway though in case it's specific to this one batch
		}

	}
	return overallError
}
