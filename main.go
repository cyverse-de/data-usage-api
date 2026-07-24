package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cyverse-de/configurate"
	a "github.com/cyverse-de/data-usage-api/amqp"
	"github.com/cyverse-de/data-usage-api/api"
	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/cyverse-de/data-usage-api/subscriptions"

	"github.com/cyverse-de/messaging/v9"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"

	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"

	"github.com/cyverse-de/go-mod/otelutils"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	_ "github.com/lib/pq"
)

const serviceName = "data-usage-api"

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

const defaultConfig = `
dataUsageApi:
  refreshInterval: 3h

db:
  uri: postgres://de:notprod@dedb:5432/de?sslmode=disable
  schema: public

icat:
  uri: postgres://ICAT:fakepassword@icat-db:5432/ICAT?sslmode=disable
  zone: iplant
  rootResources:
    - mainIngestRes
    - mainReplRes

users:
  domain: example.com

amqp:
  uri: amqp://guest:guest@rabbit:5672/
  queue_prefix: ""
  exchange:
    name: de
    type: topic
  batch_size: 100
`

func getQueueNames(prefix string) (string, string) {
	if len(prefix) > 0 {
		return fmt.Sprintf("%s.%s.batch", prefix, serviceName), fmt.Sprintf("%s.%s.individual", prefix, serviceName)
	}
	return fmt.Sprintf("%s.batch", serviceName), fmt.Sprintf("%s.individual", serviceName)
}

func main() {
	var (
		err           error
		cfg           *viper.Viper
		dbconn        *sqlx.DB
		icatconn      *sqlx.DB
		configuration *config.Config
		app           *api.App

		configPath        = flag.String("config", "/etc/iplant/de/data-usage-api.yml", "Full path to the configuration file")
		listenPort        = flag.Int("port", 60000, "The port the service listens on for requests")
		logLevel          = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		subscriptionsBase = flag.String("subscriptions-base-uri", "http://subscriptions", "The base URL for contacting the subscriptions service")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	tracerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown := otelutils.TracerProviderFromEnv(tracerCtx, serviceName, func(e error) { log.Fatal(e) })
	defer shutdown()

	log.Infof("config path is %s", *configPath)
	log.Infof("listen port is %d", *listenPort)
	log.Infof("subscriptions base URI is %s", *subscriptionsBase)

	cfg, err = configurate.InitDefaults(*configPath, defaultConfig)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("done reading configuration from %s", *configPath)

	configuration, err = config.NewFromViper(cfg)
	if err != nil {
		log.Fatal(err)
	}

	subscriptionsClient, err := subscriptions.NewClient(*subscriptionsBase)
	if err != nil {
		log.Fatal(err)
	}

	// set up database connection
	dbconn = otelsqlx.MustConnect("postgres", configuration.DBURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL))
	dbconn.SetMaxOpenConns(10)
	dbconn.SetConnMaxIdleTime(time.Minute)

	icatconn = otelsqlx.MustConnect("postgres", configuration.ICATURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL))
	icatconn.SetMaxOpenConns(10)
	icatconn.SetConnMaxIdleTime(time.Minute)

	// configure and start AMQP bits here
	batchListenClient, err := messaging.NewClient(configuration.AMQPURI, true)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Unable to create the messaging listen client (batch)"))
	}
	defer batchListenClient.Close()

	individualListenClient, err := messaging.NewClient(configuration.AMQPURI, true)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Unable to create the messaging listen client (individual)"))
	}
	defer individualListenClient.Close()

	publishClient, err := messaging.NewClient(configuration.AMQPURI, true)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Unable to create the messaging publish client"))
	}
	defer publishClient.Close()

	log.Info(configuration.AMQPExchangeName)
	err = publishClient.SetupPublishing(configuration.AMQPExchangeName)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Unable to set up message publishing"))
	}

	go batchListenClient.Listen()
	go individualListenClient.Listen()

	// we can use the same handler function for both batch and individual,
	// because the separate queues/routing keys ensure the separation
	amqpHandlerFunc := func(ctx context.Context, del amqp.Delivery) {
		var err error

		log.Tracef("Got message: %s", del.RoutingKey)
		if del.RoutingKey == "index.all" || del.RoutingKey == "index.usage.data" {
			err = a.SendBatchMessages(ctx, del, dbconn, icatconn, publishClient, configuration)
		} else if strings.HasPrefix(del.RoutingKey, a.BatchUserPrefix) {
			err = a.UpdateUserBatchHandler(ctx, del, dbconn, icatconn, subscriptionsClient, configuration)
		} else if strings.HasPrefix(del.RoutingKey, a.SingleUserPrefix) {
			err = a.UpdateUserHandler(ctx, del, dbconn, icatconn, subscriptionsClient, configuration)
		}
		if err != nil {
			log.Error(errors.Wrap(err, "Error handling message"))
			return
		}
		err = del.Ack(false)
		if err != nil {
			log.Error(errors.Wrap(err, fmt.Sprintf("Error acknowledging message: %s", del.RoutingKey)))
		}
	}

	batchQueueName, individualQueueName := getQueueNames(configuration.AMQPQueuePrefix)
	// batch handler
	// - listen for index.all (for convenience) and index.usage.data, and fetch all applicable users, batch them, and send out batch messages - start-of-batch usernames can have no dots so routing keys work
	// - listen for index.usage.data.batch.user.<start>.<end>, and update the usage information for users from <start> to <end>, inclusive
	batchListenClient.AddConsumerMulti(
		configuration.AMQPExchangeName,
		configuration.AMQPExchangeType,
		batchQueueName,
		[]string{"index.all", "index.usage.data", a.BatchUserPrefix + ".#"},
		amqpHandlerFunc,
		1)

	// individual user handler
	// - listen for index.usage.data.user.<username>, and update the usage information for just that user
	individualListenClient.AddConsumerMulti(
		configuration.AMQPExchangeName,
		configuration.AMQPExchangeType,
		individualQueueName,
		[]string{a.SingleUserPrefix + ".#"},
		amqpHandlerFunc,
		1)

	app = api.New(dbconn, icatconn, publishClient, subscriptionsClient, configuration)

	log.Infof("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.Router()))
}
