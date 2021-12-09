package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cyverse-de/configurate"
	a "github.com/cyverse-de/data-usage-api/amqp"
	"github.com/cyverse-de/data-usage-api/api"
	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/cyverse-de/messaging"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"

	_ "github.com/lib/pq"
)

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

func getQueueName(prefix string) string {
	if len(prefix) > 0 {
		return fmt.Sprintf("%s.data-usage-api", prefix)
	}
	return "data-usage-api"
}

func main() {
	var (
		err           error
		cfg           *viper.Viper
		dbconn        *sqlx.DB
		icatconn      *sqlx.DB
		configuration *config.Config
		app           *api.App

		configPath = flag.String("config", "/etc/iplant/de/data-usage-api.yml", "Full path to the configuration file")
		listenPort = flag.Int("port", 60000, "The port the service listens on for requests")
		logLevel   = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	log.Infof("config path is %s", *configPath)
	log.Infof("listen port is %d", *listenPort)

	cfg, err = configurate.InitDefaults(*configPath, defaultConfig)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("done reading configuration from %s", *configPath)

	configuration, err = config.NewFromViper(cfg)
	if err != nil {
		log.Fatal(err)
	}

	dbconn = sqlx.MustConnect("postgres", configuration.DBURI)
	icatconn = sqlx.MustConnect("postgres", configuration.ICATURI)

	// configure and start AMQP bits here
	listenClient, err := messaging.NewClient(configuration.AMQPURI, true)
	if err != nil {
		log.Fatal(errors.Wrap(err, "Unable to create the messaging listen client"))
	}
	defer listenClient.Close()

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

	go listenClient.Listen()

	queueName := getQueueName(configuration.AMQPQueuePrefix)
	listenClient.AddConsumerMulti(
		configuration.AMQPExchangeName,
		configuration.AMQPExchangeType,
		queueName,
		[]string{"index.all", "index.usage.data", "index.usage.data.batch.user.#", a.SingleUserPrefix + ".#"},
		func(del amqp.Delivery) {
			var err error
			log.Tracef("Got message: %s", del.RoutingKey)
			if del.RoutingKey == "index.all" || del.RoutingKey == "index.usage.data" {
				err = a.SendBatchMessages(del, dbconn, icatconn, publishClient, configuration)
			} else if strings.HasPrefix(del.RoutingKey, a.BatchUserPrefix) {
				err = a.UpdateUserBatchHandler(del, dbconn, icatconn, configuration)
			} else if strings.HasPrefix(del.RoutingKey, a.SingleUserPrefix) {
				err = a.UpdateUserHandler(del, dbconn, icatconn, configuration)
			}
			if err != nil {
				log.Error(errors.Wrap(err, "Error handling message"))
				return
			}
			err = del.Ack(false)
			if err != nil {
				log.Error(errors.Wrap(err, fmt.Sprintf("Error acknowledging message: %s", del.RoutingKey)))
			}
		},
		1)
	// - listen for index.all (for convenience) and index.usage.data, and fetch all applicable users, batch them, and send out batch messages - start-of-batch usernames can have no dots so routing keys work
	// - listen for index.usage.data.batch.user.<start>.<end>, and update the usage information for users from <start> to <end>, inclusive
	// - listen for index.usage.data.user.<username>, and update the usage information for just that user

	app = api.New(dbconn, icatconn, publishClient, configuration)

	log.Infof("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.Router()))
}
