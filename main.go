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
	"github.com/cyverse-de/data-usage-api/db"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/cyverse-de/data-usage-api/natsconn"
	"github.com/nats-io/nats.go"

	"github.com/cyverse-de/messaging/v9"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"

	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"

	decfg "github.com/cyverse-de/go-mod/cfg"
	"github.com/cyverse-de/go-mod/gotelnats"
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

func getQueueName(prefix string) string {
	if len(prefix) > 0 {
		return fmt.Sprintf("%s.%s", prefix, serviceName)
	}
	return serviceName
}

type UsageUpdateMessenger interface {
	SendUserUsageUpdateMessage(context.Context, *db.UserDataUsage) error
}

func main() {
	var (
		err           error
		cfg           *viper.Viper
		dbconn        *sqlx.DB
		icatconn      *sqlx.DB
		configuration *config.Config
		app           *api.App

		configPath    = flag.String("config", "/etc/iplant/de/data-usage-api.yml", "Full path to the configuration file")
		listenPort    = flag.Int("port", 60000, "The port the service listens on for requests")
		logLevel      = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		dotEnvPath    = flag.String("dotenv-path", decfg.DefaultDotEnvPath, "Path to the dotenv file")
		tlsCert       = flag.String("tlscert", gotelnats.DefaultTLSCertPath, "Path to the NATS TLS cert file")
		tlsKey        = flag.String("tlskey", gotelnats.DefaultTLSKeyPath, "Path to the NATS TLS key file")
		caCert        = flag.String("tlsca", gotelnats.DefaultTLSCAPath, "Path to the NATS TLS CA file")
		credsPath     = flag.String("creds", gotelnats.DefaultCredsPath, "Path to the NATS creds file")
		maxReconnects = flag.Int("max-reconnects", gotelnats.DefaultMaxReconnects, "Maximum number of reconnection attempts to NATS")
		reconnectWait = flag.Int("reconnect-wait", gotelnats.DefaultReconnectWait, "Seconds to wait between reconnection attempts to NATS")
		envPrefix     = flag.String("env-prefix", decfg.DefaultEnvPrefix, "The prefix for environment variables")
		natsSubject   = flag.String("nats-subject", "cyverse.data.usage.>", "The subject prefix for NATS subscriptions")
		natsQueue     = flag.String("nats-queue", "cyverse.data.usage", "The name of the NATS queue")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	tracerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown := otelutils.TracerProviderFromEnv(tracerCtx, serviceName, func(e error) { log.Fatal(e) })
	defer shutdown()

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

	// read in NATS configuration from the dotenv file.
	envCfg, err := decfg.Init(&decfg.Settings{
		EnvPrefix:   *envPrefix,
		ConfigPath:  *configPath,
		DotEnvPath:  *dotEnvPath,
		StrictMerge: false,
		FileType:    decfg.YAML,
	})
	if err != nil {
		log.Fatal(err)
	}

	// set up NATS connection
	natsCluster := envCfg.String("nats.cluster")
	if natsCluster == "" {
		log.Fatalf("The %sNATS_CLUSTER environment variable or nats.cluster configuration value must be set", *envPrefix)
	}

	log.Infof("nats.cluster is set to '%s'", natsCluster)
	log.Infof("NATS TLS cert file is %s", *tlsCert)
	log.Infof("NATS TLS key file is %s", *tlsKey)
	log.Infof("NATS CA cert file is %s", *caCert)
	log.Infof("NATS creds file is %s", *credsPath)
	log.Infof("NATS max reconnects is %d", *maxReconnects)
	log.Infof("NATS reonnect wait is %t", *reconnectWait)

	natsConn, err := natsconn.NewConnector(&natsconn.ConnectorSettings{
		BaseSubject:   *natsSubject,
		BaseQueue:     *natsQueue,
		NATSCluster:   natsCluster,
		CredsPath:     *credsPath,
		TLSKeyPath:    *tlsKey,
		TLSCertPath:   *tlsCert,
		MaxReconnects: *maxReconnects,
		ReconnectWait: *reconnectWait,
	})
	if err != nil {
		log.Fatal(err)
	}

	natsConn.Subscribe("ping", func(m *nats.Msg) {
		log.Info("ping message received")
		err := m.Respond([]byte("pong"))
		if err != nil {
			log.Error(err)
		}
	})

	log.Info("connected to nats cluster")

	// set up database connection
	dbconn = otelsqlx.MustConnect("postgres", configuration.DBURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL))
	dbconn.SetMaxOpenConns(10)
	dbconn.SetConnMaxIdleTime(time.Minute)

	icatconn = otelsqlx.MustConnect("postgres", configuration.ICATURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL))
	icatconn.SetMaxOpenConns(10)
	icatconn.SetConnMaxIdleTime(time.Minute)

	// set up the NATS connection

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

	updater := a.NewUpdater(publishClient)

	queueName := getQueueName(configuration.AMQPQueuePrefix)
	listenClient.AddConsumerMulti(
		configuration.AMQPExchangeName,
		configuration.AMQPExchangeType,
		queueName,
		[]string{"index.all", "index.usage.data", "index.usage.data.batch.user.#", a.SingleUserPrefix + ".#"},
		func(ctx context.Context, del amqp.Delivery) {
			var err error

			log.Tracef("Got message: %s", del.RoutingKey)
			if del.RoutingKey == "index.all" || del.RoutingKey == "index.usage.data" {
				err = a.SendBatchMessages(ctx, del, dbconn, icatconn, publishClient, configuration)
			} else if strings.HasPrefix(del.RoutingKey, a.BatchUserPrefix) {
				err = a.UpdateUserBatchHandler(ctx, del, dbconn, icatconn, updater, configuration)
			} else if strings.HasPrefix(del.RoutingKey, a.SingleUserPrefix) {
				err = a.UpdateUserHandler(ctx, del, dbconn, icatconn, updater, configuration)
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
