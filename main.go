package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/data-usage-api/api"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

const defaultConfig = `
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
`

func main() {
	var (
		err      error
		config   *viper.Viper
		dbconn   *sqlx.DB
		icatconn *sqlx.DB

		configPath          = flag.String("config", "/etc/iplant/de/data-usage-api.yml", "Full path to the configuration file")
		listenPort          = flag.Int("port", 60000, "The port the service listens on for requests")
		logLevel            = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		refreshIntervalFlag = flag.String("refresh-interval", "3h", "The time between full re-scans of the data store. Must parse as a time.Duration.")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	log.Infof("config path is %s", *configPath)
	log.Infof("listen port is %d", *listenPort)

	config, err = configurate.InitDefaults(*configPath, defaultConfig)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("done reading configuration from %s", *configPath)

	dbURI := config.GetString("db.uri")
	if dbURI == "" {
		log.Fatal("db.uri must be set in the configuration file")
	}

	dbSchema := config.GetString("db.schema")
	if dbSchema == "" {
		log.Fatal("db.schema must be set in the configuration file")
	}

	icatURI := config.GetString("icat.uri")
	if icatURI == "" {
		log.Fatal("icat.uri must be set in the configuration file")
	}

	userSuffix := config.GetString("users.domain")
	if userSuffix == "" {
		log.Fatal("users.domain must be set in the configuration file")
	}

	zone := config.GetString("icat.zone")
	if zone == "" {
		log.Fatal("icat.zone must be set in the configuration file")
	}

	rootResourceNames := config.GetStringSlice("icat.rootResources")
	if rootResourceNames == nil {
		log.Fatal("icat.rootResources must be set in the configuration file")
	}

	//refreshInterval, err := time.ParseDuration(*refreshIntervalFlag)
	_, err = time.ParseDuration(*refreshIntervalFlag)
	if err != nil {
		log.Fatal(err)
	}

	dbconn = sqlx.MustConnect("postgres", dbURI)
	icatconn = sqlx.MustConnect("postgres", icatURI)

	app := api.New(dbconn, dbSchema, icatconn, userSuffix, zone, rootResourceNames)

	log.Infof("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.Router()))
}
