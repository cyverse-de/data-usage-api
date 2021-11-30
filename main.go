package main

import (
	"flag"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/data-usage-api/logging"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

//func getHandler(dbClient *sqlx.DB) amqp.HandlerFn {
//	dedb := db.New(dbClient)
//
//	return func(userID, externalID, state string) {
//		event := db.CPUUsageEvent{
//			CreatedBy: userID,
//		}
//
//		// Set up a context with a deadline of 2 minutes. This should prevent a backlog of go routines
//		// from building up.
//		ctx, cancelFn := context.WithDeadline(context.Background(), time.Now().Add(time.Minute*2))
//		defer cancelFn()
//
//		// Look up the analysis ID from the externalID.
//		analysisID, err := dedb.GetAnalysisIDByExternalID(ctx, externalID)
//		if err != nil {
//			log.Error(err)
//			return
//		}
//
//		// Get the start date of the analysis.
//		analysis, err := dedb.Analysis(ctx, userID, analysisID)
//		if err != nil {
//			log.Error(err)
//			return
//		}
//
//		if !analysis.StartDate.Valid {
//			log.Errorf("analysis %s: start date was null", analysis.ID)
//		}
//		startDate := analysis.StartDate.Time
//
//		// Get the current date. Can't really depend on the sent_on field.
//		nowTime := time.Now()
//
//		// Calculate the number of hours betwen the start date and the current date.
//		hours := nowTime.Sub(startDate).Hours()
//
//		// Get the number of millicores requested for the analysis.
//		// TODO: figure out the right way to handle default values. Default to 1.0 for now.
//		millicores := 1000.0
//
//		// Multiply the number of hours by the number of millicores.
//		// Divide the result by 1000 to get the number of CPU hours. 1000 millicores = 1 CPU core.
//		cpuHours := (millicores * hours) / 1000.0
//
//		// Add the event to the database.
//		event.EffectiveDate = nowTime
//		event.RecordDate = nowTime
//		event.Value = int64(cpuHours)
//
//		if err = dedb.AddCPUUsageEvent(ctx, &event); err != nil {
//			log.Error(err)
//		}
//	}
//}

func main() {
	var (
		err    error
		config *viper.Viper
		dbconn *sqlx.DB

		configPath = flag.String("config", "/etc/iplant/de/data-usage-api.yml", "Full path to the configuration file")
		listenPort = flag.Int("port", 60000, "The port the service listens on for requests")
		//queue                    = flag.String("queue", "resource-usage-api", "The AMQP queue name for this service")
		//reconnect                = flag.Bool("reconnect", false, "Whether the AMQP client should reconnect on failure")
		logLevel            = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		refreshIntervalFlag = flag.String("refresh-interval", "3h", "The time between full re-scans of the data store. Must parse as a time.Duration.")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	log.Infof("config path is %s", *configPath)
	log.Infof("listen port is %d", listenPort)

	config, err = configurate.Init(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("done reading configuration from %s", *configPath)

	dbURI := config.GetString("db.uri")
	if dbURI == "" {
		log.Fatal("db.uri must be set in the configuration file")
	}

	icatURI := config.GetString("icat.uri")
	if icatURI == "" {
		log.Fatal("icat.uri must be set in the configuration file")
	}

	userSuffix := config.GetString("users.domain")
	if userSuffix == "" {
		log.Fatal("users.domain must be set in the configuration file")
	}

	refreshInterval, err := time.ParseDuration(*refreshIntervalFlag)
	if err != nil {
		log.Fatal(err)
	}

	dbconn = sqlx.MustConnect("postgres", dbURI)

	//app := internal.New(dbconn, userSuffix)

	//workerConfig := worker.Config{
	//	Name:                    strings.ReplaceAll(uuid.New().String(), "-", ""),
	//	ExpirationInterval:      workerLifetime,
	//	RefreshInterval:         refreshInterval,
	//	WorkerPurgeInterval:     purgeWorkersInterval,
	//	WorkSeekerPurgeInterval: purgeSeekersInterval,
	//	WorkClaimPurgeInterval:  purgeClaimsInterval,
	//	ClaimLifetime:           claimLifetime,
	//	WorkSeekingLifetime:     seekingLifetime,
	//	NewUserTotalInterval:    newUserTotalInterval,
	//}

	//log.Infof("worker name is %s", workerConfig.Name)

	//w, err := worker.New(context.Background(), &workerConfig, dbconn)
	//if err != nil {
	//	log.Fatal(err)
	//}

	//log.Infof("worker ID is %s", w.ID)

	//go w.Start(context.Background())

	//log.Infof("listening on port %d", *listenPort)
	//log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", strconv.Itoa(*listenPort)), app.Router()))
}
