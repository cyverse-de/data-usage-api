package natsconn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/db"
	"github.com/cyverse-de/data-usage-api/util"
	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/cyverse-de/p/go/qms"
	"github.com/labstack/gommon/log"
	"github.com/nats-io/nats.go"
)

type Connector struct {
	baseSubject string
	baseQueue   string
	Conn        *nats.EncodedConn
}

type ConnectorSettings struct {
	BaseSubject   string
	BaseQueue     string
	NATSCluster   string
	CredsPath     string
	CAPath        string
	TLSKeyPath    string
	TLSCertPath   string
	MaxReconnects int
	ReconnectWait int
	EnvPrefix     string
}

func (nc *Connector) buildSubject(base string, fields ...string) string {
	trimmed := strings.TrimSuffix(
		strings.TrimSuffix(base, ".*"),
		".>",
	)
	addFields := strings.Join(fields, ".")
	return fmt.Sprintf("%s.%s", trimmed, addFields)
}

func (nc *Connector) buildQueueName(qBase string, fields ...string) string {
	return fmt.Sprintf("%s.%s", qBase, strings.Join(fields, "."))
}

func (nc *Connector) Subscribe(name string, handler nats.Handler) (string, string, error) {
	var err error

	subject := nc.buildSubject(nc.baseSubject, name)
	queue := nc.buildQueueName(nc.baseQueue, name)

	if _, err = nc.Conn.QueueSubscribe(subject, queue, handler); err != nil {
		return "", "", err
	}

	return subject, queue, nil
}

func NewConnector(cs *ConnectorSettings) (*Connector, error) {
	nats.RegisterEncoder("protojson", protobufjson.NewCodec(protobufjson.WithEmitUnpopulated()))

	nc, err := nats.Connect(
		cs.NATSCluster,
		nats.UserCredentials(cs.CredsPath),
		nats.RootCAs(cs.CAPath),
		nats.ClientCert(cs.TLSCertPath, cs.TLSKeyPath),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(cs.MaxReconnects),
		nats.ReconnectWait(time.Duration(cs.ReconnectWait)*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Errorf("disconnected from nats: %s", err.Error())
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Infof("reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Errorf("connection closed: %s", nc.LastError().Error())
		}),
	)
	if err != nil {
		return nil, err
	}

	ec, err := nats.NewEncodedConn(nc, "protojson")
	if err != nil {
		return nil, err
	}

	connector := &Connector{
		baseSubject: cs.BaseSubject,
		baseQueue:   cs.BaseQueue,
		Conn:        ec,
	}

	return connector, nil
}

func (nc *Connector) SendUserUsageUpdateMessage(ctx context.Context, res *db.UserDataUsage) error {
	return gotelnats.Publish(ctx, nc.Conn, "cyverse.qms.user.usages.add",
		pbinit.NewAddUsage(res.Username, "data.size", "SET", float64(res.Total)),
	)
}

func (nc *Connector) AllResourceOveragesForUser(ctx context.Context, config *config.Config, username string) (*qms.OverageList, error) {
	var err error

	subject := "cyverse.qms.user.overages.get"

	req := &qms.AllUserOveragesRequest{
		Username: util.FixUsername(username, config),
	}

	_, span := pbinit.InitAllUserOveragesRequest(req, subject)
	defer span.End()

	resp := pbinit.NewOverageList()

	if err = gotelnats.Request(
		ctx,
		nc.Conn,
		subject,
		req,
		resp,
	); err != nil {
		return nil, err
	}

	return resp, nil
}
