package natsconn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/util"
	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/cyverse-de/go-mod/subjects"
	"github.com/cyverse-de/p/go/qms"
	"github.com/labstack/gommon/log"
	"github.com/nats-io/nats.go"
	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			if err != nil {
				log.Errorf("disconnected from nats: %s", err.Error())
			} else {
				log.Errorf("disconnected from nats")
			}
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

func (nc *Connector) SendUserUsageUpdateMessage(ctx context.Context, username string, total float64) error {
	return gotelnats.Publish(ctx, nc.Conn, "cyverse.qms.user.usages.add",
		pbinit.NewAddUsage(username, "data.size", "SET", total),
	)
}

func (nc *Connector) UserCurrentDataUsage(ctx context.Context, config *config.Config, username string) (*UserDataUsage, error) {
	var err error

	req := &qms.GetUsages{
		Username: util.FixUsername(username, config),
	}

	_, span := pbinit.InitGetUsages(req, subjects.QMSGetUserUsages)
	defer span.End()

	resp := pbinit.NewUsageList()

	if err = gotelnats.Request(ctx, nc.Conn, subjects.QMSGetUserUsages, req, resp); err != nil {
		return nil, err
	}

	var usage *qms.Usage
	for _, u := range resp.Usages {
		if u.ResourceType.Name == "data.size" {
			usage = u
		}
	}

	if usage == nil {
		return nil, sql.ErrNoRows
	}

	retval := &UserDataUsage{
		ID:           usage.Uuid,
		Total:        int64(usage.Usage),
		Time:         usage.CreatedAt.AsTime(),
		LastModified: usage.LastModifiedAt.AsTime(),
	}

	return retval, nil
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

func (nc *Connector) UpdateUsageForUser(ctx context.Context, config *config.Config, username string, usageValue float64) (*UserDataUsage, error) {
	var err error

	user := util.FixUsername(username, config)

	up := &qms.Update{
		Value:         usageValue,
		EffectiveDate: timestamppb.Now(),
		Operation: &qms.UpdateOperation{
			Name: "SET",
		},
		ResourceType: &qms.ResourceType{
			Name: "data.size",
			Unit: "bytes",
		},
		User: &qms.QMSUser{
			Username: user,
		},
		ValueType: "usages",
	}

	req := pbinit.NewAddUpdateRequest(up)
	_, span := pbinit.InitQMSAddUpdateRequest(req, subjects.QMSAddUserUpdate)
	defer span.End()

	resp := pbinit.NewQMSAddUpdateResponse()

	if err = gotelnats.Request(ctx, nc.Conn, subjects.QMSAddUserUpdate, req, resp); err != nil {
		return nil, err
	}

	usage := resp.Update

	retval := &UserDataUsage{
		ID:           usage.Uuid,
		Total:        int64(usage.Value),
		Time:         usage.EffectiveDate.AsTime(),
		LastModified: usage.EffectiveDate.AsTime(),
	}

	return retval, nil
}

func (nc *Connector) AddUserUpdatesBatch(ctx context.Context, config *config.Config, usages map[string]float64) ([]*UserDataUsage, error) {
	keys := lo.Keys(usages)
	retval := make([]*UserDataUsage, 0)
	errs := make([]error, 0)
	for _, k := range keys {
		u, err := nc.UpdateUsageForUser(ctx, config, k, usages[k])
		if err != nil {
			errs = append(errs, err)
		}
		retval = append(retval, u)
	}
	// If we got errors, throw the first one. It's a little ugly but is the
	// error that'd get thrown if we were doing it in the loop anyway.
	if len(errs) > 0 {
		return retval, errs[0]
	}
	return retval, nil
}
