package subscriptions

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/data-usage-api/util"
	"github.com/cyverse-de/p/go/ptypes"
	"github.com/cyverse-de/p/go/qms"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

// dataSizeResource is the QMS resource type this service reports usage for.
const dataSizeResource = "data.size"

// requestTimeout matches the timeout the NATS request/reply calls this client replaced used. The lookups it
// serves sit on the /current and /overage request paths, so an unresponsive subscriptions must not pin them.
const requestTimeout = 30 * time.Second

// Client talks to the subscriptions service over HTTP.
type Client struct {
	baseURL *url.URL
	client  *http.Client
}

// NewClient returns a Client for the given raw base URL.
func NewClient(baseURL string) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse the subscriptions base URL %s", baseURL)
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")

	return &Client{
		baseURL: parsed,
		client:  &http.Client{Timeout: requestTimeout},
	}, nil
}

func (c *Client) url(components ...string) *url.URL {
	newURL := *c.baseURL

	escaped := make([]string, len(components))
	for i, component := range components {
		escaped[i] = url.PathEscape(component)
	}
	newURL.Path = fmt.Sprintf("%s/%s", newURL.Path, strings.Join(escaped, "/"))

	return &newURL
}

// serviceError converts a populated response error envelope into an error. subscriptions reports request
// failures in the response body as well as the status code, so a 2xx response can still describe a failure.
func serviceError(serr *svcerror.ServiceError) error {
	if serr == nil || serr.ErrorCode == svcerror.ErrorCode_UNSET {
		return nil
	}
	return errors.Errorf("subscriptions returned an error: %s", serr.Message)
}

// do sends the request and decodes the response envelope into out.
func (c *Client) do(ctx context.Context, method string, reqURL *url.URL, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, reqURL.String(), body)
	if err != nil {
		return errors.Wrapf(err, "unable to build the request for %s", reqURL)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "unable to send the request to %s", reqURL)
	}
	defer resp.Body.Close() // nolint: errcheck

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return errors.Errorf("%s returned %d", reqURL, resp.StatusCode)
	}

	if err = json.NewDecoder(resp.Body).Decode(out); err != nil {
		return errors.Wrapf(err, "unable to parse the response body from %s", reqURL)
	}

	return nil
}

// UserCurrentDataUsage returns the user's current data.size usage as recorded in QMS. It returns sql.ErrNoRows
// when QMS has no data.size usage for the user; callers use that to trigger an asynchronous refresh.
func (c *Client) UserCurrentDataUsage(ctx context.Context, config *config.Config, username string) (*UserDataUsage, error) {
	user := util.FixUsername(username, config)

	var response qms.UsageList
	if err := c.do(ctx, http.MethodGet, c.url("users", user, "usages"), nil, &response); err != nil {
		return nil, err
	}
	if err := serviceError(response.Error); err != nil {
		return nil, err
	}

	var usage *qms.Usage
	for _, u := range response.Usages {
		if u.ResourceType != nil && u.ResourceType.Name == dataSizeResource {
			usage = u
		}
	}

	if usage == nil {
		return nil, sql.ErrNoRows
	}

	return &UserDataUsage{
		ID:           usage.Uuid,
		Total:        int64(usage.Usage),
		Time:         usage.CreatedAt.AsTime(),
		LastModified: usage.LastModifiedAt.AsTime(),
	}, nil
}

// AllResourceOveragesForUser returns every resource the user is currently over quota on.
func (c *Client) AllResourceOveragesForUser(ctx context.Context, config *config.Config, username string) (*qms.OverageList, error) {
	user := util.FixUsername(username, config)

	var response qms.OverageList
	if err := c.do(ctx, http.MethodGet, c.url("users", user, "overages"), nil, &response); err != nil {
		return nil, err
	}
	if err := serviceError(response.Error); err != nil {
		return nil, err
	}

	return &response, nil
}

// UpdateUsageForUser records the user's data.size usage in QMS, replacing any previous value.
func (c *Client) UpdateUsageForUser(ctx context.Context, config *config.Config, username string, usageValue float64) (*UserDataUsage, error) {
	user := util.FixUsername(username, config)

	request := &qms.AddUpdateRequest{
		Update: &qms.Update{
			ValueType:     "usages",
			Value:         usageValue,
			EffectiveDate: ptypes.Now(),
			Operation:     &qms.UpdateOperation{Name: "SET"},
			ResourceType:  &qms.ResourceType{Name: dataSizeResource, Unit: "bytes"},
			User:          &qms.QMSUser{Username: user},
		},
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal the usage update")
	}

	var response qms.AddUpdateResponse
	if err = c.do(ctx, http.MethodPut, c.url("user", user, "updates"), bytes.NewReader(body), &response); err != nil {
		return nil, err
	}
	if err = serviceError(response.Error); err != nil {
		return nil, err
	}
	if response.Update == nil {
		return nil, errors.New("subscriptions returned no update for the recorded usage")
	}

	usage := response.Update

	return &UserDataUsage{
		ID:           usage.Uuid,
		Total:        int64(usage.Value),
		Time:         usage.EffectiveDate.AsTime(),
		LastModified: usage.EffectiveDate.AsTime(),
	}, nil
}

// SendUserUsageUpdateMessage sets the user's data.size usage directly, bypassing the updates table. This
// duplicates the value UpdateUsageForUser already recorded via an update; it is kept because the NATS publish
// it replaces did the same thing.
func (c *Client) SendUserUsageUpdateMessage(ctx context.Context, username string, total float64) error {
	request := &qms.AddUsage{
		Username:     username,
		ResourceName: dataSizeResource,
		UpdateType:   "SET",
		UsageValue:   total,
	}

	body, err := json.Marshal(request)
	if err != nil {
		return errors.Wrap(err, "unable to marshal the usage")
	}

	var response qms.UsageResponse
	if err = c.do(ctx, http.MethodPut, c.url("users", username, "usages"), bytes.NewReader(body), &response); err != nil {
		return err
	}

	return serviceError(response.Error)
}

// AddUserUpdatesBatch records usage for each user in usages, continuing past individual failures.
func (c *Client) AddUserUpdatesBatch(ctx context.Context, config *config.Config, usages map[string]float64) ([]*UserDataUsage, error) {
	keys := lo.Keys(usages)
	retval := make([]*UserDataUsage, 0, len(keys))
	errs := make([]error, 0)
	for _, k := range keys {
		u, err := c.UpdateUsageForUser(ctx, config, k, usages[k])
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
