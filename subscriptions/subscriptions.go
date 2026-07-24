package subscriptions

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
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

// maxErrorBodySize caps how much of an error response body is read while looking for the error envelope.
const maxErrorBodySize = 64 * 1024

// Client talks to the subscriptions service over HTTP.
type Client struct {
	baseURL *url.URL
	client  *http.Client
}

// NewClient returns a Client for the given raw base URL, rejecting URLs a request could never reach so
// misconfiguration surfaces at startup instead of on the first lookup.
func NewClient(baseURL string) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse the subscriptions base URL %s", baseURL)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.Errorf("the subscriptions base URL %q must use http or https", baseURL)
	}
	if parsed.Host == "" {
		return nil, errors.Errorf("the subscriptions base URL %q has no host", baseURL)
	}

	return &Client{
		baseURL: parsed,
		client:  &http.Client{Timeout: requestTimeout},
	}, nil
}

// serviceError converts a populated response error envelope into an error. subscriptions reports failures as
// non-2xx responses, but the envelope is checked on 2xx bodies too in case a handler ever reports one there.
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
		// Error responses carry the same envelope in the body; surface its message so failures can be
		// triaged from logs without querying subscriptions.
		var envelope struct {
			Error *svcerror.ServiceError `json:"error"`
		}
		if decodeErr := json.NewDecoder(io.LimitReader(resp.Body, maxErrorBodySize)).Decode(&envelope); decodeErr == nil && envelope.Error != nil && envelope.Error.Message != "" {
			return errors.Errorf("%s returned %d: %s", reqURL, resp.StatusCode, envelope.Error.Message)
		}
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
	if err := c.do(ctx, http.MethodGet, c.baseURL.JoinPath("users", user, "usages"), nil, &response); err != nil {
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
	if err := c.do(ctx, http.MethodGet, c.baseURL.JoinPath("users", user, "overages"), nil, &response); err != nil {
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
	if err = c.do(ctx, http.MethodPut, c.baseURL.JoinPath("user", user, "updates"), bytes.NewReader(body), &response); err != nil {
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

// AddUserUpdatesBatch records usage for each user in usages, continuing past individual failures.
func (c *Client) AddUserUpdatesBatch(ctx context.Context, config *config.Config, usages map[string]float64) ([]*UserDataUsage, error) {
	keys := lo.Keys(usages)
	retval := make([]*UserDataUsage, 0, len(keys))
	errs := make([]error, 0)
	for _, k := range keys {
		u, err := c.UpdateUsageForUser(ctx, config, k, usages[k])
		if err != nil {
			errs = append(errs, err)
			continue
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
