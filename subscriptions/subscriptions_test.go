package subscriptions

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cyverse-de/data-usage-api/config"
	"github.com/cyverse-de/p/go/qms"
)

func testConfig() *config.Config {
	return &config.Config{UserSuffix: "example.org"}
}

// newTestClient returns a client pointed at a server that records the request and replies with body.
func newTestClient(t *testing.T, status int, body string, record func(*http.Request)) *Client {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if record != nil {
			record(r)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	c, err := NewClient(srv.URL)
	if err != nil {
		t.Fatalf("building the client: %s", err)
	}
	return c
}

func TestUserCurrentDataUsage(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		wantErr   error
		wantTotal int64
	}{
		{
			name:      "data.size usage is picked out of the list",
			status:    http.StatusOK,
			body:      `{"usages":[{"uuid":"cpu","usage":3,"resource_type":{"name":"cpu.hours"}},{"uuid":"data","usage":42,"resource_type":{"name":"data.size"}}]}`,
			wantTotal: 42,
		},
		{
			// api.UserCurrentUsageHandler branches on this sentinel to enqueue an async refresh and 404.
			name:    "no data.size usage yields sql.ErrNoRows",
			status:  http.StatusOK,
			body:    `{"usages":[{"uuid":"cpu","usage":3,"resource_type":{"name":"cpu.hours"}}]}`,
			wantErr: sql.ErrNoRows,
		},
		{
			name:    "empty usage list yields sql.ErrNoRows",
			status:  http.StatusOK,
			body:    `{"usages":[]}`,
			wantErr: sql.ErrNoRows,
		},
		{
			name:   "a usage without a resource type is skipped",
			status: http.StatusOK,
			body:   `{"usages":[{"uuid":"orphan","usage":9}]}`,
			// Nothing matches, so the sentinel still comes back rather than a nil dereference.
			wantErr: sql.ErrNoRows,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, tt.status, tt.body, nil)

			usage, err := c.UserCurrentDataUsage(context.Background(), testConfig(), "someuser")
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if usage.Total != tt.wantTotal {
				t.Errorf("total = %d, want %d", usage.Total, tt.wantTotal)
			}
		})
	}
}

func TestUserCurrentDataUsageRequest(t *testing.T) {
	var gotPath, gotMethod string

	c := newTestClient(t, http.StatusOK, `{"usages":[{"usage":1,"resource_type":{"name":"data.size"}}]}`,
		func(r *http.Request) {
			gotPath = r.URL.Path
			gotMethod = r.Method
		})

	if _, err := c.UserCurrentDataUsage(context.Background(), testConfig(), "someuser@elsewhere.net"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if gotMethod != http.MethodGet {
		t.Errorf("method = %s, want GET", gotMethod)
	}
	// FixUsername replaces whatever suffix came in with the configured one.
	if want := "/users/someuser@example.org/usages"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
}

func TestAllResourceOveragesForUser(t *testing.T) {
	var gotPath string

	c := newTestClient(t, http.StatusOK, `{"overages":[{"resource_name":"data.size","quota":1,"usage":2}]}`,
		func(r *http.Request) { gotPath = r.URL.Path })

	overages, err := c.AllResourceOveragesForUser(context.Background(), testConfig(), "someuser")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want := "/users/someuser@example.org/overages"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
	if len(overages.Overages) != 1 || overages.Overages[0].ResourceName != "data.size" {
		t.Errorf("overages did not round-trip: %+v", overages.Overages)
	}
}

func TestUpdateUsageForUser(t *testing.T) {
	var (
		gotPath   string
		gotMethod string
		gotBody   qms.AddUpdateRequest
	)

	c := newTestClient(t, http.StatusOK, `{"update":{"uuid":"some-uuid","value":42}}`, func(r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
	})

	usage, err := c.UpdateUsageForUser(context.Background(), testConfig(), "someuser", 42)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if gotMethod != http.MethodPut {
		t.Errorf("method = %s, want PUT", gotMethod)
	}
	// Note the singular "user" segment here; the GET routes use "users".
	if want := "/user/someuser@example.org/updates"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
	if gotBody.Update == nil {
		t.Fatal("the request body carried no update")
	}
	if gotBody.Update.ValueType != "usages" {
		t.Errorf("value_type = %q, want %q", gotBody.Update.ValueType, "usages")
	}
	if gotBody.Update.Operation == nil || gotBody.Update.Operation.Name != "SET" {
		t.Errorf("operation did not round-trip: %+v", gotBody.Update.Operation)
	}
	if gotBody.Update.ResourceType == nil || gotBody.Update.ResourceType.Name != "data.size" {
		t.Errorf("resource_type did not round-trip: %+v", gotBody.Update.ResourceType)
	}
	if usage.Total != 42 {
		t.Errorf("total = %d, want 42", usage.Total)
	}
}

func TestUpdateUsageForUserMissingUpdate(t *testing.T) {
	c := newTestClient(t, http.StatusOK, `{}`, nil)

	if _, err := c.UpdateUsageForUser(context.Background(), testConfig(), "someuser", 42); err == nil {
		t.Fatal("expected an error when the response carries no update")
	}
}

func TestErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantContain string
	}{
		{
			// A populated error envelope means failure even on a 2xx response.
			name:        "error envelope on a 2xx response",
			status:      http.StatusOK,
			body:        `{"error":{"error_code":"NOT_FOUND","status_code":404,"message":"user name not found"}}`,
			wantContain: "user name not found",
		},
		{
			// subscriptions puts the error envelope in non-2xx bodies; its message must survive for triage.
			name:        "non-2xx status carries the server message",
			status:      http.StatusInternalServerError,
			body:        `{"error":{"error_code":"INTERNAL","status_code":500,"message":"boom"}}`,
			wantContain: "boom",
		},
		{
			name:        "non-2xx with an unparseable body still reports the status",
			status:      http.StatusBadGateway,
			body:        `<html>bad gateway</html>`,
			wantContain: "502",
		},
		{
			name:   "unparseable body",
			status: http.StatusOK,
			body:   `not json`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, tt.status, tt.body, nil)

			assertErr := func(call string, err error) {
				t.Helper()
				if err == nil {
					t.Errorf("%s: expected an error, got nil", call)
					return
				}
				if tt.wantContain != "" && !strings.Contains(err.Error(), tt.wantContain) {
					t.Errorf("%s: error %q does not contain %q", call, err, tt.wantContain)
				}
			}

			_, err := c.UserCurrentDataUsage(context.Background(), testConfig(), "someuser")
			assertErr("UserCurrentDataUsage", err)
			_, err = c.AllResourceOveragesForUser(context.Background(), testConfig(), "someuser")
			assertErr("AllResourceOveragesForUser", err)
			_, err = c.UpdateUsageForUser(context.Background(), testConfig(), "someuser", 1)
			assertErr("UpdateUsageForUser", err)
		})
	}
}

func TestNewClientValidation(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{name: "plain http", baseURL: "http://subscriptions"},
		{name: "https with a path prefix", baseURL: "https://example.org/prefix/"},
		{name: "empty", baseURL: "", wantErr: true},
		{name: "missing scheme", baseURL: "subscriptions", wantErr: true},
		{name: "unsupported scheme", baseURL: "nats://subscriptions", wantErr: true},
		{name: "missing host", baseURL: "http://", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewClient(tt.baseURL); (err != nil) != tt.wantErr {
				t.Errorf("NewClient(%q) error = %v, wantErr %v", tt.baseURL, err, tt.wantErr)
			}
		})
	}
}

func TestBaseURLPathPrefix(t *testing.T) {
	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"usages":[{"usage":1,"resource_type":{"name":"data.size"}}]}`))
	}))
	t.Cleanup(srv.Close)

	c, err := NewClient(srv.URL + "/prefix/")
	if err != nil {
		t.Fatalf("building the client: %s", err)
	}

	if _, err := c.UserCurrentDataUsage(context.Background(), testConfig(), "someuser"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if want := "/prefix/users/someuser@example.org/usages"; gotPath != want {
		t.Errorf("path = %s, want %s", gotPath, want)
	}
}

func TestAddUserUpdatesBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/bad@") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":{"error_code":"INTERNAL","status_code":500,"message":"boom"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"update":{"uuid":"good-uuid","value":5}}`))
	}))
	t.Cleanup(srv.Close)

	c, err := NewClient(srv.URL)
	if err != nil {
		t.Fatalf("building the client: %s", err)
	}

	res, err := c.AddUserUpdatesBatch(context.Background(), testConfig(), map[string]float64{"good": 5, "bad": 6})
	if err == nil {
		t.Error("expected the failed user's error to be returned")
	}
	// The failed user must not leave a nil placeholder among the successful results.
	if len(res) != 1 {
		t.Fatalf("results = %d entries, want 1: %+v", len(res), res)
	}
	if res[0] == nil || res[0].ID != "good-uuid" {
		t.Errorf("the successful update did not round-trip: %+v", res[0])
	}
}
