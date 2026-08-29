package synapsys

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

const maxResponseBytes = 1 << 20

type httpTransport struct {
	base           *url.URL
	token          string
	requestTimeout time.Duration
	client         *http.Client
	transport      *http.Transport
}

type httpResponse struct {
	status int
	body   []byte
}

func newHTTPTransport(c config) (*httpTransport, error) {
	base, err := url.Parse(c.coreURL)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: c.connectTimeout, KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   c.connectTimeout,
		ResponseHeaderTimeout: c.requestTimeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   4,
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &httpTransport{
		base: base, token: c.coreToken, requestTimeout: c.requestTimeout,
		client: client, transport: transport,
	}, nil
}

func (t *httpTransport) health(ctx context.Context) (int, error) {
	response, err := t.do(ctx, http.MethodGet, "/api/health", nil, false)
	if err != nil {
		return 0, err
	}
	return response.status, nil
}

func (t *httpTransport) heartbeat(ctx context.Context, payload heartbeatRequest) (int, heartbeatResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, heartbeatResponse{}, fmt.Errorf("encode heartbeat: %w", err)
	}
	response, err := t.do(ctx, http.MethodPost, "/api/v1/workers/heartbeat", body, true)
	if err != nil {
		return 0, heartbeatResponse{}, err
	}
	if response.status < 200 || response.status >= 300 {
		return response.status, heartbeatResponse{}, nil
	}
	var decoded heartbeatResponse
	if len(response.body) != 0 {
		if err := json.Unmarshal(response.body, &decoded); err != nil {
			return response.status, heartbeatResponse{}, fmt.Errorf("decode heartbeat response: %w", err)
		}
	}
	return response.status, decoded, nil
}

func (t *httpTransport) do(parent context.Context, method, endpoint string, body []byte, authenticated bool) (httpResponse, error) {
	ctx, cancel := context.WithTimeout(parent, t.requestTimeout)
	defer cancel()

	requestURL := *t.base
	requestURL.Path = joinURLPath(t.base.Path, endpoint)
	requestURL.RawPath = ""
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), reader)
	if err != nil {
		return httpResponse{}, fmt.Errorf("create Core request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if authenticated {
		request.Header.Set("Authorization", "Bearer "+t.token)
	}

	response, err := t.client.Do(request)
	if err != nil {
		return httpResponse{}, fmt.Errorf("Core request %s %s: %w", method, safeURL(requestURL), err)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, maxResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return httpResponse{}, fmt.Errorf("read Core response: %w", err)
	}
	if len(data) > maxResponseBytes {
		return httpResponse{}, fmt.Errorf("Core response exceeded %d bytes", maxResponseBytes)
	}
	return httpResponse{status: response.StatusCode, body: data}, nil
}

func (t *httpTransport) close() { t.transport.CloseIdleConnections() }

func joinURLPath(basePath, endpoint string) string {
	prefix := strings.TrimSuffix(basePath, "/")
	suffix := "/" + strings.TrimPrefix(endpoint, "/")
	joined := path.Clean(prefix + suffix)
	if !strings.HasPrefix(joined, "/") {
		return "/" + joined
	}
	return joined
}

func safeURL(value url.URL) string {
	value.RawQuery = ""
	value.Fragment = ""
	value.User = nil
	return value.String()
}
