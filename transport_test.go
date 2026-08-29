package synapsys

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestTransportDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Add(1)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer origin.Close()

	transport, err := newHTTPTransport(config{
		coreURL: origin.URL, coreToken: "secret", connectTimeout: time.Second, requestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.close()
	status, _, err := transport.heartbeat(context.Background(), heartbeatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusFound {
		t.Fatalf("status = %d", status)
	}
	if redirected.Load() != 0 {
		t.Fatal("redirect target was contacted")
	}
}

func TestTransportParsesUnknownFieldsAndExactInt64(t *testing.T) {
	const runID int64 = 9_223_372_036_854_775_000
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"unknown":{"future":true},"processes":[{"name":"job","desiredState":"running","desiredToken":4,"runId":9223372036854775000,"newField":"ignored"}]}`))
	}))
	defer server.Close()
	transport, err := newHTTPTransport(config{
		coreURL: server.URL, connectTimeout: time.Second, requestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.close()
	_, response, err := transport.heartbeat(context.Background(), heartbeatRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Processes) != 1 || response.Processes[0].RunID == nil || *response.Processes[0].RunID != runID {
		t.Fatalf("response = %#v", response)
	}
}

func TestHeartbeatCarriesIdentityAuthenticationAndEveryProcess(t *testing.T) {
	requests := make(chan heartbeatRequest, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/health":
			w.WriteHeader(http.StatusOK)
		case "/api/v1/workers/heartbeat":
			if got := r.Header.Get("Authorization"); got != "Bearer worker-token" {
				t.Errorf("Authorization = %q", got)
			}
			var payload heartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode request: %v", err)
			}
			requests <- payload
			_ = json.NewEncoder(w).Encode(heartbeatResponse{Processes: []processUpdate{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	worker, err := New(
		WithCoreURL(server.URL), WithCoreToken("worker-token"), WithWorkerName("billing"),
		WithHeartbeatInterval(time.Second), WithLogger(discardLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Register(
		Progressive("one", func() error { return nil }),
		EndlessContext("two", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }),
	); err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	defer worker.Stop(context.Background())
	select {
	case payload := <-requests:
		if payload.WorkerName != "billing" || payload.Framework != "go" || payload.HeartbeatInterval != 1 {
			t.Fatalf("identity payload = %#v", payload)
		}
		if len(payload.Processes) != 2 {
			t.Fatalf("processes = %d", len(payload.Processes))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat not received")
	}
}
