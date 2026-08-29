package synapsys

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnabledFalseMakesNoNetworkCalls(t *testing.T) {
	worker, err := New(WithEnabled(false), WithCoreURL("http://127.0.0.1:1"), WithLogger(discardLogger()))
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Register(Progressive("job", func() error { t.Fatal("process ran"); return nil })); err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	if err := worker.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestFailFastAndNonFailFastStartup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	failFast, _ := New(
		WithCoreURL(server.URL), WithWorkerName("test"), WithFailFast(true),
		WithRequestTimeout(time.Second), WithLogger(discardLogger()),
	)
	var startupError *StartupError
	if err := failFast.Start(); !errors.As(err, &startupError) {
		t.Fatalf("fail-fast Start error = %v", err)
	}

	resilient, _ := New(
		WithCoreURL(server.URL), WithWorkerName("test"), WithFailFast(false),
		WithRequestTimeout(time.Second), WithHeartbeatInterval(time.Second), WithLogger(discardLogger()),
	)
	if err := resilient.Start(); err != nil {
		t.Fatalf("non-fail-fast Start: %v", err)
	}
	if err := resilient.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStopWaitsForConcurrentStartThenStopsWorker(t *testing.T) {
	healthStarted := make(chan struct{})
	releaseHealth := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/health" {
			close(healthStarted)
			<-releaseHealth
			w.WriteHeader(http.StatusOK)
			return
		}
		_ = json.NewEncoder(w).Encode(heartbeatResponse{})
	}))
	defer server.Close()

	worker, err := New(
		WithCoreURL(server.URL), WithWorkerName("test"), WithCoreToken("token"),
		WithHeartbeatInterval(time.Second), WithLogger(discardLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	startResult := make(chan error, 1)
	go func() { startResult <- worker.Start() }()
	<-healthStarted

	stopResult := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { stopResult <- worker.Stop(ctx) }()
	close(releaseHealth)
	if err := <-startResult; err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := <-stopResult; err != nil {
		t.Fatalf("Stop: %v", err)
	}

	worker.mu.Lock()
	started := worker.started
	worker.mu.Unlock()
	if started {
		t.Fatal("worker remained started after concurrent Stop")
	}
}

func TestFinalHeartbeatContainsCleanupLogsAndFinalState(t *testing.T) {
	var mu sync.Mutex
	var heartbeats []heartbeatRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		var payload heartbeatRequest
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode heartbeat: %v", err)
		}
		mu.Lock()
		heartbeats = append(heartbeats, payload)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(heartbeatResponse{Processes: []processUpdate{}})
	}))
	defer server.Close()

	worker, err := New(
		WithCoreURL(server.URL), WithCoreToken("token"), WithWorkerName("test"),
		WithHeartbeatInterval(time.Second), WithLogger(discardLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	logger := worker.Logger("listener")
	if err := worker.Register(EndlessContext("listener", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}, WithOnStop(func() error {
		logger.Info("cleanup complete")
		return nil
	}))); err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(); err != nil {
		t.Fatal(err)
	}
	runID := int64(303)
	if !worker.byName["listener"].start(&runID) {
		t.Fatal("direct start failed")
	}
	waitForState(t, worker.byName["listener"], StateRunning)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := worker.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(heartbeats) == 0 {
		t.Fatal("no heartbeat received")
	}
	last := heartbeats[len(heartbeats)-1]
	if len(last.Processes) != 1 || last.Processes[0].CurrentState != StateIdle {
		t.Fatalf("final process state = %#v", last.Processes)
	}
	if len(last.Logs) != 1 || last.Logs[0].Message != "cleanup complete" {
		t.Fatalf("final logs = %#v", last.Logs)
	}
}

func TestStopDeadlineLeavesIgnoringProcessStopping(t *testing.T) {
	worker := bareWorker(t)
	release := make(chan struct{})
	if err := worker.Register(Endless("stuck", func() error { <-release; return nil })); err != nil {
		t.Fatal(err)
	}
	p := worker.byName["stuck"]
	runID := int64(1)
	p.start(&runID)
	waitForState(t, p, StateRunning)
	p.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if p.settle(ctx) {
		t.Fatal("ignoring process settled")
	}
	if state := p.status().State; state != StateStopping {
		t.Fatalf("state = %s", state)
	}
	close(release)
	waitForState(t, p, StateIdle)
}

func TestUnsupportedCommandDoesNotBlockSibling(t *testing.T) {
	worker := bareWorker(t)
	var ran atomic.Bool
	if err := worker.Register(
		Progressive("bad", func() error { return nil }),
		Progressive("good", func() error { ran.Store(true); return nil }),
	); err != nil {
		t.Fatal(err)
	}
	// A malformed future state is acknowledged without action; the valid sibling
	// still receives its start command in the same response.
	token, runID := int64(1), int64(99)
	worker.applyCommands(heartbeatResponse{Processes: []processUpdate{
		{Name: "bad", DesiredState: "future-state", DesiredToken: &token, RunID: &runID},
		{Name: "good", DesiredState: "running", DesiredToken: &token, RunID: &runID},
	}})
	waitFor(t, ran.Load)
}
