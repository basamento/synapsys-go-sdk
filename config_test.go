package synapsys

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestDisabledIgnoresStartupConfiguration(t *testing.T) {
	t.Setenv("SYNAPSYS_HEARTBEAT_INTERVAL", "not-a-duration")
	worker, err := New(WithEnabled(false), WithLogger(discardLogger()))
	if err != nil {
		t.Fatalf("New disabled worker: %v", err)
	}
	if err := worker.Start(); err != nil {
		t.Fatalf("Start disabled worker: %v", err)
	}
	if worker.Settings().Enabled {
		t.Fatal("worker should be disabled")
	}
}

func TestDurationEnvironmentRequiresUnits(t *testing.T) {
	t.Setenv("SYNAPSYS_HEARTBEAT_INTERVAL", "5")
	_, err := New(WithEnabled(true), WithLogger(discardLogger()))
	if err == nil || !errors.As(err, new(*ConfigError)) {
		t.Fatalf("New error = %v, want ConfigError", err)
	}
}

func TestOptionsOverrideEnvironment(t *testing.T) {
	t.Setenv("SYNAPSYS_WORKER_NAME", "from-environment")
	t.Setenv("SYNAPSYS_HEARTBEAT_INTERVAL", "30s")
	worker, err := New(
		WithWorkerName("from-option"),
		WithHeartbeatInterval(7*time.Second),
		WithEnabled(true),
		WithLogger(discardLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	settings := worker.Settings()
	if settings.WorkerName != "from-option" || settings.HeartbeatInterval != 7*time.Second {
		t.Fatalf("settings = %#v", settings)
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name   string
		option Option
	}{
		{"fractional heartbeat", WithHeartbeatInterval(1500 * time.Millisecond)},
		{"zero heartbeat", WithHeartbeatInterval(0)},
		{"zero connect timeout", WithConnectTimeout(0)},
		{"zero request timeout", WithRequestTimeout(0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker, err := New(
				WithCoreURL("http://127.0.0.1:1"), WithWorkerName("test"),
				test.option, WithLogger(discardLogger()),
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := validateConfig(worker.config); err == nil {
				t.Fatal("validateConfig succeeded")
			}
		})
	}
}

func TestRegistrationIsAtomicAndUnique(t *testing.T) {
	worker := bareWorker(t)
	err := worker.Register(
		Progressive("same", func() error { return nil }),
		Endless("same", func() error { return nil }),
	)
	if err == nil {
		t.Fatal("Register duplicate names succeeded")
	}
	if got := len(worker.Processes()); got != 0 {
		t.Fatalf("registered %d processes after atomic failure", got)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
