package synapsys

import "time"

// ProcessType identifies a managed process's execution model.
type ProcessType string

const (
	// TypeEndless identifies business logic that remains running until stopped.
	TypeEndless ProcessType = "endless"
	// TypeProgressive identifies finite business logic that returns naturally.
	TypeProgressive ProcessType = "progressive"
)

// ProcessState is the state reported to Synapsys Core.
type ProcessState string

const (
	StateIdle     ProcessState = "idle"
	StateRunning  ProcessState = "running"
	StateStopping ProcessState = "stopping"
	StateFailed   ProcessState = "failed"
)

// LogSource identifies the conventional stream represented by a captured line.
type LogSource string

const (
	Stdout LogSource = "stdout"
	Stderr LogSource = "stderr"
)

// ProcessStatus is a concurrency-safe snapshot of one registered process.
type ProcessStatus struct {
	Name     string
	Type     ProcessType
	State    ProcessState
	AckToken int64
	RunID    *int64
}

// Settings is the resolved, non-secret worker configuration.
type Settings struct {
	Enabled           bool
	CoreURL           string
	WorkerName        string
	Host              string
	HeartbeatInterval time.Duration
	FailFast          bool
	ConnectTimeout    time.Duration
	RequestTimeout    time.Duration
	CaptureConsole    bool
}
