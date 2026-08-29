package synapsys

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Worker owns an explicit set of Synapsys processes and their lifecycle.
// A Worker is safe for concurrent observation and logging.
type Worker struct {
	config config

	mu        sync.Mutex
	processes []*managedProcess
	byName    map[string]*managedProcess
	started   bool
	starting  bool
	startDone chan struct{}
	stopping  bool
	transport *httpTransport
	hbCancel  context.CancelFunc
	hbDone    chan struct{}

	connectionMu  sync.Mutex
	coreAvailable bool

	logs logQueue

	writerMu sync.Mutex
	writers  map[string][]*captureWriter
}

// New constructs a framework-neutral worker. Environment variables are resolved
// before options, so explicit options win. Network access begins only at Start.
func New(options ...Option) (*Worker, error) {
	resolved, err := resolveConfig(options)
	if err != nil {
		return nil, err
	}
	return &Worker{
		config: resolved, byName: make(map[string]*managedProcess),
		coreAvailable: true, writers: make(map[string][]*captureWriter),
	}, nil
}

// Register adds process declarations atomically. Every process must be registered
// before Start, and names must be unique within the worker.
func (w *Worker) Register(declarations ...Process) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.starting {
		return configError("processes must be registered before the worker starts")
	}
	seen := make(map[string]struct{}, len(declarations))
	for _, declaration := range declarations {
		if declaration.invalid != nil {
			return declaration.invalid
		}
		if declaration.name == "" {
			return configError("process name must not be blank")
		}
		if declaration.typeOf != TypeEndless && declaration.typeOf != TypeProgressive {
			return configError("process %q has unsupported type %q", declaration.name, declaration.typeOf)
		}
		if declaration.run == nil {
			return configError("process %q has a nil body", declaration.name)
		}
		if _, exists := w.byName[declaration.name]; exists {
			return configError("duplicate process name %q", declaration.name)
		}
		if _, exists := seen[declaration.name]; exists {
			return configError("duplicate process name %q", declaration.name)
		}
		seen[declaration.name] = struct{}{}
	}
	for _, declaration := range declarations {
		managed := newManagedProcess(w, declaration)
		w.processes = append(w.processes, managed)
		w.byName[declaration.name] = managed
		w.logDebug("Registered process", "process", declaration.name, "type", declaration.typeOf)
	}
	return nil
}

// Settings returns resolved configuration with the bearer token omitted.
func (w *Worker) Settings() Settings { return w.config.settings() }

// Processes returns process snapshots in registration order.
func (w *Worker) Processes() []ProcessStatus {
	w.mu.Lock()
	processes := append([]*managedProcess(nil), w.processes...)
	w.mu.Unlock()
	result := make([]ProcessStatus, 0, len(processes))
	for _, process := range processes {
		result = append(result, process.status())
	}
	return result
}

// Start validates configuration, performs one bounded health probe, and starts
// the heartbeat loop. Core unavailability is non-fatal unless fail-fast is set.
func (w *Worker) Start() error {
	w.mu.Lock()
	if !w.config.enabled {
		w.mu.Unlock()
		return nil
	}
	if w.started {
		w.mu.Unlock()
		return nil
	}
	if w.starting || w.stopping {
		w.mu.Unlock()
		return configError("worker lifecycle transition is already in progress")
	}
	w.starting = true
	startDone := make(chan struct{})
	w.startDone = startDone
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.starting = false
		w.startDone = nil
		w.mu.Unlock()
		close(startDone)
	}()

	warnings, err := validateConfig(w.config)
	if err != nil {
		return err
	}
	for _, warning := range warnings {
		w.logWarn(warning)
	}
	transport, err := newHTTPTransport(w.config)
	if err != nil {
		return configError("could not configure Core transport: %v", err)
	}

	status, healthErr := transport.health(context.Background())
	reachable := healthErr == nil && status >= 200 && status < 300
	if !reachable && w.config.failFast {
		transport.close()
		cause := healthErr
		if cause == nil {
			cause = fmt.Errorf("core health check returned status %d", status)
		}
		return &StartupError{Cause: cause}
	}
	if !reachable {
		w.connectionMu.Lock()
		w.coreAvailable = false
		w.connectionMu.Unlock()
		if healthErr != nil {
			w.logWarn("Core is unreachable at startup; heartbeat retries will continue", "core", w.config.coreURL, "error", healthErr)
		} else {
			w.logWarn("Core is unreachable at startup; heartbeat retries will continue", "core", w.config.coreURL, "status", status)
		}
	}

	heartbeatContext, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	w.mu.Lock()
	w.transport = transport
	w.hbCancel = cancel
	w.hbDone = done
	w.started = true
	w.mu.Unlock()

	w.logInfo("Worker registered", "worker", w.config.workerName, "processes", len(w.Processes()),
		"core", w.config.coreURL, "host", displayHost(w.config.host), "heartbeat", formatDuration(w.config.heartbeatInterval))
	go w.heartbeatLoop(heartbeatContext, done)
	return nil
}

// Stop cooperatively stops processes, waits up to ctx's deadline, sends one
// best-effort final state/log heartbeat, and releases transport resources.
// It is idempotent; final-heartbeat failures are logged and never returned.
func (w *Worker) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w.mu.Lock()
	if w.starting {
		startDone := w.startDone
		w.mu.Unlock()
		select {
		case <-startDone:
			return w.Stop(ctx)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if !w.started || w.stopping {
		w.mu.Unlock()
		return nil
	}
	w.stopping = true
	cancel := w.hbCancel
	done := w.hbDone
	transport := w.transport
	processes := append([]*managedProcess(nil), w.processes...)
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			w.logWarn("Heartbeat did not stop before the shutdown deadline")
		}
	}

	for _, process := range processes {
		process.stop()
	}
	allStopped := true
	for _, process := range processes {
		if !process.settle(ctx) {
			allStopped = false
			w.logWarn("Process did not stop before the shutdown deadline", "process", process.decl.name)
		}
	}

	// The response is deliberately not applied: application shutdown must never
	// start new work. Current states (including honest `stopping`) and cleanup logs
	// still get one bounded delivery attempt.
	if ctx.Err() == nil && transport != nil {
		finalCtx, finalCancel := context.WithTimeout(ctx, w.config.requestTimeout)
		if err := w.tick(finalCtx, false); err != nil {
			w.logDebug("Final heartbeat failed", "error", err)
		}
		finalCancel()
	}
	if transport != nil {
		transport.close()
	}

	w.mu.Lock()
	w.started = false
	w.stopping = false
	w.transport = nil
	w.hbCancel = nil
	w.hbDone = nil
	w.mu.Unlock()
	w.logInfo("Worker stopped", "worker", w.config.workerName)

	if !allStopped && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (w *Worker) captureEnabled(level slog.Level) bool {
	return w.config.enabled && w.config.captureConsole && level >= slog.LevelInfo
}

func (w *Worker) processActive(name string) bool {
	w.mu.Lock()
	process := w.byName[name]
	w.mu.Unlock()
	if process == nil {
		return false
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	return (process.state == StateRunning || process.state == StateStopping) && process.runID != nil
}

func (w *Worker) enqueueProcessLine(name string, source LogSource, raw string) {
	if !w.config.enabled || !w.config.captureConsole {
		return
	}
	line := strings.TrimSuffix(raw, "\r")
	if strings.TrimSpace(line) == "" {
		return
	}
	w.mu.Lock()
	process := w.byName[name]
	w.mu.Unlock()
	if process == nil {
		return
	}
	runID, sequence, ok := process.nextLogContext()
	if !ok || runID == nil {
		return
	}
	if source != Stderr {
		source = Stdout
	}
	w.logs.enqueue(processLog{
		ProcessName: name, RunID: *runID, Sequence: sequence,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Source: source,
		Level:   map[bool]string{true: "error", false: "info"}[source == Stderr],
		Message: truncateRunes(line, maxLogLineRunes),
	})
}

func (w *Worker) captureException(name, text string) {
	for _, line := range strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n") {
		w.enqueueProcessLine(name, Stderr, line)
	}
}

func (w *Worker) flushWriters(name string) {
	w.writerMu.Lock()
	writers := append([]*captureWriter(nil), w.writers[name]...)
	w.writerMu.Unlock()
	for _, writer := range writers {
		writer.flush()
	}
}

func (w *Worker) markCoreUnavailable(message string, cause error) {
	w.connectionMu.Lock()
	wasAvailable := w.coreAvailable
	w.coreAvailable = false
	w.connectionMu.Unlock()
	if wasAvailable {
		if cause != nil {
			w.logWarn(message+"; heartbeat retries will continue", "error", cause)
		} else {
			w.logWarn(message + "; heartbeat retries will continue")
		}
	} else if cause != nil {
		w.logDebug(message, "error", cause)
	} else {
		w.logDebug(message)
	}
}

func (w *Worker) markCoreAvailable() {
	w.connectionMu.Lock()
	wasAvailable := w.coreAvailable
	w.coreAvailable = true
	w.connectionMu.Unlock()
	if !wasAvailable {
		w.logInfo("Core connection restored")
	}
}

func (w *Worker) logDebug(message string, args ...any) {
	w.config.logger.Debug("[Synapsys] "+message, args...)
}

func (w *Worker) logInfo(message string, args ...any) {
	w.config.logger.Info("[Synapsys] "+message, args...)
}

func (w *Worker) logWarn(message string, args ...any) {
	w.config.logger.Warn("[Synapsys] "+message, args...)
}

func (w *Worker) logError(message string, args ...any) {
	w.config.logger.Error("[Synapsys] "+message, args...)
}

func displayHost(host string) string {
	if host == "" {
		return "<unset>"
	}
	return host
}
