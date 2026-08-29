package synapsys

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

const frameworkIdentifier = "go"

type heartbeatProcess struct {
	Name         string       `json:"name"`
	Type         ProcessType  `json:"type"`
	CurrentState ProcessState `json:"currentState"`
	AckToken     int64        `json:"ackToken"`
	RunID        *int64       `json:"runId"`
}

type heartbeatRequest struct {
	WorkerName        string             `json:"workerName"`
	Framework         string             `json:"framework"`
	Host              string             `json:"host,omitempty"`
	HeartbeatInterval int64              `json:"heartbeatInterval"`
	Processes         []heartbeatProcess `json:"processes"`
	Logs              []processLog       `json:"logs,omitempty"`
}

type processUpdate struct {
	Name         string `json:"name"`
	DesiredState string `json:"desiredState"`
	DesiredToken *int64 `json:"desiredToken"`
	RunID        *int64 `json:"runId"`
}

type heartbeatResponse struct {
	Processes []processUpdate `json:"processes"`
}

func (w *Worker) heartbeatLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			w.safeTick(ctx, true)
			timer.Reset(w.config.heartbeatInterval)
		}
	}
}

func (w *Worker) safeTick(ctx context.Context, applyCommands bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			w.logError("Unexpected heartbeat panic; heartbeat retries will continue", "panic", recovered)
		}
	}()
	if err := w.tick(ctx, applyCommands); err != nil {
		w.logError("Unexpected heartbeat failure; heartbeat retries will continue", "error", err)
	}
}

func (w *Worker) tick(ctx context.Context, applyCommands bool) error {
	logs := w.logs.drain(maxLogsPerBeat, maxLogBytesPerBeat)
	payload := w.heartbeatPayload(logs)
	status, response, err := w.transport.heartbeat(ctx, payload)
	if err != nil {
		w.logs.requeueFront(logs)
		w.markCoreUnavailable("Failed to contact Core", err)
		return nil
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		w.logs.requeueFront(logs)
		w.markCoreUnavailable(fmt.Sprintf("Heartbeat rejected by Core with status %d", status), nil)
		return nil
	}
	w.markCoreAvailable()
	if applyCommands {
		w.applyCommands(response)
	}
	return nil
}

func (w *Worker) heartbeatPayload(logs []processLog) heartbeatRequest {
	w.mu.Lock()
	processes := append([]*managedProcess(nil), w.processes...)
	w.mu.Unlock()
	snapshots := make([]heartbeatProcess, 0, len(processes))
	for _, process := range processes {
		snapshots = append(snapshots, process.snapshot())
	}
	return heartbeatRequest{
		WorkerName: w.config.workerName, Framework: frameworkIdentifier,
		Host: w.config.host, HeartbeatInterval: int64(w.config.heartbeatInterval / time.Second),
		Processes: snapshots, Logs: logs,
	}
}

func (w *Worker) applyCommands(response heartbeatResponse) {
	for _, update := range response.Processes {
		w.mu.Lock()
		process := w.byName[update.Name]
		w.mu.Unlock()
		if process == nil {
			w.logWarn("Core sent a command for an unknown process; ignoring", "process", update.Name)
			continue
		}
		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					w.logError("Command handling panicked", "process", update.Name, "panic", recovered)
				}
			}()
			w.applyCommand(process, update)
		}()
	}
}

func (w *Worker) applyCommand(process *managedProcess, update processUpdate) {
	if update.DesiredToken == nil {
		process.syncRunID(update.RunID)
		return
	}
	token := *update.DesiredToken
	if token <= process.acknowledged() {
		process.syncRunID(update.RunID)
		return
	}
	accepted := true
	switch update.DesiredState {
	case string(StateIdle):
		// Idle means no outstanding command, never stop.
		process.syncRunID(update.RunID)
	case string(StateRunning):
		w.logInfo("Starting process", "process", process.decl.name, "type", process.decl.typeOf, "token", token, "runId", update.RunID)
		if update.RunID == nil {
			w.logWarn("Starting process without a run ID; execution logs cannot be attached", "process", process.decl.name)
		}
		accepted = process.start(update.RunID)
	case string(StateStopping):
		process.syncRunID(update.RunID)
		w.logInfo("Stopping process", "process", process.decl.name, "type", process.decl.typeOf, "token", token)
		accepted = process.stop()
	default:
		process.syncRunID(update.RunID)
		w.logDebug("Ignoring unsupported desired state", "process", process.decl.name, "state", update.DesiredState)
	}
	if accepted {
		process.acknowledge(token)
	} else {
		w.logDebug("Command remains pending until the current transition completes", "process", process.decl.name, "token", token)
	}
}
