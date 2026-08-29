package synapsys

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestProgressiveCompletesAndDuplicateStartDoesNotDuplicate(t *testing.T) {
	worker := bareWorker(t)
	var calls atomic.Int32
	release := make(chan struct{})
	if err := worker.Register(ProgressiveContext("job", func(context.Context) error {
		calls.Add(1)
		<-release
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	process := worker.byName["job"]
	runID := int64(9_223_372_036_854_775_000)
	token := int64(1)
	worker.applyCommand(process, processUpdate{Name: "job", DesiredState: "running", DesiredToken: &token, RunID: &runID})
	worker.applyCommand(process, processUpdate{Name: "job", DesiredState: "running", DesiredToken: &token, RunID: &runID})
	waitFor(t, func() bool { return calls.Load() == 1 })
	close(release)
	waitForState(t, process, StateIdle)
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
	if process.acknowledged() != token {
		t.Fatalf("ack = %d, want %d", process.acknowledged(), token)
	}
}

func TestStartWhileStoppingRemainsPendingThenRuns(t *testing.T) {
	worker := bareWorker(t)
	firstRelease := make(chan struct{})
	secondRelease := make(chan struct{})
	var calls atomic.Int32
	if err := worker.Register(EndlessContext("listener", func(context.Context) error {
		call := calls.Add(1)
		if call == 1 {
			<-firstRelease // deliberately ignores cancellation until released
		} else {
			<-secondRelease
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	p := worker.byName["listener"]
	run1, run2 := int64(101), int64(202)
	start1, stop1, start2 := int64(1), int64(2), int64(3)
	worker.applyCommand(p, processUpdate{DesiredState: "running", DesiredToken: &start1, RunID: &run1})
	waitFor(t, func() bool { return calls.Load() == 1 })
	worker.applyCommand(p, processUpdate{DesiredState: "stopping", DesiredToken: &stop1, RunID: &run1})
	worker.applyCommand(p, processUpdate{DesiredState: "running", DesiredToken: &start2, RunID: &run2})
	if got := p.acknowledged(); got != stop1 {
		t.Fatalf("ack while stopping = %d, want %d", got, stop1)
	}
	if got := p.status().RunID; got == nil || *got != run1 {
		t.Fatalf("active run ID = %v, want %d", got, run1)
	}
	close(firstRelease)
	waitForState(t, p, StateIdle)
	worker.applyCommand(p, processUpdate{DesiredState: "running", DesiredToken: &start2, RunID: &run2})
	waitFor(t, func() bool { return calls.Load() == 2 })
	if got := p.acknowledged(); got != start2 {
		t.Fatalf("ack after retry = %d, want %d", got, start2)
	}
	close(secondRelease)
	waitForState(t, p, StateIdle)
}

func TestCancellationAndCleanup(t *testing.T) {
	worker := bareWorker(t)
	var cleanup atomic.Int32
	if err := worker.Register(ProgressiveContext("job", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}, WithOnStop(func() error {
		cleanup.Add(1)
		return errors.New("cleanup failed")
	}))); err != nil {
		t.Fatal(err)
	}
	p := worker.byName["job"]
	runID, start, stop := int64(1), int64(1), int64(2)
	worker.applyCommand(p, processUpdate{DesiredState: "running", DesiredToken: &start, RunID: &runID})
	waitForState(t, p, StateRunning)
	worker.applyCommand(p, processUpdate{DesiredState: "stopping", DesiredToken: &stop, RunID: &runID})
	waitForState(t, p, StateIdle)
	if cleanup.Load() != 1 {
		t.Fatalf("cleanup calls = %d", cleanup.Load())
	}
}

func TestErrorAndPanicBecomeFailed(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{"error", func() error { return errors.New("boom") }},
		{"panic", func() error { panic("boom") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			worker := bareWorker(t)
			if err := worker.Register(Progressive("job", test.run)); err != nil {
				t.Fatal(err)
			}
			p := worker.byName["job"]
			runID, token := int64(55), int64(1)
			worker.applyCommand(p, processUpdate{DesiredState: "running", DesiredToken: &token, RunID: &runID})
			waitForState(t, p, StateFailed)
			if worker.logs.size() == 0 {
				t.Fatal("failure produced no captured execution log")
			}
		})
	}
}

func TestEndlessWithoutContextUsesOnStopToUnblock(t *testing.T) {
	worker := bareWorker(t)
	release := make(chan struct{})
	if err := worker.Register(Endless("legacy", func() error {
		<-release
		return fmt.Errorf("listener closed")
	}, WithOnStop(func() error { close(release); return nil }))); err != nil {
		t.Fatal(err)
	}
	p := worker.byName["legacy"]
	runID, start, stop := int64(7), int64(1), int64(2)
	worker.applyCommand(p, processUpdate{DesiredState: "running", DesiredToken: &start, RunID: &runID})
	waitForState(t, p, StateRunning)
	worker.applyCommand(p, processUpdate{DesiredState: "stopping", DesiredToken: &stop, RunID: &runID})
	waitForState(t, p, StateIdle)
}

func bareWorker(t *testing.T) *Worker {
	t.Helper()
	worker, err := New(WithEnabled(false), WithLogger(discardLogger()))
	if err != nil {
		t.Fatal(err)
	}
	worker.config.enabled = true
	worker.config.captureConsole = true
	return worker
}

func waitForState(t *testing.T, process *managedProcess, state ProcessState) {
	t.Helper()
	waitFor(t, func() bool { return process.status().State == state })
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(time.Millisecond)
	}
}
