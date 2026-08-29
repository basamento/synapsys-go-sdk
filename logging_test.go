package synapsys

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestLoggerAndWriterCaptureOnlyActiveProcess(t *testing.T) {
	worker := bareWorker(t)
	release := make(chan struct{})
	if err := worker.Register(Progressive("job", func() error { <-release; return nil })); err != nil {
		t.Fatal(err)
	}
	logger := worker.Logger("job")
	var console bytes.Buffer
	writer := worker.Writer("job", Stdout, &console)

	logger.Info("outside")
	_, _ = writer.Write([]byte("outside\n"))
	if worker.logs.size() != 0 {
		t.Fatal("captured output outside an execution")
	}

	p := worker.byName["job"]
	runID := int64(9_223_372_036_854_775_000)
	if !p.start(&runID) {
		t.Fatal("start was not accepted")
	}
	waitForState(t, p, StateRunning)
	logger.Info("processed", "count", 3)
	logger.Error("failed once")
	_, _ = writer.Write([]byte("first "))
	_, _ = writer.Write([]byte("line\n"))
	close(release)
	waitForState(t, p, StateIdle)

	entries := worker.logs.drain(10, 96_000)
	if len(entries) != 3 {
		t.Fatalf("captured %d entries: %#v", len(entries), entries)
	}
	if entries[0].RunID != runID || entries[0].Sequence != 1 || !strings.Contains(entries[0].Message, "count=3") {
		t.Fatalf("first entry = %#v", entries[0])
	}
	if entries[1].Source != Stderr || entries[1].Level != "error" {
		t.Fatalf("error entry = %#v", entries[1])
	}
	if !strings.Contains(console.String(), "outside") || !strings.Contains(console.String(), "first line") {
		t.Fatalf("tee output = %q", console.String())
	}
}

func TestLogQueueBoundsBytesAndRequeues(t *testing.T) {
	var queue logQueue
	queue.enqueue(processLog{Message: "éé"}) // four UTF-8 bytes
	queue.enqueue(processLog{Message: "next"})
	batch := queue.drain(100, 4)
	if len(batch) != 1 {
		t.Fatalf("drained %d entries", len(batch))
	}
	queue.requeueFront(batch)
	redrained := queue.drain(100, 100)
	if len(redrained) != 2 || redrained[0].Message != "éé" {
		t.Fatalf("requeued order = %#v", redrained)
	}
}

func TestUnknownSlogLevelsBelowInfoAreNotCaptured(t *testing.T) {
	worker := bareWorker(t)
	release := make(chan struct{})
	if err := worker.Register(Progressive("job", func() error { <-release; return nil })); err != nil {
		t.Fatal(err)
	}
	p := worker.byName["job"]
	runID := int64(1)
	p.start(&runID)
	worker.Logger("job").Log(context.Background(), slog.LevelDebug, "debug")
	close(release)
	waitForState(t, p, StateIdle)
	if worker.logs.size() != 0 {
		t.Fatal("debug record was captured")
	}
}

func TestLoggerPreservesSlogAttributeGroupScope(t *testing.T) {
	worker := bareWorker(t)
	release := make(chan struct{})
	if err := worker.Register(Progressive("job", func() error { <-release; return nil })); err != nil {
		t.Fatal(err)
	}
	p := worker.byName["job"]
	runID := int64(1)
	p.start(&runID)
	waitForState(t, p, StateRunning)

	logger := worker.Logger("job").With("component", "billing").WithGroup("request").With("id", 7)
	logger.Info("handled", "items", 3)
	close(release)
	waitForState(t, p, StateIdle)

	entries := worker.logs.drain(10, 96_000)
	if len(entries) != 1 {
		t.Fatalf("entries = %#v", entries)
	}
	message := entries[0].Message
	for _, want := range []string{`component="billing"`, "request.id=7", "request.items=3"} {
		if !strings.Contains(message, want) {
			t.Fatalf("message %q does not contain %q", message, want)
		}
	}
	if strings.Contains(message, "request.component") {
		t.Fatalf("attribute added before WithGroup was regrouped: %q", message)
	}
}
