package synapsys

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	maxLogQueueEntries = 10_000
	maxLogLineRunes    = 16_000
	maxPartialBytes    = maxLogLineRunes * utf8.UTFMax
	maxLogsPerBeat     = 100
	maxLogBytesPerBeat = 96_000
)

type processLog struct {
	ProcessName string    `json:"processName"`
	RunID       int64     `json:"runId"`
	Sequence    int64     `json:"sequence"`
	Timestamp   string    `json:"timestamp"`
	Source      LogSource `json:"source"`
	Level       string    `json:"level"`
	Message     string    `json:"message"`
}

type logQueue struct {
	mu      sync.Mutex
	entries []processLog
}

func (q *logQueue) enqueue(entry processLog) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) >= maxLogQueueEntries {
		copy(q.entries, q.entries[1:])
		q.entries = q.entries[:len(q.entries)-1]
	}
	q.entries = append(q.entries, entry)
}

func (q *logQueue) drain(maxEntries, maxBytes int) []processLog {
	q.mu.Lock()
	defer q.mu.Unlock()
	count := 0
	bytes := 0
	for count < len(q.entries) && count < maxEntries {
		nextBytes := len([]byte(q.entries[count].Message))
		if count > 0 && bytes+nextBytes > maxBytes {
			break
		}
		bytes += nextBytes
		count++
	}
	if count == 0 {
		return nil
	}
	result := append([]processLog(nil), q.entries[:count]...)
	copy(q.entries, q.entries[count:])
	q.entries = q.entries[:len(q.entries)-count]
	return result
}

func (q *logQueue) requeueFront(entries []processLog) {
	if len(entries) == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	combined := make([]processLog, 0, min(maxLogQueueEntries, len(entries)+len(q.entries)))
	combined = append(combined, entries...)
	combined = append(combined, q.entries...)
	if len(combined) > maxLogQueueEntries {
		combined = combined[:maxLogQueueEntries]
	}
	q.entries = combined
}

func (q *logQueue) size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Logger returns a process-scoped structured logger. Records continue to the
// application's configured logger and, while that process has an active run,
// are also queued for Core.
func (w *Worker) Logger(processName string) *slog.Logger {
	return slog.New(&captureHandler{
		worker: w, processName: strings.TrimSpace(processName), next: w.config.logger.Handler(),
	})
}

// Writer returns a process-scoped tee. Bytes always go to next; complete lines
// written during an active run are also queued for Core. The caller owns next.
func (w *Worker) Writer(processName string, source LogSource, next io.Writer) io.Writer {
	if next == nil {
		next = io.Discard
	}
	if source != Stderr {
		source = Stdout
	}
	writer := &captureWriter{worker: w, processName: strings.TrimSpace(processName), source: source, next: next}
	w.writerMu.Lock()
	w.writers[writer.processName] = append(w.writers[writer.processName], writer)
	w.writerMu.Unlock()
	return writer
}

type captureHandler struct {
	worker      *Worker
	processName string
	next        slog.Handler
	attrs       []scopedAttr
	groups      []string
}

type scopedAttr struct {
	groups []string
	attr   slog.Attr
}

func (h *captureHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level) || h.worker.captureEnabled(level)
}

func (h *captureHandler) Handle(ctx context.Context, record slog.Record) error {
	var nextErr error
	if h.next.Enabled(ctx, record.Level) {
		nextErr = h.next.Handle(ctx, record)
	}
	if h.worker.captureEnabled(record.Level) {
		h.worker.enqueueProcessLine(h.processName, sourceForLevel(record.Level), formatRecord(record, h.attrs, h.groups))
	}
	return nextErr
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copyOf := *h
	copyOf.next = h.next.WithAttrs(attrs)
	copyOf.attrs = append([]scopedAttr(nil), h.attrs...)
	for _, attr := range attrs {
		copyOf.attrs = append(copyOf.attrs, scopedAttr{
			groups: append([]string(nil), h.groups...),
			attr:   attr,
		})
	}
	return &copyOf
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	copyOf := *h
	copyOf.next = h.next.WithGroup(name)
	copyOf.groups = append(append([]string(nil), h.groups...), name)
	return &copyOf
}

func sourceForLevel(level slog.Level) LogSource {
	if level >= slog.LevelError {
		return Stderr
	}
	return Stdout
}

func formatRecord(record slog.Record, preset []scopedAttr, groups []string) string {
	var output strings.Builder
	output.WriteString(record.Message)
	for _, item := range preset {
		appendAttr(&output, item.groups, item.attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		appendAttr(&output, groups, attr)
		return true
	})
	return output.String()
}

func appendAttr(output *strings.Builder, groups []string, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	key := strings.Join(append(append([]string(nil), groups...), attr.Key), ".")
	if attr.Value.Kind() == slog.KindGroup {
		childGroups := groups
		if attr.Key != "" {
			childGroups = append(append([]string(nil), groups...), attr.Key)
		}
		for _, child := range attr.Value.Group() {
			appendAttr(output, childGroups, child)
		}
		return
	}
	output.WriteByte(' ')
	output.WriteString(key)
	output.WriteByte('=')
	if attr.Value.Kind() == slog.KindString {
		output.WriteString(strconv.Quote(attr.Value.String()))
		return
	}
	output.WriteString(fmt.Sprint(attr.Value.Any()))
}

type captureWriter struct {
	worker      *Worker
	processName string
	source      LogSource
	next        io.Writer
	mu          sync.Mutex
	partial     []byte
}

func (w *captureWriter) Write(data []byte) (int, error) {
	n, err := w.next.Write(data)
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.worker.processActive(w.processName) {
		w.partial = w.partial[:0]
		return n, err
	}
	w.partial = append(w.partial, data[:n]...)
	for {
		index := -1
		for i, value := range w.partial {
			if value == '\n' {
				index = i
				break
			}
		}
		if index < 0 {
			break
		}
		line := string(w.partial[:index])
		w.partial = append(w.partial[:0], w.partial[index+1:]...)
		w.worker.enqueueProcessLine(w.processName, w.source, strings.TrimSuffix(line, "\r"))
	}
	if len(w.partial) > maxPartialBytes {
		w.flushLocked()
	}
	return n, err
}

func (w *captureWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushLocked()
}

func (w *captureWriter) flushLocked() {
	if len(w.partial) == 0 {
		return
	}
	line := string(w.partial)
	w.partial = w.partial[:0]
	w.worker.enqueueProcessLine(w.processName, w.source, strings.TrimSuffix(line, "\r"))
}

func truncateRunes(value string, limit int) string {
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	runes := []rune(value)
	return string(runes[:limit]) + "…"
}
