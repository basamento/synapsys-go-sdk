package synapsys

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
)

// Process is a declaration registered with a Worker.
// Its fields are intentionally private so invalid combinations cannot be built
// without using Endless, EndlessContext, Progressive, or ProgressiveContext.
type Process struct {
	name    string
	typeOf  ProcessType
	run     func(context.Context) error
	onStop  func() error
	invalid error
}

// ProcessOption configures one process declaration.
type ProcessOption struct {
	apply func(*Process) error
}

// WithOnStop installs an optional cleanup hook. Endless hooks run as soon as a
// stop is accepted so they can unblock legacy listeners. Progressive hooks run
// after the body returns from a requested stop.
func WithOnStop(hook func() error) ProcessOption {
	return ProcessOption{apply: func(p *Process) error {
		if hook == nil {
			return configError("on-stop hook for process %q must not be nil", p.name)
		}
		p.onStop = hook
		return nil
	}}
}

// Endless declares long-lived logic with its own stop mechanism.
func Endless(name string, run func() error, options ...ProcessOption) Process {
	if run == nil {
		return Process{name: name, typeOf: TypeEndless, invalid: configError("endless process %q has a nil body", name)}
	}
	return newProcess(name, TypeEndless, func(context.Context) error { return run() }, options)
}

// EndlessContext declares long-lived logic that observes cooperative cancellation.
func EndlessContext(name string, run func(context.Context) error, options ...ProcessOption) Process {
	if run == nil {
		return Process{name: name, typeOf: TypeEndless, invalid: configError("endless process %q has a nil body", name)}
	}
	return newProcess(name, TypeEndless, run, options)
}

// Progressive declares finite work that does not require in-flight cancellation.
func Progressive(name string, run func() error, options ...ProcessOption) Process {
	if run == nil {
		return Process{name: name, typeOf: TypeProgressive, invalid: configError("progressive process %q has a nil body", name)}
	}
	return newProcess(name, TypeProgressive, func(context.Context) error { return run() }, options)
}

// ProgressiveContext declares finite work that observes cooperative cancellation.
func ProgressiveContext(name string, run func(context.Context) error, options ...ProcessOption) Process {
	if run == nil {
		return Process{name: name, typeOf: TypeProgressive, invalid: configError("progressive process %q has a nil body", name)}
	}
	return newProcess(name, TypeProgressive, run, options)
}

func newProcess(name string, typeOf ProcessType, run func(context.Context) error, options []ProcessOption) Process {
	p := Process{name: strings.TrimSpace(name), typeOf: typeOf, run: run}
	for _, candidate := range options {
		if candidate.apply == nil {
			p.invalid = configError("nil option for process %q", name)
			break
		}
		if err := candidate.apply(&p); err != nil {
			p.invalid = err
			break
		}
	}
	return p
}

type managedProcess struct {
	owner *Worker
	decl  Process

	mu             sync.Mutex
	state          ProcessState
	ackToken       int64
	runID          *int64
	sequence       int64
	cancel         context.CancelCauseFunc
	done           chan struct{}
	stopRequested  bool
	finishing      bool
	cleanupStarted bool
	cleanupDone    chan struct{}
}

func newManagedProcess(owner *Worker, declaration Process) *managedProcess {
	return &managedProcess{owner: owner, decl: declaration, state: StateIdle}
}

func (p *managedProcess) status() ProcessStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ProcessStatus{
		Name: p.decl.name, Type: p.decl.typeOf, State: p.state,
		AckToken: p.ackToken, RunID: cloneInt64(p.runID),
	}
}

func (p *managedProcess) snapshot() heartbeatProcess {
	status := p.status()
	return heartbeatProcess{
		Name: status.Name, Type: status.Type, CurrentState: status.State,
		AckToken: status.AckToken, RunID: status.RunID,
	}
}

func (p *managedProcess) syncRunID(runID *int64) {
	p.mu.Lock()
	p.runID = cloneInt64(runID)
	p.mu.Unlock()
}

func (p *managedProcess) acknowledged() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ackToken
}

func (p *managedProcess) acknowledge(token int64) {
	p.mu.Lock()
	if token > p.ackToken {
		p.ackToken = token
	}
	p.mu.Unlock()
}

// start returns true when the desired running state is accepted. A new start
// received while stopping remains unacknowledged so Core redelivers it after the
// previous execution reaches idle.
func (p *managedProcess) start(runID *int64) bool {
	p.mu.Lock()
	if p.finishing {
		p.mu.Unlock()
		return false
	}
	if p.state == StateRunning {
		p.mu.Unlock()
		return true
	}
	if p.state == StateStopping {
		p.mu.Unlock()
		return false
	}
	if p.done != nil {
		select {
		case <-p.done:
		default:
			p.mu.Unlock()
			return false
		}
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan struct{})
	p.state = StateRunning
	p.runID = cloneInt64(runID)
	p.sequence = 0
	p.cancel = cancel
	p.done = done
	p.stopRequested = false
	p.finishing = false
	p.cleanupStarted = false
	p.cleanupDone = nil
	p.mu.Unlock()

	go p.execute(ctx, done)
	return true
}

func (p *managedProcess) stop() bool {
	p.mu.Lock()
	if p.finishing {
		p.mu.Unlock()
		return true
	}
	if p.state == StateIdle || p.state == StateFailed {
		p.mu.Unlock()
		return true
	}
	if p.state == StateStopping {
		p.mu.Unlock()
		return true
	}
	if p.state != StateRunning {
		p.mu.Unlock()
		return false
	}
	p.state = StateStopping
	p.stopRequested = true
	cancel := p.cancel
	startCleanup := p.decl.typeOf == TypeEndless && p.decl.onStop != nil && !p.cleanupStarted
	var cleanupDone chan struct{}
	if startCleanup {
		p.cleanupStarted = true
		cleanupDone = make(chan struct{})
		p.cleanupDone = cleanupDone
	}
	p.mu.Unlock()

	if cancel != nil {
		cancel(context.Canceled)
	}
	if startCleanup {
		go func() {
			defer close(cleanupDone)
			p.invokeCleanup()
		}()
	}
	return true
}

func (p *managedProcess) execute(ctx context.Context, done chan struct{}) {
	var runErr error
	var panicValue any
	var panicStack []byte

	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				panicValue = recovered
				panicStack = debug.Stack()
			}
		}()
		runErr = p.decl.run(ctx)
	}()

	p.mu.Lock()
	p.finishing = true
	stopped := p.stopRequested
	cleanupDone := p.cleanupDone
	needsProgressiveCleanup := stopped && p.decl.typeOf == TypeProgressive && p.decl.onStop != nil && !p.cleanupStarted
	if needsProgressiveCleanup {
		p.cleanupStarted = true
	}
	p.mu.Unlock()

	if needsProgressiveCleanup {
		p.invokeCleanup()
	}
	if cleanupDone != nil {
		<-cleanupDone
	}

	failed := false
	if panicValue != nil {
		failed = true
		p.owner.captureException(p.decl.name, fmt.Sprintf("panic: %v\n%s", panicValue, panicStack))
		p.owner.logError("Process panicked", "process", p.decl.name, "type", p.decl.typeOf, "panic", panicValue)
	} else if runErr != nil && !p.isCleanCancellation(runErr, stopped) {
		failed = true
		p.owner.captureException(p.decl.name, runErr.Error())
		p.owner.logError("Process failed", "process", p.decl.name, "type", p.decl.typeOf, "error", runErr)
	}

	// Flush partial lines while the run id and active state are still available.
	p.owner.flushWriters(p.decl.name)

	p.mu.Lock()
	if failed {
		p.state = StateFailed
	} else {
		p.state = StateIdle
	}
	p.cancel = nil
	p.finishing = false
	if p.done == done {
		close(done)
	}
	p.mu.Unlock()
}

func (p *managedProcess) isCleanCancellation(err error, stopped bool) bool {
	if !stopped {
		return false
	}
	if p.decl.typeOf == TypeEndless {
		// Closing a listener commonly returns a non-context error. Once an Endless
		// stop was requested, returning is the desired outcome.
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (p *managedProcess) invokeCleanup() {
	defer func() {
		if recovered := recover(); recovered != nil {
			stack := debug.Stack()
			p.owner.captureException(p.decl.name, fmt.Sprintf("panic in on-stop hook: %v\n%s", recovered, stack))
			p.owner.logError("on-stop hook panicked", "process", p.decl.name, "panic", recovered)
		}
	}()
	if err := p.decl.onStop(); err != nil {
		p.owner.captureException(p.decl.name, "on-stop hook: "+err.Error())
		p.owner.logError("on-stop hook failed", "process", p.decl.name, "error", err)
	}
}

func (p *managedProcess) settle(ctx context.Context) bool {
	p.mu.Lock()
	done := p.done
	p.mu.Unlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *managedProcess) nextLogContext() (*int64, int64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if (p.state != StateRunning && p.state != StateStopping) || p.runID == nil {
		return nil, 0, false
	}
	p.sequence++
	return cloneInt64(p.runID), p.sequence, true
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copyOf := *value
	return &copyOf
}
