# Synapsys Go SDK

Framework-neutral Go integration for registering business processes with Synapsys
Core and applying remote lifecycle control.

The SDK does not schedule functions. Core decides when a process starts or stops:

- **Endless** processes are listeners, consumers, or loops that remain active.
- **Progressive** processes perform finite work once and return naturally.

Everything uses one outbound heartbeat request. The worker never opens a port.

## Requirements

- Go 1.22 or later
- A reachable Synapsys Core instance
- A worker token created in Core

## Install

```bash
go get github.com/basamento/synapsys-go-sdk@v0.1.0
```

## Configure and start

Set the two deployment-specific values:

```bash
export SYNAPSYS_CORE_URL=https://core.example.com
export SYNAPSYS_CORE_TOKEN=syn_your_worker_token
```

Then register processes and start one explicit worker:

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/basamento/synapsys-go-sdk"
)

func main() {
	worker, err := synapsys.New(
		synapsys.WithWorkerName("billing-api"),
	)
	if err != nil {
		log.Fatal(err)
	}

	err = worker.Register(
		synapsys.EndlessContext("invoice-poller", func(ctx context.Context) error {
			for {
				if err := pollInvoices(ctx); err != nil {
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
			}
		}),
		synapsys.Progressive("monthly-report", buildMonthlyReport),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := worker.Start(); err != nil {
		log.Fatal(err)
	}

	// Integrate this with the application's normal shutdown path.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = worker.Stop(ctx)
	}()
	select {}
}
```

`Start` succeeds when Core is unavailable unless `fail-fast` is enabled. The
heartbeat continues retrying and never stops running business work.

## Process declarations

Every body returns an error. A non-nil error or a recovered panic produces the
`failed` state. A body can be an ordinary function or method value from a package
that has no Synapsys dependency.

### Endless with cooperative cancellation

```go
synapsys.EndlessContext("queue-consumer", func(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message := <-messages:
			consume(message)
		}
	}
})
```

### Endless with an existing stop mechanism

```go
synapsys.Endless(
	"legacy-listener",
	listener.Serve,
	synapsys.WithOnStop(listener.Close),
)
```

The hook runs when Stop is accepted and can close a socket or listener to unblock
the body.

### Progressive finite work

```go
synapsys.Progressive("monthly-report", reports.Build)
```

### Progressive with in-flight cancellation

```go
synapsys.ProgressiveContext("bulk-import", func(ctx context.Context) error {
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := importRow(ctx, row); err != nil {
			return err
		}
	}
	return nil
})
```

Cancellation is cooperative. Go cannot terminate a goroutine. If a body ignores
its context and has no external stop mechanism, it remains `stopping` until it
returns. A panic in a goroutine created by the business function is also outside
the SDK's recovery boundary and can terminate the application.

## Process logs

Go has no supported goroutine-local mechanism that can attribute global
`os.Stdout` and `os.Stderr` writes to concurrent processes. The SDK therefore does
not replace those streams. Use a process-scoped logger or writer.

Structured logging with `slog`:

```go
processLog := worker.Logger("invoice-poller")

synapsys.EndlessContext("invoice-poller", func(ctx context.Context) error {
	processLog.Info("poll complete", "invoices", 4)
	return runPoller(ctx)
})
```

The logger passes every record to the application's configured `slog.Logger` and
also sends Info-or-higher records produced during an active execution to Core.

For the standard `log` package or third-party loggers accepting `io.Writer`:

```go
processOutput := worker.Writer("invoice-poller", synapsys.Stdout, os.Stdout)
processLog := log.New(processOutput, "", log.LstdFlags)
```

The writer is a tee: application output remains intact. Output outside an active
execution is written normally but is not sent to Core. Console capture transmits
the resulting process output to Core; disable it if those messages may contain
sensitive data.

## Configuration

Options override environment variables. Durations in environment variables must
carry units accepted by `time.ParseDuration`; a unitless string is rejected.

| Environment variable | Option | Default |
| --- | --- | --- |
| `SYNAPSYS_CORE_URL` | `WithCoreURL` | required |
| `SYNAPSYS_CORE_TOKEN` | `WithCoreToken` | none; Core normally rejects it |
| `SYNAPSYS_WORKER_NAME` | `WithWorkerName` | executable name |
| `SYNAPSYS_ENABLED` | `WithEnabled` | `true` |
| `SYNAPSYS_HOST` | `WithHost` | machine hostname |
| `SYNAPSYS_HEARTBEAT_INTERVAL` | `WithHeartbeatInterval` | `5s` |
| `SYNAPSYS_FAIL_FAST` | `WithFailFast` | `false` |
| `SYNAPSYS_CONNECT_TIMEOUT` | `WithConnectTimeout` | `2s` |
| `SYNAPSYS_REQUEST_TIMEOUT` | `WithRequestTimeout` | `5s` |
| `SYNAPSYS_CAPTURE_CONSOLE` | `WithConsoleCapture` | `true` |
| — | `WithLogger` | `slog.Default()` |

`enabled=false` is a complete runtime no-op: no validation, network calls,
heartbeat goroutine, or capture. Process declaration errors are still reported.

The SDK warns when a token is sent over non-loopback plaintext HTTP. Redirects are
never followed, so a bearer token cannot be forwarded to another host.

## Shutdown

Use a bounded context from the application's shutdown path:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := worker.Stop(ctx); err != nil {
	log.Printf("Synapsys processes exceeded shutdown deadline: %v", err)
}
```

The SDK stops normal heartbeats, requests cooperative process stops, waits up to
the deadline, and makes one best-effort final heartbeat carrying final states and
cleanup logs. Commands in the final response are ignored because shutdown must not
start new work.

## Development

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
```

The module intentionally has no runtime dependencies.

## API stability

Releases follow semantic versioning. While the module is `v0.x`, public API
changes may occur in a minor release and are called out in release notes. After
`v1.0.0`, incompatible public API or wire-behavior changes require a new major
module path; compatible additions and fixes use minor and patch releases.

## License

Apache License 2.0. See [LICENSE](LICENSE).

Copyright 2026 Julian Marzoli (Basamento).
