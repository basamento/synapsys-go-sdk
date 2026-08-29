package synapsys

import (
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultConnectTimeout    = 2 * time.Second
	defaultRequestTimeout    = 5 * time.Second
)

type config struct {
	enabled           bool
	coreURL           string
	coreToken         string
	workerName        string
	host              string
	heartbeatInterval time.Duration
	failFast          bool
	connectTimeout    time.Duration
	requestTimeout    time.Duration
	captureConsole    bool
	logger            *slog.Logger
}

// Option configures a Worker. Options override environment variables.
type Option struct {
	apply func(*config) error
}

// WithEnabled completely disables or enables Synapsys integration.
func WithEnabled(enabled bool) Option { return option(func(c *config) { c.enabled = enabled }) }

// WithCoreURL sets the Synapsys Core base URL.
func WithCoreURL(value string) Option { return option(func(c *config) { c.coreURL = value }) }

// WithCoreToken sets the bearer token. Environment-based secrets are preferred.
func WithCoreToken(value string) Option { return option(func(c *config) { c.coreToken = value }) }

// WithWorkerName sets the stable identity reported across restarts.
func WithWorkerName(value string) Option { return option(func(c *config) { c.workerName = value }) }

// WithHost sets the host reported to Core.
func WithHost(value string) Option { return option(func(c *config) { c.host = value }) }

// WithHeartbeatInterval sets the positive whole-second heartbeat interval.
func WithHeartbeatInterval(value time.Duration) Option {
	return option(func(c *config) { c.heartbeatInterval = value })
}

// WithFailFast makes an unreachable Core fail Worker.Start.
func WithFailFast(value bool) Option { return option(func(c *config) { c.failFast = value }) }

// WithConnectTimeout bounds TCP connection establishment.
func WithConnectTimeout(value time.Duration) Option {
	return option(func(c *config) { c.connectTimeout = value })
}

// WithRequestTimeout bounds a complete Core request.
func WithRequestTimeout(value time.Duration) Option {
	return option(func(c *config) { c.requestTimeout = value })
}

// WithConsoleCapture enables or disables explicit process Logger and Writer capture.
// Go does not replace the process-global os.Stdout or os.Stderr streams.
func WithConsoleCapture(value bool) Option {
	return option(func(c *config) { c.captureConsole = value })
}

// WithLogger supplies the application's slog logger for SDK diagnostics and
// process-scoped logger passthrough.
func WithLogger(value *slog.Logger) Option {
	return Option{apply: func(c *config) error {
		if value == nil {
			return configError("logger must not be nil")
		}
		c.logger = value
		return nil
	}}
}

func option(apply func(*config)) Option {
	return Option{apply: func(c *config) error { apply(c); return nil }}
}

func resolveConfig(options []Option) (config, error) {
	c := config{
		enabled:           true,
		heartbeatInterval: defaultHeartbeatInterval,
		connectTimeout:    defaultConnectTimeout,
		requestTimeout:    defaultRequestTimeout,
		captureConsole:    true,
		logger:            slog.Default(),
		workerName:        defaultWorkerName(),
		host:              defaultHost(),
	}

	if raw, ok := os.LookupEnv("SYNAPSYS_ENABLED"); ok {
		value, err := parseBoolEnv("SYNAPSYS_ENABLED", raw)
		if err != nil {
			return config{}, err
		}
		c.enabled = value
	}
	if err := applyOptions(&c, options); err != nil {
		return config{}, err
	}
	if !c.enabled {
		return c, nil
	}

	if err := applyEnvironment(&c); err != nil {
		return config{}, err
	}
	if err := applyOptions(&c, options); err != nil {
		return config{}, err
	}
	return c, nil
}

func applyOptions(c *config, options []Option) error {
	for _, candidate := range options {
		if candidate.apply == nil {
			return configError("nil worker option")
		}
		if err := candidate.apply(c); err != nil {
			return err
		}
	}
	return nil
}

func applyEnvironment(c *config) error {
	setStringEnv("SYNAPSYS_CORE_URL", &c.coreURL)
	setStringEnv("SYNAPSYS_CORE_TOKEN", &c.coreToken)
	setStringEnv("SYNAPSYS_WORKER_NAME", &c.workerName)
	setStringEnv("SYNAPSYS_HOST", &c.host)

	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"SYNAPSYS_HEARTBEAT_INTERVAL", &c.heartbeatInterval},
		{"SYNAPSYS_CONNECT_TIMEOUT", &c.connectTimeout},
		{"SYNAPSYS_REQUEST_TIMEOUT", &c.requestTimeout},
	} {
		if raw, ok := os.LookupEnv(item.name); ok {
			value, err := parseDurationEnv(item.name, raw)
			if err != nil {
				return err
			}
			*item.target = value
		}
	}

	for _, item := range []struct {
		name   string
		target *bool
	}{
		{"SYNAPSYS_FAIL_FAST", &c.failFast},
		{"SYNAPSYS_CAPTURE_CONSOLE", &c.captureConsole},
	} {
		if raw, ok := os.LookupEnv(item.name); ok {
			value, err := parseBoolEnv(item.name, raw)
			if err != nil {
				return err
			}
			*item.target = value
		}
	}
	return nil
}

func setStringEnv(name string, target *string) {
	if value, ok := os.LookupEnv(name); ok {
		*target = strings.TrimSpace(value)
	}
}

func parseBoolEnv(name, raw string) (bool, error) {
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, configError("%s must be a boolean: %q", name, raw)
	}
	return value, nil
}

func parseDurationEnv(name, raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, configError("%s must include a duration and unit, for example 5s", name)
	}
	if _, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return 0, configError("%s must include a unit, for example 5s; unitless strings are not accepted", name)
	}
	value, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, configError("%s is not a valid duration: %q", name, raw)
	}
	return value, nil
}

func validateConfig(c config) ([]string, error) {
	if strings.TrimSpace(c.coreURL) == "" {
		return nil, configError("core URL is required; set SYNAPSYS_CORE_URL or use WithCoreURL")
	}
	parsed, err := url.Parse(c.coreURL)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, configError("core URL must be an absolute http or https URL: %q", c.coreURL)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, configError("core URL must not contain user information, a query, or a fragment")
	}
	if strings.TrimSpace(c.workerName) == "" {
		return nil, configError("worker name is required; set SYNAPSYS_WORKER_NAME or use WithWorkerName")
	}
	if c.heartbeatInterval <= 0 || c.heartbeatInterval%time.Second != 0 {
		return nil, configError("heartbeat interval must be a positive whole number of seconds")
	}
	if c.connectTimeout <= 0 {
		return nil, configError("connect timeout must be positive")
	}
	if c.requestTimeout <= 0 {
		return nil, configError("request timeout must be positive")
	}

	warnings := make([]string, 0, 2)
	if c.coreToken == "" {
		warnings = append(warnings, "Core token is not configured; Core will reject heartbeats")
	}
	if parsed.Scheme == "http" && c.coreToken != "" && !isLoopback(parsed.Hostname()) {
		warnings = append(warnings, "Core token will be sent over plaintext HTTP")
	}
	return warnings, nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func defaultWorkerName() string {
	if len(os.Args) > 0 {
		name := strings.TrimSuffix(filepath.Base(os.Args[0]), filepath.Ext(os.Args[0]))
		if name != "" {
			return name
		}
	}
	return ""
}

func defaultHost() string {
	value, err := os.Hostname()
	if err != nil {
		return ""
	}
	return value
}

func (c config) settings() Settings {
	return Settings{
		Enabled: c.enabled, CoreURL: c.coreURL, WorkerName: c.workerName, Host: c.host,
		HeartbeatInterval: c.heartbeatInterval, FailFast: c.failFast,
		ConnectTimeout: c.connectTimeout, RequestTimeout: c.requestTimeout,
		CaptureConsole: c.captureConsole,
	}
}

func formatDuration(value time.Duration) string { return fmt.Sprintf("%s", value) }
