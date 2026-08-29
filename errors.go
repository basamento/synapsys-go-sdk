package synapsys

import "fmt"

// ConfigError reports invalid SDK configuration or process declarations.
type ConfigError struct{ Message string }

func (e *ConfigError) Error() string { return "[Synapsys] " + e.Message }

func configError(format string, args ...any) error {
	return &ConfigError{Message: fmt.Sprintf(format, args...)}
}

// StartupError reports a fail-fast startup failure.
type StartupError struct{ Cause error }

func (e *StartupError) Error() string {
	return "[Synapsys] startup failed: " + e.Cause.Error()
}

func (e *StartupError) Unwrap() error { return e.Cause }
