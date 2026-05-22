// Package log provides the Logger interface and three adapters (NoOp, Slog,
// Std) used by the warren library for structured log emission.
//
// All adapters route every string through [internal/redact.URI] before
// emission, so AMQP URIs containing credentials never appear in log output.
package log

// Logger is the log-emission interface used by Connection, Publisher,
// Consumer, and Topology. The library ships three implementations:
// [NewNoOp], [NewSlog], and [NewStd].
//
// Implementations must be safe for concurrent use.
type Logger interface {
	// Debug logs a message at debug level.
	Debug(msg string)
	// Info logs a message at info level.
	Info(msg string)
	// Warning logs a message at warning level.
	Warning(msg string)
	// Error logs a message at error level.
	Error(msg string)

	// Debugf logs a formatted message at debug level.
	Debugf(format string, args ...any)
	// Infof logs a formatted message at info level.
	Infof(format string, args ...any)
	// Warningf logs a formatted message at warning level.
	Warningf(format string, args ...any)
	// Errorf logs a formatted message at error level.
	Errorf(format string, args ...any)
}
