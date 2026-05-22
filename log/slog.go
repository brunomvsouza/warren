package log

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/brunomvsouza/warren/internal/redact"
)

// slogLogger wraps a *slog.Logger, routing every message through
// internal/redact.URI before emission.
type slogLogger struct{ l *slog.Logger }

// NewSlog returns a Logger that delegates to the provided *slog.Logger.
// Every message is run through internal/redact.URI to strip AMQP credentials
// before the underlying slog handler sees it.
func NewSlog(l *slog.Logger) Logger { return slogLogger{l: l} }

func (s slogLogger) Debug(msg string) {
	s.l.Log(context.Background(), slog.LevelDebug, redact.URI(msg))
}

func (s slogLogger) Info(msg string) {
	s.l.Log(context.Background(), slog.LevelInfo, redact.URI(msg))
}

func (s slogLogger) Warning(msg string) {
	s.l.Log(context.Background(), slog.LevelWarn, redact.URI(msg))
}

func (s slogLogger) Error(msg string) {
	s.l.Log(context.Background(), slog.LevelError, redact.URI(msg))
}

func (s slogLogger) Debugf(format string, args ...any) {
	s.l.Log(context.Background(), slog.LevelDebug, redact.URI(fmt.Sprintf(format, args...)))
}

func (s slogLogger) Infof(format string, args ...any) {
	s.l.Log(context.Background(), slog.LevelInfo, redact.URI(fmt.Sprintf(format, args...)))
}

func (s slogLogger) Warningf(format string, args ...any) {
	s.l.Log(context.Background(), slog.LevelWarn, redact.URI(fmt.Sprintf(format, args...)))
}

func (s slogLogger) Errorf(format string, args ...any) {
	s.l.Log(context.Background(), slog.LevelError, redact.URI(fmt.Sprintf(format, args...)))
}
