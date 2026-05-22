package log

import (
	"fmt"
	stdlog "log"

	"github.com/brunomvsouza/warren/internal/redact"
)

// stdLogger wraps a *log.Logger, routing every message through
// internal/redact.URI before emission.
type stdLogger struct{ l *stdlog.Logger }

// NewStd returns a Logger that delegates to the provided *log.Logger from the
// standard library. Every message is run through internal/redact.URI to strip
// AMQP credentials before printing. A level prefix (DEBUG / INFO / WARN /
// ERROR) is prepended to each line.
func NewStd(l *stdlog.Logger) Logger { return stdLogger{l: l} }

func (s stdLogger) Debug(msg string) {
	s.l.Print("DEBUG " + redact.URI(msg))
}

func (s stdLogger) Info(msg string) {
	s.l.Print("INFO " + redact.URI(msg))
}

func (s stdLogger) Warning(msg string) {
	s.l.Print("WARN " + redact.URI(msg))
}

func (s stdLogger) Error(msg string) {
	s.l.Print("ERROR " + redact.URI(msg))
}

func (s stdLogger) Debugf(format string, args ...any) {
	s.l.Print("DEBUG " + redact.URI(fmt.Sprintf(format, args...)))
}

func (s stdLogger) Infof(format string, args ...any) {
	s.l.Print("INFO " + redact.URI(fmt.Sprintf(format, args...)))
}

func (s stdLogger) Warningf(format string, args ...any) {
	s.l.Print("WARN " + redact.URI(fmt.Sprintf(format, args...)))
}

func (s stdLogger) Errorf(format string, args ...any) {
	s.l.Print("ERROR " + redact.URI(fmt.Sprintf(format, args...)))
}
