package log

// noopLogger is a Logger that discards every message.
type noopLogger struct{}

// NewNoOp returns a Logger that silently discards all messages.
// It is the default logger used by Connection when no WithLogger option
// is provided.
func NewNoOp() Logger { return noopLogger{} }

func (noopLogger) Debug(string)            {}
func (noopLogger) Info(string)             {}
func (noopLogger) Warning(string)          {}
func (noopLogger) Error(string)            {}
func (noopLogger) Debugf(string, ...any)   {}
func (noopLogger) Infof(string, ...any)    {}
func (noopLogger) Warningf(string, ...any) {}
func (noopLogger) Errorf(string, ...any)   {}
