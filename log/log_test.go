package log_test

import (
	"bytes"
	stdlog "log"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	amqplog "github.com/brunomvsouza/amqp/log"
)

// sensitiveURI is used across all redaction tests. It contains a
// percent-encoded password so both raw ("p@ss") and encoded ("p%40ss")
// forms must be absent from the output.
const sensitiveURI = "amqp://user:p%40ss@h:5672/v"

// — NoOp adapter ——————————————————————————————————————————————————————————————

func TestNoOp_doesNotPanic(t *testing.T) {
	l := amqplog.NewNoOp()
	require.NotNil(t, l)
	l.Debug("d")
	l.Info("i")
	l.Warning("w")
	l.Error("e")
	l.Debugf("df %s", "x")
	l.Infof("if %s", "x")
	l.Warningf("wf %s", "x")
	l.Errorf("ef %s", "x")
}

// — Slog adapter ——————————————————————————————————————————————————————————————

func newSlogLogger(t *testing.T, buf *bytes.Buffer) amqplog.Logger {
	t.Helper()
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return amqplog.NewSlog(slog.New(h))
}

func TestSlog_roundTrip(t *testing.T) {
	tests := []struct {
		name    string
		call    func(l amqplog.Logger)
		wantKey string // slog text key to look for
		wantMsg string
	}{
		{"Debug", func(l amqplog.Logger) { l.Debug("debug msg") }, "level=DEBUG", "debug msg"},
		{"Info", func(l amqplog.Logger) { l.Info("info msg") }, "level=INFO", "info msg"},
		{"Warning", func(l amqplog.Logger) { l.Warning("warn msg") }, "level=WARN", "warn msg"},
		{"Error", func(l amqplog.Logger) { l.Error("error msg") }, "level=ERROR", "error msg"},
		{"Debugf", func(l amqplog.Logger) { l.Debugf("debug %s", "fmt") }, "level=DEBUG", "debug fmt"},
		{"Infof", func(l amqplog.Logger) { l.Infof("info %s", "fmt") }, "level=INFO", "info fmt"},
		{"Warningf", func(l amqplog.Logger) { l.Warningf("warn %s", "fmt") }, "level=WARN", "warn fmt"},
		{"Errorf", func(l amqplog.Logger) { l.Errorf("error %s", "fmt") }, "level=ERROR", "error fmt"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := newSlogLogger(t, &buf)
			tc.call(l)
			out := buf.String()
			assert.Contains(t, out, tc.wantKey, "level not found in output")
			assert.Contains(t, out, tc.wantMsg, "message not found in output")
		})
	}
}

func TestSlog_redactsAMQPCredentials(t *testing.T) {
	var buf bytes.Buffer
	l := newSlogLogger(t, &buf)

	l.Info(sensitiveURI)

	out := buf.String()
	assert.Contains(t, out, "amqp://***@h:5672/v", "redacted URI must be present")
	assert.NotContains(t, out, "p@ss", "decoded password must not appear")
	assert.NotContains(t, out, "p%40ss", "encoded password must not appear")
}

func TestSlog_redactsAMQPCredentials_amqps(t *testing.T) {
	var buf bytes.Buffer
	l := newSlogLogger(t, &buf)

	l.Errorf("connecting to %s failed", "amqps://admin:s3cr3t@broker:5671/prod")

	out := buf.String()
	assert.Contains(t, out, "amqps://***@broker:5671/prod")
	assert.NotContains(t, out, "s3cr3t")
}

// — Std adapter ———————————————————————————————————————————————————————————————

func newStdLogger(t *testing.T, buf *bytes.Buffer) amqplog.Logger {
	t.Helper()
	return amqplog.NewStd(stdlog.New(buf, "", 0))
}

func TestStd_roundTrip(t *testing.T) {
	tests := []struct {
		name   string
		call   func(l amqplog.Logger)
		wantIn string
	}{
		{"Debug", func(l amqplog.Logger) { l.Debug("debug msg") }, "debug msg"},
		{"Info", func(l amqplog.Logger) { l.Info("info msg") }, "info msg"},
		{"Warning", func(l amqplog.Logger) { l.Warning("warn msg") }, "warn msg"},
		{"Error", func(l amqplog.Logger) { l.Error("error msg") }, "error msg"},
		{"Debugf", func(l amqplog.Logger) { l.Debugf("debug %s", "fmt") }, "debug fmt"},
		{"Infof", func(l amqplog.Logger) { l.Infof("info %s", "fmt") }, "info fmt"},
		{"Warningf", func(l amqplog.Logger) { l.Warningf("warn %s", "fmt") }, "warn fmt"},
		{"Errorf", func(l amqplog.Logger) { l.Errorf("error %s", "fmt") }, "error fmt"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := newStdLogger(t, &buf)
			tc.call(l)
			out := buf.String()
			assert.True(t, strings.Contains(out, tc.wantIn),
				"message %q not found in output %q", tc.wantIn, out)
		})
	}
}

func TestStd_roundTrip_includesLevelPrefix(t *testing.T) {
	tests := []struct {
		name   string
		call   func(l amqplog.Logger)
		prefix string
	}{
		{"Debug", func(l amqplog.Logger) { l.Debug("x") }, "DEBUG"},
		{"Info", func(l amqplog.Logger) { l.Info("x") }, "INFO"},
		{"Warning", func(l amqplog.Logger) { l.Warning("x") }, "WARN"},
		{"Error", func(l amqplog.Logger) { l.Error("x") }, "ERROR"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := newStdLogger(t, &buf)
			tc.call(l)
			assert.Contains(t, buf.String(), tc.prefix)
		})
	}
}

func TestStd_redactsAMQPCredentials(t *testing.T) {
	var buf bytes.Buffer
	l := newStdLogger(t, &buf)

	l.Info(sensitiveURI)

	out := buf.String()
	assert.Contains(t, out, "amqp://***@h:5672/v", "redacted URI must be present")
	assert.NotContains(t, out, "p@ss", "decoded password must not appear")
	assert.NotContains(t, out, "p%40ss", "encoded password must not appear")
}

func TestStd_redactsAMQPCredentials_amqps(t *testing.T) {
	var buf bytes.Buffer
	l := newStdLogger(t, &buf)

	l.Warningf("retry connecting to %s", "amqps://admin:s3cr3t@broker:5671/prod")

	out := buf.String()
	assert.Contains(t, out, "amqps://***@broker:5671/prod")
	assert.NotContains(t, out, "s3cr3t")
}
