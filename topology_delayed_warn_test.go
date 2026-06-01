package warren

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingLogger captures warning lines for assertions.
type recordingLogger struct {
	warnings []string
}

func (l *recordingLogger) Debug(string)          {}
func (l *recordingLogger) Info(string)           {}
func (l *recordingLogger) Warning(msg string)    { l.warnings = append(l.warnings, msg) }
func (l *recordingLogger) Error(string)          {}
func (l *recordingLogger) Debugf(string, ...any) {}
func (l *recordingLogger) Infof(string, ...any)  {}
func (l *recordingLogger) Warningf(f string, a ...any) {
	l.warnings = append(l.warnings, fmt.Sprintf(f, a...))
}
func (l *recordingLogger) Errorf(string, ...any) {}

// TestWarnDelayedExchanges_durabilityWarning asserts that declaring an
// ExchangeDelayed exchange emits exactly one durability warning per delayed
// exchange, and that the warning references node-local / non-durable loss
// (SPEC §6.5 / T57). Non-delayed exchanges produce no warning.
func TestWarnDelayedExchanges_durabilityWarning(t *testing.T) {
	t.Run("one warning per delayed exchange", func(t *testing.T) {
		l := &recordingLogger{}
		exchanges := []Exchange{
			{Name: "events", Kind: ExchangeTopic},
			{Name: "scheduled", Kind: ExchangeDelayed, Args: map[string]any{"x-delayed-type": "topic"}},
			{Name: "reminders", Kind: ExchangeDelayed, Args: map[string]any{"x-delayed-type": "direct"}},
		}

		warnDelayedExchanges(l, exchanges)

		require.Len(t, l.warnings, 2)
		for _, w := range l.warnings {
			assert.Contains(t, strings.ToLower(w), "node-local")
			assert.Contains(t, strings.ToLower(w), "lost")
		}
		assert.Contains(t, l.warnings[0], "scheduled")
		assert.Contains(t, l.warnings[1], "reminders")
	})

	t.Run("no delayed exchange means no warning", func(t *testing.T) {
		l := &recordingLogger{}
		warnDelayedExchanges(l, []Exchange{
			{Name: "events", Kind: ExchangeTopic},
			{Name: "direct", Kind: ExchangeDirect},
		})
		assert.Empty(t, l.warnings)
	})
}
