package warren

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/metrics"
)

// dropSpyMetrics counts consumer_drop_no_dlx_total increments (T65).
type dropSpyMetrics struct {
	metrics.NoOpConsumerMetrics
	drops atomic.Int64
}

func (m *dropSpyMetrics) RecordConsumerDropNoDLX(_ string) { m.drops.Add(1) }

// T65: a Consumer with MaxRedeliveries>0 and a wired Topology lacking a DLX for
// the source queue warns at Build (parity with the Replier check). AllowMissingDLX
// and a present DeadLetter both suppress the warning.
func TestConsumer_missingDLX_warnsAtBuild(t *testing.T) {
	noDLX := &Topology{Queues: []Queue{{Name: "orders", Durable: true}}}
	withDLX := &Topology{
		Queues:      []Queue{{Name: "orders", Durable: true}},
		DeadLetters: []DeadLetter{{Source: "orders"}},
	}

	hasMissingDLXWarning := func(rec *recordingLogger) bool {
		for _, w := range rec.warnings {
			if strings.Contains(w, "no DeadLetter") {
				return true
			}
		}
		return false
	}

	t.Run("warns when MaxRedeliveries>0 and no DLX", func(t *testing.T) {
		conn := newFakeConsumerConn(t)
		rec := &recordingLogger{}
		conn.opts.logger = rec
		c, err := ConsumerFor[string](conn).Queue("orders").MaxRedeliveries(3).Topology(noDLX).Build()
		require.NoError(t, err)
		assert.True(t, hasMissingDLXWarning(rec), "must warn about the missing DeadLetter")
		assert.False(t, c.knownHasDLX)
	})

	t.Run("no warning with a DeadLetter wired", func(t *testing.T) {
		conn := newFakeConsumerConn(t)
		rec := &recordingLogger{}
		conn.opts.logger = rec
		c, err := ConsumerFor[string](conn).Queue("orders").MaxRedeliveries(3).Topology(withDLX).Build()
		require.NoError(t, err)
		assert.False(t, hasMissingDLXWarning(rec))
		assert.True(t, c.knownHasDLX, "knownHasDLX must be true when a DeadLetter is wired")
	})

	t.Run("AllowMissingDLX suppresses the warning", func(t *testing.T) {
		conn := newFakeConsumerConn(t)
		rec := &recordingLogger{}
		conn.opts.logger = rec
		_, err := ConsumerFor[string](conn).Queue("orders").MaxRedeliveries(3).Topology(noDLX).AllowMissingDLX().Build()
		require.NoError(t, err)
		assert.False(t, hasMissingDLXWarning(rec))
	})

	t.Run("no warning when MaxRedeliveries==0", func(t *testing.T) {
		conn := newFakeConsumerConn(t)
		rec := &recordingLogger{}
		conn.opts.logger = rec
		_, err := ConsumerFor[string](conn).Queue("orders").Topology(noDLX).Build()
		require.NoError(t, err)
		assert.False(t, hasMissingDLXWarning(rec))
	})
}

// TestConsumer_recordPoisonDropNoDLX verifies the drop counter increments only
// when no DLX is known (parity with replier_drop_no_dlx_total).
func TestConsumer_recordPoisonDropNoDLX(t *testing.T) {
	t.Run("increments when no DLX known", func(t *testing.T) {
		cm := &dropSpyMetrics{}
		c := &Consumer[string]{queue: "orders", cm: cm, knownHasDLX: false}
		c.recordPoisonDropNoDLX()
		assert.Equal(t, int64(1), cm.drops.Load())
	})

	t.Run("no increment when DLX known", func(t *testing.T) {
		cm := &dropSpyMetrics{}
		c := &Consumer[string]{queue: "orders", cm: cm, knownHasDLX: true}
		c.recordPoisonDropNoDLX()
		assert.Equal(t, int64(0), cm.drops.Load())
	})
}
