package warren

import (
	"errors"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/metrics"
)

// newCountingInspectConn returns a managedConn whose chanFactory both serves the
// passive-declare probe and counts how many times a channel was opened, so a test
// can assert whether the broker round-trip ran.
func newCountingInspectConn(ch *fakeInspectChannel, opened *int) *managedConn {
	conn := &Connection{}
	mc := &managedConn{opts: &conn.opts}
	mc.chanFactory = func() (topologyChannel, error) { *opened++; return ch, nil }
	return mc
}

func TestClassifyCancel_SkipsProbeWhenClassUnobserved(t *testing.T) {
	// OnCancel is set (so the warning log never references the class) and the metrics
	// sink is the NoOp default (so RecordCancelled discards it): no observer needs the
	// class, so classifyCancel must skip the broker round-trip entirely.
	var opened int
	mc := newCountingInspectConn(&fakeInspectChannel{}, &opened)

	reason := classifyCancel(mc, "q", func(string) {}, metrics.NoOpConsumerMetrics{})

	assert.Equal(t, cancelReasonUnknown, reason)
	assert.Zero(t, opened, "no broker channel must be opened when the class is unobserved")
}

func TestClassifyCancel_ProbesWhenLogNeedsClass(t *testing.T) {
	// OnCancel is unset, so the fallback warning log prints the class — it must probe
	// even though the metrics sink is the NoOp default.
	var opened int
	mc := newCountingInspectConn(&fakeInspectChannel{}, &opened)

	reason := classifyCancel(mc, "q", nil, metrics.NoOpConsumerMetrics{})

	assert.Equal(t, cancelReasonExclusiveRevoked, reason)
	assert.Equal(t, 1, opened, "must probe when OnCancel is unset (the log needs the class)")
}

func TestClassifyCancel_ProbesWhenMetricsObserveClass(t *testing.T) {
	// A real (non-NoOp) metrics sink records the class, so classifyCancel must probe
	// even though OnCancel is set.
	var opened int
	mc := newCountingInspectConn(&fakeInspectChannel{}, &opened)

	reason := classifyCancel(mc, "q", func(string) {}, &countingConsumerMetrics{})

	assert.Equal(t, cancelReasonExclusiveRevoked, reason)
	assert.Equal(t, 1, opened, "must probe when a non-NoOp metrics sink records the class")
}

// fakeInspectChannel is a topologyChannel that also answers QueueDeclarePassive,
// used to exercise classifyBrokerCancel without a live broker.
type fakeInspectChannel struct {
	passiveErr error
	closed     bool
}

func (f *fakeInspectChannel) ExchangeDeclare(_, _ string, _, _, _, _ bool, _ amqp091.Table) error {
	return nil
}

func (f *fakeInspectChannel) QueueDeclare(_ string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	return amqp091.Queue{}, nil
}

func (f *fakeInspectChannel) QueueDeclarePassive(name string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	if f.passiveErr != nil {
		return amqp091.Queue{}, f.passiveErr
	}
	return amqp091.Queue{Name: name}, nil
}

func (f *fakeInspectChannel) QueueBind(_, _, _ string, _ bool, _ amqp091.Table) error { return nil }

func (f *fakeInspectChannel) Close() error {
	f.closed = true
	return nil
}

func newInspectConn(t *testing.T, ch *fakeInspectChannel) *managedConn {
	t.Helper()
	conn := &Connection{}
	conn.opts.logger = nil // unused on this path
	mc := &managedConn{opts: &conn.opts}
	mc.chanFactory = func() (topologyChannel, error) { return ch, nil }
	return mc
}

func TestClassifyBrokerCancel_QueueDeleted(t *testing.T) {
	// A 404 NOT_FOUND on the passive declare means the queue was deleted.
	ch := &fakeInspectChannel{passiveErr: &amqp091.Error{Code: 404, Reason: "NOT_FOUND"}}
	mc := newInspectConn(t, ch)

	reason := classifyBrokerCancel(mc, "gone")

	assert.Equal(t, cancelReasonQueueDeleted, reason)
	assert.True(t, ch.closed, "the inspect channel must be closed")
}

func TestClassifyBrokerCancel_QueueExists_ExclusiveRevoked(t *testing.T) {
	// The queue still exists, so the broker cancelled us for another reason
	// (e.g. an exclusive lock was revoked / a single-active-consumer handoff).
	ch := &fakeInspectChannel{passiveErr: nil}
	mc := newInspectConn(t, ch)

	reason := classifyBrokerCancel(mc, "still-here")

	assert.Equal(t, cancelReasonExclusiveRevoked, reason)
}

func TestClassifyBrokerCancel_NonNotFoundError_Unknown(t *testing.T) {
	// A non-404 broker error is not classifiable into the deleted/revoked split.
	ch := &fakeInspectChannel{passiveErr: &amqp091.Error{Code: 405, Reason: "RESOURCE_LOCKED"}}
	mc := newInspectConn(t, ch)

	reason := classifyBrokerCancel(mc, "locked")

	assert.Equal(t, cancelReasonUnknown, reason)
}

func TestClassifyBrokerCancel_OpenChannelFails_Unknown(t *testing.T) {
	// No live socket (raw==nil, no chanFactory) → openChannel errors → unknown.
	conn := &Connection{}
	mc := &managedConn{opts: &conn.opts}

	reason := classifyBrokerCancel(mc, "anything")

	assert.Equal(t, cancelReasonUnknown, reason)
}

func TestClassifyBrokerCancel_ReasonIsBoundedEnum(t *testing.T) {
	// Whatever the outcome, the returned reason is always one of the closed
	// vocabulary values — never the unbounded ctag-<uuidv7> consumer tag.
	enums := map[string]struct{}{
		cancelReasonQueueDeleted:     {},
		cancelReasonExclusiveRevoked: {},
		cancelReasonUnknown:          {},
	}
	for _, perr := range []error{
		nil,
		&amqp091.Error{Code: 404, Reason: "NOT_FOUND"},
		&amqp091.Error{Code: 500, Reason: "WAT"},
		errors.New("transport blew up"),
	} {
		ch := &fakeInspectChannel{passiveErr: perr}
		reason := classifyBrokerCancel(newInspectConn(t, ch), "q")
		_, ok := enums[reason]
		require.Truef(t, ok, "reason %q must be a bounded enum", reason)
		assert.NotContains(t, reason, "ctag-", "reason must never be the consumer tag")
	}
}
