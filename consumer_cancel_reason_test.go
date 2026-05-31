package warren

import (
	"errors"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
