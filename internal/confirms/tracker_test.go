package confirms_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/amqp/internal/confirms"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

const shortTimeout = 50 * time.Millisecond

func asyncWait(t *testing.T, tr *confirms.Tracker, tag uint64) <-chan error {
	t.Helper()
	ch := make(chan error, 1)
	go func() { ch <- tr.Wait(context.Background(), tag, time.Second) }()
	return ch
}

// — UnroutableError.Error ——————————————————————————————————————————————————

func TestUnroutableError_Error_ContainsReplyCode(t *testing.T) {
	e := &confirms.UnroutableError{ReplyCode: 312}
	assert.Equal(t, "confirms: mandatory publish unroutable (reply code 312)", e.Error())
}

// — Single ack ————————————————————————————————————————————————————————————

func TestTracker_SingleAck_ResolvesNil(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	ch := asyncWait(t, tr, 1)

	tr.Ack(1, false)

	assert.NoError(t, <-ch)
}

// — Single nack ———————————————————————————————————————————————————————————

func TestTracker_SingleNack_ResolvesErrNacked(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	ch := asyncWait(t, tr, 1)

	tr.Nack(1, false)

	assert.ErrorIs(t, <-ch, confirms.ErrNacked)
}

// — Multiple=true ack ————————————————————————————————————————————————————

func TestTracker_MultipleAck_ResolvesAllTagsUpToN(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	require.NoError(t, tr.Register(2))
	require.NoError(t, tr.Register(3))
	ch1 := asyncWait(t, tr, 1)
	ch2 := asyncWait(t, tr, 2)
	ch3 := asyncWait(t, tr, 3)

	tr.Ack(2, true) // resolves 1 and 2

	assert.NoError(t, <-ch1)
	assert.NoError(t, <-ch2)

	tr.Ack(3, false)
	assert.NoError(t, <-ch3)
}

// — Multiple=true nack ———————————————————————————————————————————————————

func TestTracker_MultipleNack_ResolvesAllTagsUpToNWithNacked(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	require.NoError(t, tr.Register(2))
	require.NoError(t, tr.Register(3))
	ch1 := asyncWait(t, tr, 1)
	ch2 := asyncWait(t, tr, 2)
	ch3 := asyncWait(t, tr, 3)

	tr.Nack(2, true) // nacks 1 and 2

	assert.ErrorIs(t, <-ch1, confirms.ErrNacked)
	assert.ErrorIs(t, <-ch2, confirms.ErrNacked)

	tr.Ack(3, false)
	assert.NoError(t, <-ch3)
}

// — Broker nack with multiple=true (large subset) ————————————————————————

func TestTracker_MultipleNackLarge_ResolvesCorrectSubset(t *testing.T) {
	tr := confirms.New()
	for i := uint64(1); i <= 5; i++ {
		require.NoError(t, tr.Register(i))
	}
	var chs [6]<-chan error
	for i := uint64(1); i <= 5; i++ {
		chs[i] = asyncWait(t, tr, i)
	}

	tr.Nack(3, true) // nacks 1, 2, 3

	for i := uint64(1); i <= 3; i++ {
		assert.ErrorIs(t, <-chs[i], confirms.ErrNacked, "tag %d", i)
	}

	tr.Ack(5, true) // acks 4, 5
	for i := uint64(4); i <= 5; i++ {
		assert.NoError(t, <-chs[i], "tag %d", i)
	}
}

// — basic.return + basic.ack (mandatory publish unroutable) ———————————————

func TestTracker_ReturnThenAck_312_ResolvesUnroutable(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(2))
	ch := asyncWait(t, tr, 2)

	tr.MarkReturned(2, 312)
	tr.Ack(2, false)

	err := <-ch
	require.Error(t, err)
	var ue *confirms.UnroutableError
	require.ErrorAs(t, err, &ue)
	assert.Equal(t, uint16(312), ue.ReplyCode)
}

func TestTracker_ReturnThenAck_313_ResolvesUnroutable(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(5))
	ch := asyncWait(t, tr, 5)

	tr.MarkReturned(5, 313)
	tr.Ack(5, false)

	err := <-ch
	require.Error(t, err)
	var ue *confirms.UnroutableError
	require.ErrorAs(t, err, &ue)
	assert.Equal(t, uint16(313), ue.ReplyCode)
}

// — basic.return + Ack with multiple=true ————————————————————————————————

func TestTracker_ReturnThenMultipleAck_ResolvesUnroutable(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	require.NoError(t, tr.Register(2))
	ch1 := asyncWait(t, tr, 1)
	ch2 := asyncWait(t, tr, 2)

	tr.MarkReturned(1, 312)
	tr.Ack(2, true) // multiple=true covers tag 1 (returned) and tag 2 (normal)

	var ue *confirms.UnroutableError
	require.ErrorAs(t, <-ch1, &ue)
	assert.Equal(t, uint16(312), ue.ReplyCode)
	assert.NoError(t, <-ch2)
}

// — Out-of-order acks ————————————————————————————————————————————————————

func TestTracker_OutOfOrderAcks_AllResolveCorrectly(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	require.NoError(t, tr.Register(2))
	require.NoError(t, tr.Register(3))
	ch1 := asyncWait(t, tr, 1)
	ch2 := asyncWait(t, tr, 2)
	ch3 := asyncWait(t, tr, 3)

	tr.Ack(3, false)
	tr.Ack(1, false)
	tr.Ack(2, false)

	assert.NoError(t, <-ch1)
	assert.NoError(t, <-ch2)
	assert.NoError(t, <-ch3)
}

// — Channel close —————————————————————————————————————————————————————————

func TestTracker_CloseAll_ResolvesAllPendingWithErrClosed(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	require.NoError(t, tr.Register(2))
	ch1 := asyncWait(t, tr, 1)
	ch2 := asyncWait(t, tr, 2)

	tr.CloseAll()

	assert.ErrorIs(t, <-ch1, confirms.ErrClosed)
	assert.ErrorIs(t, <-ch2, confirms.ErrClosed)
}

// CloseAll after Ack must not overwrite the ack result with ErrClosed.
// This exercises the default branch in CloseAll (buffer already full).
func TestTracker_CloseAll_AfterAck_PreservesAckResult(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	tr.Ack(1, false) // resolve into buffer before CloseAll
	tr.CloseAll()    // default branch: buffer full, entry left for Wait
	err := tr.Wait(context.Background(), 1, shortTimeout)
	assert.NoError(t, err)
}

// CloseAll on an empty tracker must not panic.
func TestTracker_CloseAll_EmptyTracker_IsNoOp(t *testing.T) {
	tr := confirms.New()
	tr.CloseAll() // no-op, must not panic
}

// — Register after CloseAll returns ErrClosed ————————————————————————————

func TestTracker_Register_AfterCloseAll_ReturnsErrClosed(t *testing.T) {
	tr := confirms.New()
	tr.CloseAll()
	err := tr.Register(1)
	assert.ErrorIs(t, err, confirms.ErrClosed)
}

// — Wait on unregistered tag ——————————————————————————————————————————————

func TestTracker_Wait_UnregisteredTag_ReturnsErrClosed(t *testing.T) {
	tr := confirms.New()
	err := tr.Wait(context.Background(), 99, shortTimeout)
	assert.ErrorIs(t, err, confirms.ErrClosed)
}

// — Ack before Wait (exercises buffered-channel rendezvous) ———————————————

func TestTracker_AckBeforeWait_StillResolves(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	tr.Ack(1, false) // resolve into buffer BEFORE Wait is called
	err := tr.Wait(context.Background(), 1, shortTimeout)
	assert.NoError(t, err)
}

// — Double-Ack same tag (exercises default branch of resolveOne) ——————————

func TestTracker_DoubleAck_SameTag_SecondIsNoOp(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	require.NoError(t, tr.Register(2))
	ch1 := asyncWait(t, tr, 1)
	ch2 := asyncWait(t, tr, 2)

	tr.Ack(2, true)  // resolveUpTo: resolves both 1 and 2
	tr.Ack(1, false) // resolveOne(1): default branch hit (buffer already full)

	assert.NoError(t, <-ch1)
	assert.NoError(t, <-ch2)
}

// — Timeout ————————————————————————————————————————————————————————————————

func TestTracker_Wait_ReturnsErrTimeoutWhenNoConfirmArrives(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))

	err := tr.Wait(context.Background(), 1, shortTimeout)

	assert.ErrorIs(t, err, confirms.ErrTimeout)
}

// — ctx cancellation ————————————————————————————————————————————————————————

func TestTracker_Wait_CtxCancelled_ReturnsCtxErr(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Wait
	err := tr.Wait(ctx, 1, time.Second)
	assert.ErrorIs(t, err, context.Canceled)
}

// — Nack-only (no return) ——————————————————————————————————————————————————

func TestTracker_BrokerNackAlone_ResolvesErrNacked(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(10))
	ch := asyncWait(t, tr, 10)

	tr.Nack(10, false)

	assert.ErrorIs(t, <-ch, confirms.ErrNacked)
}

// — MarkReturned on unregistered tag is a no-op ——————————————————————————

func TestTracker_MarkReturned_UnregisteredTag_IsNoOp(t *testing.T) {
	tr := confirms.New()
	tr.MarkReturned(99, 312) // must not panic or have any effect
}

// — Cancel ————————————————————————————————————————————————————————————————

// Cancel removes the slot; a subsequent Wait returns ErrClosed.
func TestTracker_Cancel_SubsequentWait_ReturnsErrClosed(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	tr.Cancel(1)
	err := tr.Wait(context.Background(), 1, shortTimeout)
	assert.ErrorIs(t, err, confirms.ErrClosed)
}

// Cancel of a tag that was never registered must not panic.
func TestTracker_Cancel_UnregisteredTag_IsNoOp(t *testing.T) {
	tr := confirms.New()
	tr.Cancel(99) // must not panic or have any effect
}
