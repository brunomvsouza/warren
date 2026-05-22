package confirms_test

import (
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
	go func() { ch <- tr.Wait(tag, time.Second) }()
	return ch
}

// — Single ack ————————————————————————————————————————————————————————————

func TestTracker_SingleAck_ResolvesNil(t *testing.T) {
	tr := confirms.New()
	tr.Register(1)
	ch := asyncWait(t, tr, 1)

	tr.Ack(1, false)

	assert.NoError(t, <-ch)
}

// — Single nack ———————————————————————————————————————————————————————————

func TestTracker_SingleNack_ResolvesErrNacked(t *testing.T) {
	tr := confirms.New()
	tr.Register(1)
	ch := asyncWait(t, tr, 1)

	tr.Nack(1, false)

	assert.ErrorIs(t, <-ch, confirms.ErrNacked)
}

// — Multiple=true ack ————————————————————————————————————————————————————

func TestTracker_MultipleAck_ResolvesAllTagsUpToN(t *testing.T) {
	tr := confirms.New()
	tr.Register(1)
	tr.Register(2)
	tr.Register(3)
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
	tr.Register(1)
	tr.Register(2)
	tr.Register(3)
	ch1 := asyncWait(t, tr, 1)
	ch2 := asyncWait(t, tr, 2)
	ch3 := asyncWait(t, tr, 3)

	tr.Nack(2, true) // nacks 1 and 2

	assert.ErrorIs(t, <-ch1, confirms.ErrNacked)
	assert.ErrorIs(t, <-ch2, confirms.ErrNacked)

	tr.Ack(3, false)
	assert.NoError(t, <-ch3)
}

// — Broker nack with multiple=true (all five tags) ————————————————————————

func TestTracker_MultipleNackLarge_ResolvesCorrectSubset(t *testing.T) {
	tr := confirms.New()
	for i := uint64(1); i <= 5; i++ {
		tr.Register(i)
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
	tr.Register(2)
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
	tr.Register(5)
	ch := asyncWait(t, tr, 5)

	tr.MarkReturned(5, 313)
	tr.Ack(5, false)

	err := <-ch
	require.Error(t, err)
	var ue *confirms.UnroutableError
	require.ErrorAs(t, err, &ue)
	assert.Equal(t, uint16(313), ue.ReplyCode)
}

// — Out-of-order acks ————————————————————————————————————————————————————

func TestTracker_OutOfOrderAcks_AllResolveCorrectly(t *testing.T) {
	tr := confirms.New()
	tr.Register(1)
	tr.Register(2)
	tr.Register(3)
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
	tr.Register(1)
	tr.Register(2)
	ch1 := asyncWait(t, tr, 1)
	ch2 := asyncWait(t, tr, 2)

	tr.CloseAll()

	assert.ErrorIs(t, <-ch1, confirms.ErrClosed)
	assert.ErrorIs(t, <-ch2, confirms.ErrClosed)
}

// — Timeout ————————————————————————————————————————————————————————————————

func TestTracker_Wait_ReturnsErrTimeoutWhenNoConfirmArrives(t *testing.T) {
	tr := confirms.New()
	tr.Register(1)

	err := tr.Wait(1, shortTimeout)

	assert.ErrorIs(t, err, confirms.ErrTimeout)
}

// — Nack-only (no return) ——————————————————————————————————————————————————

func TestTracker_BrokerNackAlone_ResolvesErrNacked(t *testing.T) {
	tr := confirms.New()
	tr.Register(10)
	ch := asyncWait(t, tr, 10)

	tr.Nack(10, false)

	assert.ErrorIs(t, <-ch, confirms.ErrNacked)
}
