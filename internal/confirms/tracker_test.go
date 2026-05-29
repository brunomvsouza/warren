package confirms_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/internal/confirms"
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

// — Out-of-order register ————————————————————————————————————————————————

// TestTracker_OutOfOrderRegister_MultipleAckResolvesCorrectly registers delivery
// tags out of ascending order. Real channels assign tags monotonically, but the
// order index must stay sorted regardless (binary-insert path) so a multiple=true
// frame resolves the correct subset.
func TestTracker_OutOfOrderRegister_MultipleAckResolvesCorrectly(t *testing.T) {
	tr := confirms.New()
	for _, tag := range []uint64{3, 1, 5, 2, 4} {
		require.NoError(t, tr.Register(tag))
	}
	chs := make(map[uint64]<-chan error, 5)
	for tag := uint64(1); tag <= 5; tag++ {
		chs[tag] = asyncWait(t, tr, tag)
	}

	tr.Ack(3, true) // must resolve 1, 2, 3 — not 4, 5

	for tag := uint64(1); tag <= 3; tag++ {
		assert.NoError(t, <-chs[tag], "tag %d should ack", tag)
	}

	tr.Nack(5, true) // resolves 4, 5
	for tag := uint64(4); tag <= 5; tag++ {
		assert.ErrorIs(t, <-chs[tag], confirms.ErrNacked, "tag %d should nack", tag)
	}
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

// Cancel removes the slot; a subsequent Wait returns ErrClosed immediately (tag is gone).
func TestTracker_Cancel_SubsequentWait_ReturnsErrClosed(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))
	tr.Cancel(1)
	// timeout=0: Wait must return ErrClosed via the "!ok" path, not via timeout.
	err := tr.Wait(context.Background(), 1, 0)
	assert.ErrorIs(t, err, confirms.ErrClosed)
}

// Cancel of a tag that was never registered must not panic.
func TestTracker_Cancel_UnregisteredTag_IsNoOp(t *testing.T) {
	tr := confirms.New()
	tr.Cancel(99) // must not panic or have any effect
}

// Cancel while Wait is already blocking must unblock Wait with ErrClosed immediately.
func TestTracker_Cancel_WhileWaitBlocking_ReturnsErrClosed(t *testing.T) {
	tr := confirms.New()
	require.NoError(t, tr.Register(1))

	result := make(chan error, 1)
	blocking := make(chan struct{})
	go func() {
		close(blocking) // signal that goroutine has started
		result <- tr.Wait(context.Background(), 1, time.Second)
	}()
	<-blocking
	time.Sleep(2 * time.Millisecond) // let Wait enter its select

	tr.Cancel(1)

	select {
	case err := <-result:
		assert.ErrorIs(t, err, confirms.ErrClosed)
	case <-time.After(shortTimeout):
		t.Fatal("Wait did not unblock after Cancel")
	}
}

// — Lens-09 (PC-06): pooled confirm-timeout timer ————————————————————————————

// TestTracker_Wait_ArmingTimer_DoesNotAllocate is the PC-06 allocation guard.
// The default ConfirmTimeout=30s arms a time.Timer on every Wait — i.e. on every
// publish and every batch element. A fresh time.NewTimer per call allocates; the
// timer must be pooled/reset so arming it adds no per-Wait allocation. The guard
// measures the same Register→Ack→Wait cycle with a timeout (arms the timer) and
// without (no timer) and asserts the two allocate identically.
func TestTracker_Wait_ArmingTimer_DoesNotAllocate(t *testing.T) {
	ctx := context.Background()

	var tag uint64
	cycle := func(timeout time.Duration) func() {
		return func() {
			tag++
			tr := confirms.New()
			_ = tr.Register(tag)
			tr.Ack(tag, false)
			_ = tr.Wait(ctx, tag, timeout)
		}
	}

	withTimer := testing.AllocsPerRun(2000, cycle(30*time.Second))
	noTimer := testing.AllocsPerRun(2000, cycle(0))

	assert.Equal(t, noTimer, withTimer,
		"arming the confirm-timeout timer must not allocate; pool/reset the timer (Lens-09 PC-06)")
}

// — Lens-09 (PC-11): resolveUpTo low-water-mark, no per-frame scan/alloc ——————

// TestTracker_MultipleAck_DoesNotAllocatePerFrame is the PC-11 allocation guard.
// Resolving a multiple=true frame must not scan the whole pending map and sort a
// freshly allocated slice on every frame (the old O(outstanding)/frame design).
// With a contiguous low-water-mark over an ascending index, advancing the
// confirmed watermark allocates nothing per frame.
func TestTracker_MultipleAck_DoesNotAllocatePerFrame(t *testing.T) {
	tr := confirms.New()
	const N = 4096
	for i := uint64(1); i <= N; i++ {
		require.NoError(t, tr.Register(i))
	}

	var tag uint64
	allocs := testing.AllocsPerRun(2000, func() {
		tag++
		tr.Ack(tag, true) // advances the low-water-mark by exactly one tag
	})

	assert.Zero(t, allocs,
		"resolving a multiple=true frame must not allocate per frame (Lens-09 PC-11)")
}

// TestTracker_MultipleAck_LowWaterMark_LargeOutstanding exercises the resolve
// path with a deep outstanding window and an interleaving of single and multiple
// acks, asserting every waiter resolves exactly once with the right verdict.
// It is the behavioural companion to the PC-11 allocation guard.
func TestTracker_MultipleAck_LowWaterMark_LargeOutstanding(t *testing.T) {
	tr := confirms.New()
	const N = 500
	results := make([]<-chan error, N+1)
	for i := uint64(1); i <= N; i++ {
		require.NoError(t, tr.Register(i))
		results[i] = asyncWait(t, tr, i)
	}

	// Single-ack a scattered subset first (creates ghosts above the watermark).
	tr.Ack(250, false)
	tr.Ack(100, false)
	// Then a broad multiple=true ack sweeps everything up to 400.
	tr.Ack(400, true)
	// Finally a multiple=true nack covers the remainder.
	tr.Nack(N, true)

	for i := uint64(1); i <= 400; i++ {
		assert.NoError(t, <-results[i], "tag %d should ack", i)
	}
	for i := uint64(401); i <= N; i++ {
		assert.ErrorIs(t, <-results[i], confirms.ErrNacked, "tag %d should nack", i)
	}
}
