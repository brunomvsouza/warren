package confirms

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// White-box tests for the Lens-09 (PC-11) low-water-mark order index. They assert
// the internal invariants the public behavioural tests cannot observe: that the
// confirmed watermark (head) advances past resolved ghosts and that the order
// slice is compacted so it stays bounded by the outstanding window.

func orderIsSorted(t *testing.T, tr *Tracker) {
	t.Helper()
	assert.True(t, slices.IsSorted(tr.order), "order index must stay ascending: %v", tr.order)
}

// TestTracker_advanceLowWater_SweepsResolvedGhost verifies a leading entry that
// has been resolved and deleted from pending (a "ghost") is swept past on the
// next operation, advancing head to the lowest still-tracked tag.
func TestTracker_advanceLowWater_SweepsResolvedGhost(t *testing.T) {
	tr := New()
	require.NoError(t, tr.Register(1))
	require.NoError(t, tr.Register(2))
	require.NoError(t, tr.Register(3))
	require.Equal(t, 0, tr.head)

	// Resolve tag 1 and drain it via Wait, deleting it from pending — order still
	// holds it as a ghost, head has not moved.
	tr.Ack(1, false)
	require.NoError(t, tr.Wait(context.Background(), 1, 0))
	assert.Equal(t, 0, tr.head, "Wait must not move the watermark on its own")

	// The next Register runs advanceLowWater, which sweeps the tag-1 ghost and
	// stops at tag 2 (still pending).
	require.NoError(t, tr.Register(4))
	assert.Equal(t, 1, tr.head, "head must advance past the resolved ghost")
	assert.Equal(t, []uint64{2, 3, 4}, tr.order[tr.head:], "live window is tags 2..4")
	orderIsSorted(t, tr)
}

// TestTracker_compactOrder_BoundsTheIndex verifies a multiple=true frame that
// advances head well past the compaction threshold reclaims the dead prefix, so
// the order slice tracks only the outstanding window rather than every tag ever
// registered.
func TestTracker_compactOrder_BoundsTheIndex(t *testing.T) {
	tr := New()
	const total = 200
	for i := uint64(1); i <= total; i++ {
		require.NoError(t, tr.Register(i))
	}
	require.Len(t, tr.order, total)

	// Resolve tags 1..130 in one frame. head=130 > 64 and 130*2 >= 200, so the
	// dead prefix is compacted away and head resets to 0.
	tr.Ack(130, true)

	assert.Equal(t, 0, tr.head, "compaction resets head to 0")
	assert.Len(t, tr.order, total-130, "order keeps only the 70 outstanding tags")
	require.NotEmpty(t, tr.order)
	assert.Equal(t, uint64(131), tr.order[0], "lowest retained tag is 131")
	orderIsSorted(t, tr)
}

// TestTracker_LongLived_OrderStaysBounded drives many register→ack→wait cycles
// through the public API and asserts the order index never accumulates beyond a
// small multiple of the live window — i.e. there is no unbounded growth on a
// long-lived channel.
func TestTracker_LongLived_OrderStaysBounded(t *testing.T) {
	tr := New()
	ctx := context.Background()
	const window = 8
	var next uint64

	for round := 0; round < 5000; round++ {
		next++
		require.NoError(t, tr.Register(next))
		if next > window {
			tag := next - window
			tr.Ack(tag, false)
			require.NoError(t, tr.Wait(ctx, tag, 0))
		}
		assert.LessOrEqual(t, len(tr.order), 4*window+128,
			"order index must stay bounded by the outstanding window, not total tags")
	}
	orderIsSorted(t, tr)
}
