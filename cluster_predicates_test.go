package warren_test

// Unit coverage for the pure predicates the cluster reliability lane (Phase 9.5 /
// T166) relies on. Like lossByMessageID in chaos_reconnect_loss_test.go, these live
// in an un-tagged test file so their logic is verifiable on the fast unit lane
// without a live cluster, and reused verbatim by the `cluster`-tagged campaigns
// (which add only a live cluster + *testing.T around them). filterClusterCanceled
// gates every campaign's "consumer must stop cleanly" assertion, so a too-lax
// version would mask a real consumer-stop defect on every cluster run — it earns
// fast-lane coverage here rather than being exercised solely by a ~minute-scale
// cluster run.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// filterClusterCanceled drops the benign context.Canceled a consumer returns when
// it is stopped on purpose, so it is not recorded as a surface error. (A
// cluster-lane local: the integration lane's filterCanceled lives behind the
// `integration` tag and is not compiled here.)
func filterClusterCanceled(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func TestFilterClusterCanceled(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		assert.NoError(t, filterClusterCanceled(nil))
	})

	t.Run("context.Canceled is dropped (the on-purpose stop)", func(t *testing.T) {
		assert.NoError(t, filterClusterCanceled(context.Canceled))
	})

	t.Run("a wrapped context.Canceled is also dropped", func(t *testing.T) {
		// The consume loop returns ctx.Err() wrapped; errors.Is must see through the
		// wrap, or a clean stop would be misreported as a surface error on every run.
		wrapped := fmt.Errorf("consume loop exited: %w", context.Canceled)
		assert.NoError(t, filterClusterCanceled(wrapped))
	})

	t.Run("a real error is preserved unchanged", func(t *testing.T) {
		real := errors.New("channel closed unexpectedly")
		got := filterClusterCanceled(real)
		require.Error(t, got)
		assert.Equal(t, real, got, "a genuine failure must survive so the campaign can assert on it")
	})

	t.Run("context.DeadlineExceeded is NOT dropped (only Canceled is benign)", func(t *testing.T) {
		// A deadline-exceeded is a real timeout the campaign should see; the filter
		// must distinguish it from the deliberate cancel, not swallow both.
		require.ErrorIs(t, filterClusterCanceled(context.DeadlineExceeded), context.DeadlineExceeded)
	})
}
