//go:build stress

package warren

import "testing"

// TestStress re-runs the scheduling-sensitive tests so their determinism can be hammered
// under -race and CI scheduling pressure. These exercise the handler-timeout select's
// outer-ctx-cancel branch, which previously raced handlerDone vs hCtx.Done() — a 50/50 Go
// select that flaked intermittently until the testHookBeforeTimeoutDrain seam made the
// choice deterministic.
//
// Membership lives here, compiled only under `-tags=stress`, rather than as a -run regex
// in the Makefile: the entries reference the test functions by value, so renaming or
// removing one breaks the build instead of letting the stress lane silently go stale.
//
// Run with:
//
//	make test-stress              # default STRESS_COUNT=200
//	make test-stress STRESS_COUNT=1000
//
// which expands to: go test -race -tags=stress -count=$(STRESS_COUNT) -run '^TestStress$' .
// (-count re-runs the whole TestStress wrapper, so each iteration runs every member.)
func TestStress(t *testing.T) {
	stress := []struct {
		name string
		fn   func(*testing.T)
	}{
		{"BatchConsumer_HandlerTimeout_CtxCancelledDuringHandler_NoFrame", TestBatchConsumer_HandlerTimeout_CtxCancelledDuringHandler_NoFrame},
		{"BatchConsumer_processBatchSpan_outerCtxCancel_noOutcome", TestBatchConsumer_processBatchSpan_outerCtxCancel_noOutcome},
		{"Consumer_Consume_TimeoutPath_OuterCtxCancelled_NoAckNoMetric", TestConsumer_Consume_TimeoutPath_OuterCtxCancelled_NoAckNoMetric},
	}
	for _, s := range stress {
		t.Run(s.name, s.fn)
	}
}
