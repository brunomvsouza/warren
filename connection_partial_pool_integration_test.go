//go:build integration

package warren_test

// T67 (R10-12 / SRE-08) real-broker assertion for the partial-pool-connect
// policy: when one pooled socket cannot connect at boot but ≥1 connection in
// its role succeeds, Dial SUCCEEDS, the degraded-capacity signal fires, and the
// missing socket reconnects under supervision (rather than fail-fast blocking
// the whole deploy, or silently losing capacity with no signal).

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/metrics"
)

// bootDegradedSpy counts connection_degraded_total{reason="boot_reduced_capacity"}.
type bootDegradedSpy struct {
	metrics.NoOpClientMetrics
	bootReduced atomic.Int64
	reconnects  atomic.Int64
}

func (s *bootDegradedSpy) RecordDegraded(_, reason string) {
	if reason == "boot_reduced_capacity" {
		s.bootReduced.Add(1)
	}
}
func (s *bootDegradedSpy) RecordReconnect(_ string) { s.reconnects.Add(1) }

func TestDial_partialPool_succeedsAndRecovers_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spy := &bootDegradedSpy{}

	// Fail the very first dial (one publisher socket unreachable at boot), then
	// delegate to the real dialer so the rest connect and the failed socket
	// recovers under supervision.
	var firstFailed atomic.Bool
	conn, err := warren.Dial(ctx,
		warren.WithAddr(url),
		warren.WithPublisherConnections(2),
		warren.WithConsumerConnections(2),
		warren.WithMetrics(spy),
		warren.WithReconnectBackoff(warren.RetryPolicy{Retries: 1}),
		warren.WithDialer(func(network, addr string) (net.Conn, error) {
			if firstFailed.CompareAndSwap(false, true) {
				return nil, errors.New("simulated unreachable socket at boot")
			}
			return net.DialTimeout(network, addr, 10*time.Second)
		}),
	)
	require.NoError(t, err, "Dial must succeed when ≥1 connection per role connects")
	require.NotNil(t, conn)
	defer func() {
		closeCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = conn.Close(closeCtx)
	}()

	// The degraded-capacity signal must have fired for the reduced publisher pool.
	assert.GreaterOrEqual(t, spy.bootReduced.Load(), int64(1),
		"connection_degraded_total{reason=boot_reduced_capacity} must increment at a partial boot")

	// The missing socket must recover under supervision — Health passes once the
	// whole pool is live.
	require.Eventually(t, func() bool {
		return conn.Health(ctx) == nil
	}, 15*time.Second, 100*time.Millisecond,
		"the boot-degraded socket must reconnect under supervision and Health must pass")
}
