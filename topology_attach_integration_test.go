//go:build integration

package warren_test

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	amqp "github.com/brunomvsouza/warren"
)

func TestTopology_AttachTo_redeclaresAfterReconnect_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	reconnected := make(chan struct{}, 1)
	conn, err := amqp.Dial(ctx,
		amqp.WithAddr(url),
		amqp.WithOnReconnect(func() {
			select {
			case reconnected <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	})

	topo := &amqp.Topology{
		Queues: []amqp.Queue{
			{Name: "test.attach.q1", Durable: false, AutoDelete: true},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn))

	// Force a reconnect to exercise the barrier.
	require.NoError(t, conn.ForceReconnect())

	// Wait deterministically for the reconnect hook to fire.
	select {
	case <-reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for WithOnReconnect callback after ForceReconnect")
	}

	// Verify the queue was actually re-declared by doing a passive inspect.
	rawConn, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rawConn.Close()
	rawCh, err := rawConn.Channel()
	require.NoError(t, err)
	defer rawCh.Close()
	_, err = rawCh.QueueInspect("test.attach.q1")
	require.NoError(t, err, "queue test.attach.q1 must exist after topology redeclare")
}

func TestTopology_AttachTo_onReconnectFiresAfterTopologyRedeclared_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	reconnected := make(chan struct{}, 1)
	conn, err := amqp.Dial(ctx,
		amqp.WithAddr(url),
		amqp.WithOnReconnect(func() {
			select {
			case reconnected <- struct{}{}:
			default:
			}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	})

	topo := &amqp.Topology{
		Queues: []amqp.Queue{
			{Name: "test.attach.order.q", Durable: false, AutoDelete: true},
		},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.AttachTo(conn))

	require.NoError(t, conn.ForceReconnect())

	select {
	case <-reconnected:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for WithOnReconnect callback")
	}
}

func TestTopology_AttachTo_degradedOnMismatch_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	// Clean up the durable queue after the test regardless of outcome.
	t.Cleanup(func() {
		rawConn, err := amqp091.Dial(url)
		if err != nil {
			return
		}
		defer rawConn.Close()
		rawCh, err := rawConn.Channel()
		if err != nil {
			return
		}
		defer rawCh.Close()
		_, _ = rawCh.QueueDelete("test.attach.durable", false, false, false)
	})

	degradedCh := make(chan error, 1)
	conn, err := amqp.Dial(ctx,
		amqp.WithAddr(url),
		amqp.WithOnTopologyDegraded(func(err error) {
			select {
			case degradedCh <- err:
			default:
			}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	})

	// Declare a durable queue.
	topo1 := &amqp.Topology{
		Queues: []amqp.Queue{
			{Name: "test.attach.durable", Durable: true},
		},
	}
	require.NoError(t, topo1.Declare(ctx, conn))

	// Register a topology that conflicts: non-durable declaration of the same queue.
	conflicting := &amqp.Topology{
		Queues: []amqp.Queue{
			{Name: "test.attach.durable", Durable: false},
		},
	}
	require.NoError(t, conflicting.AttachTo(conn))

	// Force reconnect — the hook will fail with PRECONDITION_FAILED.
	require.NoError(t, conn.ForceReconnect())

	// Wait for degraded callback.
	select {
	case degradedErr := <-degradedCh:
		assert.ErrorIs(t, degradedErr, amqp.ErrTopologyRedeclareFailed)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for WithOnTopologyDegraded callback")
	}
}
