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

	"github.com/brunomvsouza/warren"
)

// deleteDurableQueue removes a durable queue left behind by a test so reruns
// start clean. Best-effort: failures are ignored.
func deleteDurableQueue(url, name string) {
	rawConn, err := amqp091.Dial(url)
	if err != nil {
		return
	}
	defer rawConn.Close() //nolint:errcheck
	ch, err := rawConn.Channel()
	if err != nil {
		return
	}
	defer ch.Close() //nolint:errcheck
	_, _ = ch.QueueDelete(name, false, false, false)
}

func TestTopology_Declare_happyPath_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Exchanges: []warren.Exchange{
			{Name: "test.events", Kind: warren.ExchangeTopic, Durable: false, AutoDelete: true},
		},
		Queues: []warren.Queue{
			{Name: "test.orders", Durable: false, AutoDelete: true},
		},
		Bindings: []warren.Binding{
			{Exchange: "test.events", Queue: "test.orders", RoutingKey: "order.#"},
		},
	}

	err = topo.Declare(ctx, conn)
	require.NoError(t, err)
}

func TestTopology_Declare_idempotent_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{
			{Name: "test.idempotent", Durable: false, AutoDelete: true},
		},
	}

	require.NoError(t, topo.Declare(ctx, conn))
	require.NoError(t, topo.Declare(ctx, conn), "second Declare must be idempotent")
}

func TestTopology_Declare_mismatch_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Declare a durable queue first.
	topo1 := &warren.Topology{
		Queues: []warren.Queue{
			{Name: "test.mismatch", Durable: true},
		},
	}
	require.NoError(t, topo1.Declare(ctx, conn))

	conn2, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn2.Close(closeCtx)
	}()

	// Try to redeclare with a conflicting non-durable flag.
	topo2 := &warren.Topology{
		Queues: []warren.Queue{
			{Name: "test.mismatch", Durable: false},
		},
	}
	err = topo2.Declare(ctx, conn2)
	require.Error(t, err)
	assert.ErrorIs(t, err, warren.ErrTopologyMismatch, "must be ErrTopologyMismatch")
	assert.ErrorIs(t, err, warren.ErrPreconditionFailed, "must also unwrap ErrPreconditionFailed")
}

// TestTopology_Declare_noWaitDowngradesMismatchToAsync_integration is the T15
// regression for the NoWait caveat: a conflicting redeclare normally surfaces
// ErrTopologyMismatch, but with NoWait=true mismatch detection is downgraded to
// asynchronous so Declare returns nil.
func TestTopology_Declare_noWaitDowngradesMismatchToAsync_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const qname = "test.nowait.async"
	t.Cleanup(func() { deleteDurableQueue(url, qname) })

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	// Declare a durable queue synchronously.
	base := &warren.Topology{
		Queues: []warren.Queue{{Name: qname, Durable: true}},
	}
	require.NoError(t, base.Declare(ctx, conn))

	// A conflicting redeclare WITHOUT NoWait surfaces ErrTopologyMismatch.
	conflict := &warren.Topology{
		Queues: []warren.Queue{{Name: qname, Durable: false}},
	}
	err = conflict.Declare(ctx, conn)
	require.Error(t, err)
	assert.ErrorIs(t, err, warren.ErrTopologyMismatch)
	assert.ErrorIs(t, err, warren.ErrPreconditionFailed)

	// The same conflict WITH NoWait=true is downgraded to async: Declare returns nil.
	conflictNoWait := &warren.Topology{
		Queues: []warren.Queue{{Name: qname, Durable: false, NoWait: true}},
	}
	assert.NoError(t, conflictNoWait.Declare(ctx, conn),
		"NoWait=true must downgrade mismatch detection to async (Declare returns nil)")
}

func TestTopology_Declare_dlxExpansion_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{
			{Name: "test.dlx.source", Durable: false, AutoDelete: true},
		},
		DeadLetters: []warren.DeadLetter{
			{Source: "test.dlx.source", Exchange: "test.dlx.exchange"},
		},
	}

	// DLX expansion must succeed: source queue gets x-dead-letter-exchange injected
	// and the DLX exchange + DLQ queue are created automatically.
	err = topo.Declare(ctx, conn)
	require.NoError(t, err)
}

func TestTopology_Declare_quorumWithDeliveryLimit_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{
			{
				Name:          "test.quorum.dl",
				Durable:       true,
				Type:          warren.QueueTypeQuorum,
				DeliveryLimit: 5,
			},
		},
	}

	err = topo.Declare(ctx, conn)
	require.NoError(t, err)
}
