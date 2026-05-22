//go:build integration

package warren_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	amqp "github.com/brunomvsouza/warren"
)

func TestTopology_Declare_happyPath_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := amqp.Dial(ctx, amqp.WithAddr(url))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})

	topo := &amqp.Topology{
		Exchanges: []amqp.Exchange{
			{Name: "test.events", Kind: amqp.ExchangeTopic, Durable: false, AutoDelete: true},
		},
		Queues: []amqp.Queue{
			{Name: "test.orders", Durable: false, AutoDelete: true},
		},
		Bindings: []amqp.Binding{
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

	conn, err := amqp.Dial(ctx, amqp.WithAddr(url))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})

	topo := &amqp.Topology{
		Queues: []amqp.Queue{
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

	conn, err := amqp.Dial(ctx, amqp.WithAddr(url))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})

	// Declare a durable queue first.
	topo1 := &amqp.Topology{
		Queues: []amqp.Queue{
			{Name: "test.mismatch", Durable: true},
		},
	}
	require.NoError(t, topo1.Declare(ctx, conn))

	conn2, err := amqp.Dial(ctx, amqp.WithAddr(url))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn2.Close(ctx)
	})

	// Try to redeclare with a conflicting non-durable flag.
	topo2 := &amqp.Topology{
		Queues: []amqp.Queue{
			{Name: "test.mismatch", Durable: false},
		},
	}
	err = topo2.Declare(ctx, conn2)
	require.Error(t, err)
	assert.ErrorIs(t, err, amqp.ErrTopologyMismatch, "must be ErrTopologyMismatch")
	assert.ErrorIs(t, err, amqp.ErrPreconditionFailed, "must also unwrap ErrPreconditionFailed")
}

func TestTopology_Declare_dlxExpansion_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	conn, err := amqp.Dial(ctx, amqp.WithAddr(url))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})

	topo := &amqp.Topology{
		Queues: []amqp.Queue{
			{Name: "test.dlx.source", Durable: false, AutoDelete: true},
		},
		DeadLetters: []amqp.DeadLetter{
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

	conn, err := amqp.Dial(ctx, amqp.WithAddr(url))
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})

	topo := &amqp.Topology{
		Queues: []amqp.Queue{
			{
				Name:          "test.quorum.dl",
				Durable:       true,
				Type:          amqp.QueueTypeQuorum,
				DeliveryLimit: 5,
			},
		},
	}

	err = topo.Declare(ctx, conn)
	require.NoError(t, err)
}
