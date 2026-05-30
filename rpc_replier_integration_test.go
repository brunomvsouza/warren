//go:build integration

package warren_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	"github.com/brunomvsouza/warren/metrics"
)

// replierDropMetrics counts the load-bearing replier_drop_no_dlx_total increments
// so the no-DLX silent-drop contract can be asserted from an integration test.
type replierDropMetrics struct {
	metrics.NoOpConsumerMetrics
	drops atomic.Int64
}

func (m *replierDropMetrics) RecordReplierDropNoDLX(string) { m.drops.Add(1) }

// serveReplier starts r.Serve in a goroutine and returns a stop function that
// cancels it and waits for it to drain.
func serveReplier[Req, Resp any](r *warren.Replier[Req, Resp], h warren.ReplyHandler[Req, Resp]) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Serve(ctx, h) }()
	return func() {
		cancel()
		<-done
	}
}

// getMessage polls queue with basic.get until a message arrives or within elapses.
func getMessage(t *testing.T, url, queue string, within time.Duration) (amqp091.Delivery, bool) {
	t.Helper()
	rc, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rc.Close() //nolint:errcheck
	ch, err := rc.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		d, ok, gerr := ch.Get(queue, true)
		require.NoError(t, gerr)
		if ok {
			return d, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return amqp091.Delivery{}, false
}

// passiveDepth returns the message count of a classic, no-arg queue.
func passiveDepth(t *testing.T, url, queue string) int {
	t.Helper()
	rc, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rc.Close() //nolint:errcheck
	ch, err := rc.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck
	q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	require.NoError(t, err)
	return q.Messages
}

// TestReplier_HappyPath_integration round-trips a request through a real Replier
// and the T29 Caller and asserts the response is correct.
func TestReplier_HappyPath_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	queue := fmt.Sprintf("warren.rpc.replier.happy.%d", time.Now().UnixNano())
	defer deleteQueues(url, queue)
	declareQueue(t, url, queue)

	ctx := context.Background()
	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	replier, err := warren.ReplierFor[rpcEcho, rpcEcho](conn).Queue(queue).Build()
	require.NoError(t, err)
	stop := serveReplier(replier, func(_ context.Context, req rpcEcho) (rpcEcho, error) {
		return rpcEcho{N: req.N + 1}, nil
	})
	defer stop()

	caller, err := warren.CallerFor[rpcEcho, rpcEcho](conn).RoutingKey(queue).Build()
	require.NoError(t, err)
	defer caller.Close(ctx) //nolint:errcheck

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := caller.Call(cctx, rpcEcho{N: 41})
	require.NoError(t, err)
	assert.Equal(t, 42, resp.N)
}

// TestReplier_HandlerError_WithDLX_integration asserts that a handler error nacks
// the request to a configured DLX (it lands in the DLQ), OnError fires once, and
// the caller times out cleanly.
func TestReplier_HandlerError_WithDLX_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	queue := fmt.Sprintf("warren.rpc.replier.dlx.%d", time.Now().UnixNano())
	dlx := queue + ".dlx"
	dlq := queue + ".dlq"
	defer deleteQueues(url, queue, dlq)
	defer deleteExchanges(url, dlx)

	ctx := context.Background()
	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	// The DeadLetter pre-pass injects x-dead-letter-exchange on the source queue and
	// creates the DLX + DLQ; we add the DLX→DLQ binding (topic "#") explicitly so the
	// dead-lettered request actually lands in the DLQ.
	topo := &warren.Topology{
		Queues:      []warren.Queue{{Name: queue, Durable: true}},
		DeadLetters: []warren.DeadLetter{{Source: queue}},
		Bindings:    []warren.Binding{{Exchange: dlx, Queue: dlq, RoutingKey: "#"}},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	var onErr atomic.Int64
	replier, err := warren.ReplierFor[rpcEcho, rpcEcho](conn).
		Queue(queue).
		Topology(topo).
		OnError(func(context.Context, rpcEcho, error) { onErr.Add(1) }).
		Build()
	require.NoError(t, err)
	stop := serveReplier(replier, func(_ context.Context, _ rpcEcho) (rpcEcho, error) {
		return rpcEcho{}, fmt.Errorf("handler refuses this request")
	})
	defer stop()

	caller, err := warren.CallerFor[rpcEcho, rpcEcho](conn).RoutingKey(queue).Build()
	require.NoError(t, err)
	defer caller.Close(ctx) //nolint:errcheck

	cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	_, err = caller.Call(cctx, rpcEcho{N: 7})
	require.ErrorIs(t, err, warren.ErrCallTimeout, "handler error must surface to the caller as ErrCallTimeout")

	assert.Equal(t, int64(1), onErr.Load(), "OnError must fire exactly once")
	_, ok := getMessage(t, url, dlq, 5*time.Second)
	assert.True(t, ok, "the dead-lettered request must land in the DLQ")
}

// TestReplier_HandlerError_NoDLX_integration asserts the no-DLX drop is real and
// observable: OnError fires, replier_drop_no_dlx_total increments, the request is
// gone from the source queue, and the caller times out.
func TestReplier_HandlerError_NoDLX_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	queue := fmt.Sprintf("warren.rpc.replier.nodlx.%d", time.Now().UnixNano())
	defer deleteQueues(url, queue)
	declareQueue(t, url, queue)

	ctx := context.Background()
	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	dropMetrics := &replierDropMetrics{}
	var onErr atomic.Int64
	replier, err := warren.ReplierFor[rpcEcho, rpcEcho](conn).
		Queue(queue).
		Metrics(dropMetrics).
		OnError(func(context.Context, rpcEcho, error) { onErr.Add(1) }).
		Build()
	require.NoError(t, err)
	stop := serveReplier(replier, func(_ context.Context, _ rpcEcho) (rpcEcho, error) {
		return rpcEcho{}, fmt.Errorf("handler refuses this request")
	})
	defer stop()

	caller, err := warren.CallerFor[rpcEcho, rpcEcho](conn).RoutingKey(queue).Build()
	require.NoError(t, err)
	defer caller.Close(ctx) //nolint:errcheck

	cctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	_, err = caller.Call(cctx, rpcEcho{N: 3})
	require.ErrorIs(t, err, warren.ErrCallTimeout)

	assert.Equal(t, int64(1), onErr.Load(), "OnError must fire once")
	assert.Equal(t, int64(1), dropMetrics.drops.Load(), "replier_drop_no_dlx_total must increment by 1")

	// The drop is real: the request is gone from the source queue.
	require.Eventually(t, func() bool {
		return passiveDepth(t, url, queue) == 0
	}, 5*time.Second, 100*time.Millisecond, "the dropped request must leave the source queue")
}
