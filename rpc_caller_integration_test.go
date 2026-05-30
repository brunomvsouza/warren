//go:build integration

package warren_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

// rpcEcho is the request and response payload for the Caller integration tests.
// The echo replier returns the request body verbatim, so a round-trip that
// preserves N proves the CorrelationID demultiplexing routed the reply correctly.
type rpcEcho struct {
	N int `json:"n"`
}

// startEchoReplier stands in for the not-yet-implemented Replier[Req,Resp] (T30):
// a raw amqp091 consumer on queue that publishes each request body straight back
// to its ReplyTo with the original CorrelationId. It works for both direct
// reply-to (ReplyTo == "amq.rabbitmq.reply-to") and an exclusive reply queue,
// because in both cases the reply is a publish to the default exchange keyed on
// ReplyTo. Returns a stop function that tears the replier down.
func startEchoReplier(t *testing.T, url, queue string) func() {
	t.Helper()
	rc, err := amqp091.Dial(url)
	require.NoError(t, err)
	ch, err := rc.Channel()
	require.NoError(t, err)

	_, err = ch.QueueDeclare(queue, false, false, false, false, nil)
	require.NoError(t, err)

	deliveries, err := ch.Consume(queue, "", false /* autoAck */, false, false, false, nil)
	require.NoError(t, err)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				_ = ch.PublishWithContext(context.Background(), "", d.ReplyTo, false, false, amqp091.Publishing{
					ContentType:   d.ContentType,
					CorrelationId: d.CorrelationId,
					Body:          d.Body,
				})
				_ = d.Ack(false)
			}
		}
	}()

	return func() {
		close(done)
		_ = ch.Close()
		_ = rc.Close()
		wg.Wait()
	}
}

// TestCaller_Concurrent100_integration exercises 100 concurrent Call invocations
// against an echo replier and asserts every response matches its request — the
// CorrelationID demultiplexing must keep replies separated under load.
func TestCaller_Concurrent100_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	queue := fmt.Sprintf("warren.rpc.caller.concurrent.%d", time.Now().UnixNano())
	defer deleteQueues(url, queue)

	stop := startEchoReplier(t, url, queue)
	defer stop()

	ctx := context.Background()
	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	caller, err := warren.CallerFor[rpcEcho, rpcEcho](conn).RoutingKey(queue).Build()
	require.NoError(t, err)
	defer caller.Close(ctx) //nolint:errcheck

	const n = 100
	var wg sync.WaitGroup
	errs := make([]error, n)
	got := make([]int, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			resp, err := caller.Call(cctx, rpcEcho{N: i})
			errs[i] = err
			got[i] = resp.N
		}(i)
	}
	wg.Wait()

	for i := range n {
		require.NoErrorf(t, errs[i], "call %d failed", i)
		assert.Equalf(t, i, got[i], "call %d got a mismatched reply", i)
	}
}

// TestCaller_CtxTimeout_integration asserts an unanswered request surfaces as
// ErrCallTimeout once the ctx deadline fires (no replier is running).
func TestCaller_CtxTimeout_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	queue := fmt.Sprintf("warren.rpc.caller.timeout.%d", time.Now().UnixNano())
	defer deleteQueues(url, queue)

	ctx := context.Background()
	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	// Declare the request queue so the request is parked (not silently dropped).
	declareQueue(t, url, queue)

	caller, err := warren.CallerFor[rpcEcho, rpcEcho](conn).RoutingKey(queue).Build()
	require.NoError(t, err)
	defer caller.Close(ctx) //nolint:errcheck

	cctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = caller.Call(cctx, rpcEcho{N: 1})
	require.Error(t, err)
	assert.ErrorIs(t, err, warren.ErrCallTimeout)
	assert.Less(t, time.Since(start), 5*time.Second, "Call must return promptly on ctx deadline")
}

// TestCaller_ExclusiveReplyQueue_integration exercises the UseExclusiveReplyQueue
// fallback: a real exclusive auto-delete reply queue with regular ack semantics.
func TestCaller_ExclusiveReplyQueue_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	queue := fmt.Sprintf("warren.rpc.caller.exclusive.%d", time.Now().UnixNano())
	defer deleteQueues(url, queue)

	stop := startEchoReplier(t, url, queue)
	defer stop()

	ctx := context.Background()
	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	caller, err := warren.CallerFor[rpcEcho, rpcEcho](conn).
		RoutingKey(queue).
		UseExclusiveReplyQueue().
		Prefetch(16).
		Build()
	require.NoError(t, err)
	defer caller.Close(ctx) //nolint:errcheck

	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := caller.Call(cctx, rpcEcho{N: 42})
	require.NoError(t, err)
	assert.Equal(t, 42, resp.N)
}

// TestCaller_ChannelCloseMidCall_integration forces a reconnect while a request
// is in flight (no reply will arrive) and asserts the in-flight Call surfaces
// ErrChannelClosed rather than hanging until its ctx deadline.
func TestCaller_ChannelCloseMidCall_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	queue := fmt.Sprintf("warren.rpc.caller.chanclose.%d", time.Now().UnixNano())
	defer deleteQueues(url, queue)

	ctx := context.Background()
	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	declareQueue(t, url, queue)

	caller, err := warren.CallerFor[rpcEcho, rpcEcho](conn).RoutingKey(queue).Build()
	require.NoError(t, err)
	defer caller.Close(ctx) //nolint:errcheck

	callErr := make(chan error, 1)
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		_, err := caller.Call(cctx, rpcEcho{N: 1})
		callErr <- err
	}()

	// Give the call time to establish its session and publish, then drop the
	// underlying socket out from under it.
	time.Sleep(400 * time.Millisecond)
	require.NoError(t, conn.ForceReconnect())

	select {
	case err := <-callErr:
		assert.ErrorIs(t, err, warren.ErrChannelClosed,
			"a mid-call channel close must surface as ErrChannelClosed")
	case <-time.After(15 * time.Second):
		t.Fatal("in-flight call did not resolve after ForceReconnect")
	}
}

// declareQueue declares a classic queue via a raw amqp091 connection so a request
// has somewhere to land in tests that run without a replier.
func declareQueue(t *testing.T, url, queue string) {
	t.Helper()
	rc, err := amqp091.Dial(url)
	require.NoError(t, err)
	defer rc.Close() //nolint:errcheck
	ch, err := rc.Channel()
	require.NoError(t, err)
	defer ch.Close() //nolint:errcheck
	_, err = ch.QueueDeclare(queue, false, false, false, false, nil)
	require.NoError(t, err)
}
