package warren

import (
	"context"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/codec"
)

// echoPayload is a tiny round-trippable JSON payload used to prove the request
// body survives the Caller's encode → publish → reply → decode path unchanged.
type echoPayload struct {
	N int `json:"n"`
}

// fakeCallerChannel is a test double for callerChannel. It records every publish
// and, when echo is enabled, immediately delivers a reply carrying the same body
// and CorrelationId — an in-memory echo server that lets the demultiplexing logic
// be exercised without a live broker. It is safe for concurrent publishers.
type fakeCallerChannel struct {
	mu         sync.Mutex
	deliveries chan amqp091.Delivery
	published  []amqp091.Publishing
	consumed   []consumeArgs
	closed     bool

	echo          bool          // when true, every publish produces a matching reply
	publishSignal chan struct{} // non-blocking notification per publish (nil = ignored)
}

type consumeArgs struct {
	queue   string
	autoAck bool
}

func newFakeCallerChannel(echo bool) *fakeCallerChannel {
	return &fakeCallerChannel{
		deliveries: make(chan amqp091.Delivery, 256),
		echo:       echo,
	}
}

func (f *fakeCallerChannel) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg amqp091.Publishing) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return amqp091.ErrClosed
	}
	f.published = append(f.published, msg)
	echo := f.echo
	sig := f.publishSignal
	dl := f.deliveries
	f.mu.Unlock()

	if echo {
		dl <- amqp091.Delivery{
			CorrelationId: msg.CorrelationId,
			ContentType:   msg.ContentType,
			Body:          msg.Body,
		}
	}
	if sig != nil {
		select {
		case sig <- struct{}{}:
		default:
		}
	}
	return nil
}

func (f *fakeCallerChannel) Consume(queue, _ string, autoAck, _, _, _ bool, _ amqp091.Table) (<-chan amqp091.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumed = append(f.consumed, consumeArgs{queue: queue, autoAck: autoAck})
	return f.deliveries, nil
}

func (f *fakeCallerChannel) QueueDeclare(_ string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	return amqp091.Queue{Name: "reply-q-fake"}, nil
}

func (f *fakeCallerChannel) Qos(int, int, bool) error { return nil }

func (f *fakeCallerChannel) NotifyClose(c chan *amqp091.Error) chan *amqp091.Error { return c }

func (f *fakeCallerChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	close(f.deliveries)
	return nil
}

func (f *fakeCallerChannel) snapshotPublished() []amqp091.Publishing {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]amqp091.Publishing, len(f.published))
	copy(out, f.published)
	return out
}

func (f *fakeCallerChannel) snapshotConsumed() []consumeArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]consumeArgs, len(f.consumed))
	copy(out, f.consumed)
	return out
}

// newTestCaller builds a Caller wired to fake, bypassing the broker-dependent
// builder. mc is left nil; ensureSession skips the reconnect barrier when mc is nil.
func newTestCaller[Req, Resp any](fake *fakeCallerChannel) *Caller[Req, Resp] {
	return &Caller[Req, Resp]{
		codec:      codec.NewJSON(),
		routingKey: "rpc.requests",
		newChannel: func() (callerChannel, error) { return fake, nil },
	}
}

func TestCaller_Call_AutoStampsCorrelationIDAndReplyTo(t *testing.T) {
	defer goleak.VerifyNone(t)
	fake := newFakeCallerChannel(true /* echo */)
	c := newTestCaller[echoPayload, echoPayload](fake)
	defer c.Close(context.Background()) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := c.Call(ctx, echoPayload{N: 7})
	require.NoError(t, err)
	assert.Equal(t, 7, resp.N, "echoed reply should round-trip the request body")

	pubs := fake.snapshotPublished()
	require.Len(t, pubs, 1)
	assert.NotEmpty(t, pubs[0].CorrelationId, "Call must auto-stamp a CorrelationId")
	assert.Equal(t, directReplyToQueue, pubs[0].ReplyTo, "Call must auto-stamp the direct reply-to address")
	assert.NotEmpty(t, pubs[0].MessageId, "request inherits the standard MessageID default")

	// The reply consumer must subscribe to the direct reply-to pseudo-queue with no-ack.
	cons := fake.snapshotConsumed()
	require.Len(t, cons, 1)
	assert.Equal(t, directReplyToQueue, cons[0].queue)
	assert.True(t, cons[0].autoAck, "direct reply-to requires no-ack consumption")
}

func TestCaller_Call_ConcurrentCallsMatchByCorrelationID(t *testing.T) {
	defer goleak.VerifyNone(t)
	fake := newFakeCallerChannel(true /* echo */)
	c := newTestCaller[echoPayload, echoPayload](fake)
	defer c.Close(context.Background()) //nolint:errcheck

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	got := make([]int, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			resp, err := c.Call(ctx, echoPayload{N: i})
			errs[i] = err
			got[i] = resp.N
		}(i)
	}
	wg.Wait()

	for i := range n {
		require.NoErrorf(t, errs[i], "call %d failed", i)
		assert.Equalf(t, i, got[i], "call %d received a mismatched reply (correlation routing broken)", i)
	}

	// All 50 concurrent calls must share a single dedicated channel/subscription.
	assert.Len(t, fake.snapshotConsumed(), 1, "concurrent calls must reuse one reply subscription")
}

func TestCaller_Call_CtxTimeoutReturnsErrCallTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)
	fake := newFakeCallerChannel(false /* no echo: reply never arrives */)
	c := newTestCaller[echoPayload, echoPayload](fake)
	defer c.Close(context.Background()) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := c.Call(ctx, echoPayload{N: 1})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrCallTimeout, "a ctx deadline must surface as ErrCallTimeout")
}

func TestCaller_Call_ChannelCloseReturnsErrChannelClosed(t *testing.T) {
	defer goleak.VerifyNone(t)
	fake := newFakeCallerChannel(false /* no echo */)
	fake.publishSignal = make(chan struct{}, 1)
	c := newTestCaller[echoPayload, echoPayload](fake)
	defer c.Close(context.Background()) //nolint:errcheck

	type result struct {
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		_, err := c.Call(context.Background(), echoPayload{N: 1})
		resCh <- result{err: err}
	}()

	// Wait until the request has been published (session established + waiting),
	// then close the underlying channel out from under the in-flight call.
	select {
	case <-fake.publishSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("publish never happened")
	}
	require.NoError(t, fake.Close())

	select {
	case r := <-resCh:
		assert.ErrorIs(t, r.err, ErrChannelClosed, "a mid-call channel close must surface as ErrChannelClosed")
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight call did not resolve after channel close")
	}
}

func TestCallerBuilder_Build_Validations(t *testing.T) {
	t.Run("nil conn", func(t *testing.T) {
		_, err := CallerFor[echoPayload, echoPayload](nil).Build()
		assert.ErrorIs(t, err, ErrInvalidOptions)
	})

	t.Run("Prefetch requires exclusive reply queue", func(t *testing.T) {
		conn := &Connection{conConns: []*managedConn{{}}}
		_, err := CallerFor[echoPayload, echoPayload](conn).Prefetch(10).Build()
		assert.ErrorIs(t, err, ErrInvalidOptions)
	})
}
