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
	qosPrefetch   int           // records the last basic.qos prefetch count
	qosSize       int           // records the last basic.qos prefetch_size (bytes); must stay 0 (T78/G6)
	qosCalled     bool          // records whether Qos was invoked at all
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

func (f *fakeCallerChannel) Qos(prefetch, size int, _ bool) error {
	f.mu.Lock()
	f.qosPrefetch = prefetch
	f.qosSize = size
	f.qosCalled = true
	f.mu.Unlock()
	return nil
}

func (f *fakeCallerChannel) snapshotQos() (prefetch, size int, called bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.qosPrefetch, f.qosSize, f.qosCalled
}

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

// TestCaller_Call_ReopensSessionAfterChannelDeath proves the Caller's reconnect
// recovery: after a channel death resolves an in-flight call with ErrChannelClosed,
// the NEXT Call must transparently open a fresh session (re-subscribe) and succeed.
// This is the entire reconnect story for RPC on the caller side — promised in the
// Caller type docs but otherwise unverified.
func TestCaller_Call_ReopensSessionAfterChannelDeath(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake1 := newFakeCallerChannel(false /* no echo: this session will die */)
	fake1.publishSignal = make(chan struct{}, 1)
	fake2 := newFakeCallerChannel(true /* echo: the reopened session round-trips */)
	fakes := []*fakeCallerChannel{fake1, fake2}

	var mu sync.Mutex
	var opens int
	c := &Caller[echoPayload, echoPayload]{
		codec:      codec.NewJSON(),
		routingKey: "rpc.requests",
		newChannel: func() (callerChannel, error) {
			mu.Lock()
			defer mu.Unlock()
			f := fakes[opens]
			opens++
			return f, nil
		},
	}
	defer c.Close(context.Background()) //nolint:errcheck

	// First call establishes session #1; kill its channel so the call resolves with
	// ErrChannelClosed.
	resCh := make(chan error, 1)
	go func() {
		_, err := c.Call(context.Background(), echoPayload{N: 1})
		resCh <- err
	}()
	select {
	case <-fake1.publishSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("first publish never happened")
	}
	require.NoError(t, fake1.Close())
	select {
	case err := <-resCh:
		require.ErrorIs(t, err, ErrChannelClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("first call did not resolve after channel close")
	}

	// Second call: ensureSession must detect the dead session and open session #2.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.Call(ctx, echoPayload{N: 7})
	require.NoError(t, err)
	assert.Equal(t, 7, resp.N, "the reopened session must round-trip the reply")

	mu.Lock()
	got := opens
	mu.Unlock()
	assert.Equal(t, 2, got, "a dead session must trigger exactly one reopen (2 channels opened)")

	// Both sessions subscribed to the direct reply-to pseudo-queue independently.
	assert.Len(t, fake2.snapshotConsumed(), 1, "session #2 issues its own subscription")
}

// TestCaller_Close_IsIdempotent asserts the first Close succeeds and a second
// returns ErrAlreadyClosed.
func TestCaller_Close_IsIdempotent(t *testing.T) {
	defer goleak.VerifyNone(t)
	fake := newFakeCallerChannel(true /* echo */)
	c := newTestCaller[echoPayload, echoPayload](fake)

	// Drive one call so a live session/dispatcher exists to be torn down.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.Call(ctx, echoPayload{N: 1})
	require.NoError(t, err)

	require.NoError(t, c.Close(context.Background()), "first Close releases the channel")
	assert.ErrorIs(t, c.Close(context.Background()), ErrAlreadyClosed, "second Close returns ErrAlreadyClosed")
}

// TestCaller_Health_NilConnReturnsErrNotConnected covers the unit-construction path
// where a Caller has no pinned connection.
func TestCaller_Health_NilConnReturnsErrNotConnected(t *testing.T) {
	c := newTestCaller[echoPayload, echoPayload](newFakeCallerChannel(false))
	assert.ErrorIs(t, c.Health(context.Background()), ErrNotConnected,
		"a Caller with no pinned connection reports ErrNotConnected")
}

// TestCaller_Call_ExclusiveReplyQueue_DeclaresQueueAndAppliesQos exercises the
// UseExclusiveReplyQueue branch of openSession in the fast lane: a real exclusive
// reply queue is declared, basic.qos is applied with the configured prefetch, the
// reply consumer uses regular (non-auto) acks, and requests carry the declared
// queue name as ReplyTo (not the direct reply-to pseudo-queue).
func TestCaller_Call_ExclusiveReplyQueue_DeclaresQueueAndAppliesQos(t *testing.T) {
	defer goleak.VerifyNone(t)
	fake := newFakeCallerChannel(true /* echo */)
	c := &Caller[echoPayload, echoPayload]{
		codec:             codec.NewJSON(),
		routingKey:        "rpc.requests",
		useExclusiveQueue: true,
		prefetch:          10,
		newChannel:        func() (callerChannel, error) { return fake, nil },
	}
	defer c.Close(context.Background()) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.Call(ctx, echoPayload{N: 5})
	require.NoError(t, err)
	assert.Equal(t, 5, resp.N)

	cons := fake.snapshotConsumed()
	require.Len(t, cons, 1)
	assert.Equal(t, "reply-q-fake", cons[0].queue, "exclusive mode consumes the declared queue")
	assert.False(t, cons[0].autoAck, "an exclusive reply queue uses regular acks")

	prefetch, size, called := fake.snapshotQos()
	assert.True(t, called, "basic.qos must be applied in exclusive mode")
	assert.Equal(t, 10, prefetch, "the configured Prefetch must be passed to basic.qos")
	assert.Equal(t, 0, size, "basic.qos prefetch_size (bytes) must always be 0 (RabbitMQ rejects non-zero with 540, G6/T78)")

	pubs := fake.snapshotPublished()
	require.Len(t, pubs, 1)
	assert.Equal(t, "reply-q-fake", pubs[0].ReplyTo, "ReplyTo must be the declared queue, not amq.rabbitmq.reply-to")
}

// TestCaller_Qos_PrefetchSizeAlwaysZero is the T78/RMQ-04 guard: warren never
// sends a non-zero AMQP basic.qos prefetch_size (bytes). RabbitMQ rejects a
// non-zero per-consumer prefetch_size with 540 NOT_IMPLEMENTED on both 3.13 and
// 4.x (gate G6), so PrefetchBytes is dropped client-side and every Qos call must
// pass size=0. This locks the behaviour regardless of the configured Prefetch.
func TestCaller_Qos_PrefetchSizeAlwaysZero(t *testing.T) {
	defer goleak.VerifyNone(t)
	fake := newFakeCallerChannel(true /* echo */)
	c := &Caller[echoPayload, echoPayload]{
		codec:             codec.NewJSON(),
		routingKey:        "rpc.requests",
		useExclusiveQueue: true,
		prefetch:          250,
		newChannel:        func() (callerChannel, error) { return fake, nil },
	}
	defer c.Close(context.Background()) //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := c.Call(ctx, echoPayload{N: 1})
	require.NoError(t, err)

	_, size, called := fake.snapshotQos()
	require.True(t, called, "basic.qos must be applied in exclusive mode")
	assert.Equal(t, 0, size, "prefetch_size must be 0; a non-zero value is rejected by RabbitMQ (540, G6)")
}

// TestCallerBuilder_OptionsAreLastWins asserts the last-wins option policy that
// CLAUDE.md lists as a builder invariant (calling a setter twice keeps the final).
func TestCallerBuilder_OptionsAreLastWins(t *testing.T) {
	b := CallerFor[echoPayload, echoPayload](nil).
		Exchange("x1").Exchange("x2").
		RoutingKey("rk1").RoutingKey("rk2").
		Prefetch(1).Prefetch(9)
	assert.Equal(t, "x2", b.exchange)
	assert.Equal(t, "rk2", b.routingKey)
	assert.Equal(t, uint16(9), b.prefetch)
	assert.True(t, b.prefetchSet)

	strict := codec.NewJSONStrict()
	b2 := CallerFor[echoPayload, echoPayload](nil).Codec(codec.NewJSON()).Codec(strict)
	assert.Equal(t, strict, b2.c, "the last Codec wins")
}
