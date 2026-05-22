package amqp

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/amqp/codec"
	"github.com/brunomvsouza/amqp/internal/confirms"
	"github.com/brunomvsouza/amqp/metrics"
)

// — fake pub channel —————————————————————————————————————————————————————

// fakePubChannel is a test double for pubChannel. It is safe for use by AT MOST
// ONE goroutine at a time (matches the pool's exclusive-acquisition contract).
type fakePubChannel struct {
	mu         sync.Mutex
	seq        uint64
	confirmCh  chan amqp091.Confirmation
	closedCh   chan *amqp091.Error
	publishes  []amqp091.Publishing
	autoAck    bool
	publishErr error
}

func newFakePubCh(autoAck bool) *fakePubChannel {
	return &fakePubChannel{
		seq:      1,
		closedCh: make(chan *amqp091.Error, 1),
		autoAck:  autoAck,
	}
}

func (f *fakePubChannel) GetNextPublishSeqNo() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.seq
	f.seq++
	return s
}

func (f *fakePubChannel) Confirm(bool) error { return nil }

func (f *fakePubChannel) NotifyPublish(ch chan amqp091.Confirmation) chan amqp091.Confirmation {
	f.mu.Lock()
	f.confirmCh = ch
	f.mu.Unlock()
	return ch
}

func (f *fakePubChannel) NotifyClose(ch chan *amqp091.Error) chan *amqp091.Error {
	return f.closedCh
}

func (f *fakePubChannel) Close() error {
	f.mu.Lock()
	ch := f.confirmCh
	f.confirmCh = nil
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}

func (f *fakePubChannel) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg amqp091.Publishing) error {
	f.mu.Lock()
	if f.publishErr != nil {
		err := f.publishErr
		f.mu.Unlock()
		return err
	}
	f.publishes = append(f.publishes, msg)
	tag := f.seq - 1 // last tag returned by GetNextPublishSeqNo
	ch := f.confirmCh
	isAutoAck := f.autoAck
	f.mu.Unlock()

	if isAutoAck && ch != nil {
		go func() { ch <- amqp091.Confirmation{DeliveryTag: tag, Ack: true} }()
	}
	return nil
}

func (f *fakePubChannel) sendNack(tag uint64) {
	f.mu.Lock()
	ch := f.confirmCh
	f.mu.Unlock()
	if ch != nil {
		ch <- amqp091.Confirmation{DeliveryTag: tag, Ack: false}
	}
}

func (f *fakePubChannel) lastPublish() (amqp091.Publishing, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.publishes) == 0 {
		return amqp091.Publishing{}, false
	}
	return f.publishes[len(f.publishes)-1], true
}

// — capture metrics ——————————————————————————————————————————————————————

type capturePublisherMetrics struct {
	mu       sync.Mutex
	inFlight []struct {
		exchange string
		delta    int64
	}
	records []struct{ exchange, outcome string }
	retries []struct{ exchange, reason string }
}

func (m *capturePublisherMetrics) InFlightAdd(exchange string, delta int64) {
	m.mu.Lock()
	m.inFlight = append(m.inFlight, struct {
		exchange string
		delta    int64
	}{exchange, delta})
	m.mu.Unlock()
}

func (m *capturePublisherMetrics) RecordPublish(exchange, outcome string, _ time.Duration) {
	m.mu.Lock()
	m.records = append(m.records, struct{ exchange, outcome string }{exchange, outcome})
	m.mu.Unlock()
}

func (m *capturePublisherMetrics) RecordRetry(exchange, reason string) {
	m.mu.Lock()
	m.retries = append(m.retries, struct{ exchange, reason string }{exchange, reason})
	m.mu.Unlock()
}

// — helpers ——————————————————————————————————————————————————————————————

// wireFakePool creates a publisherConnPool of size 1 backed by fake and wires
// the confirm tracker goroutine. stop() closes the goroutine and waits for it
// to exit before returning; it must be called before goleak.VerifyNone.
//
// NOTE: pool size is fixed at 1 so that tests never have concurrent goroutines
// calling methods on the same fake channel (which would break the seq counter).
func wireFakePool(fake *fakePubChannel) (*publisherConnPool, func()) {
	tracker := confirms.New()
	confirmCh := make(chan amqp091.Confirmation, 16)

	fake.mu.Lock()
	fake.confirmCh = confirmCh
	fake.mu.Unlock()

	goroutineDone := make(chan struct{})
	go func() {
		defer close(goroutineDone)
		for c := range confirmCh {
			if c.Ack {
				tracker.Ack(c.DeliveryTag, false)
			} else {
				tracker.Nack(c.DeliveryTag, false)
			}
		}
		tracker.CloseAll()
	}()

	pool := newPublisherConnPool(1, func() (publisherEntry, error) {
		return publisherEntry{ch: fake, tracker: tracker, closeCh: fake.closedCh}, nil
	})

	stop := func() {
		_ = fake.Close() // closes confirmCh → goroutine exits
		<-goroutineDone  // wait for full exit
	}
	return pool, stop
}

type testPayload struct {
	Value string `json:"value"`
}

// newTestPub builds a Publisher[M] with a single fake-backed pool.
// IMPORTANT: caller must defer stopPool() BEFORE defer goleak.VerifyNone(t)
// (register goleak first so it runs last due to LIFO).
func newTestPub[M any](fake *fakePubChannel, pm metrics.PublisherMetrics) (*Publisher[M], *publisherConnPool, func()) {
	pool, stopPool := wireFakePool(fake)
	mc := &managedConn{}
	pub := &Publisher[M]{
		pools: []*publisherConnPool{pool}, mcs: []*managedConn{mc},
		codec: codec.NewJSON(), pm: pm, exchange: "x",
	}
	return pub, pool, stopPool
}

// — tests ————————————————————————————————————————————————————————————————

func TestPublisher_Publish_succeedsOnAck(t *testing.T) {
	fake := newFakePubCh(true /* autoAck */)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	// LIFO: goleak registered first → runs last; close goroutines first.
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{Value: "hello"}})
	require.NoError(t, err)
}

func TestPublisher_Publish_encodesBody(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	v := testPayload{Value: "world"}
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &v}))

	p, ok := fake.lastPublish()
	require.True(t, ok)
	assert.Contains(t, string(p.Body), `"world"`)
	assert.Equal(t, "application/json", p.ContentType)
}

func TestPublisher_Publish_populatesMessageID(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}}))

	p, ok := fake.lastPublish()
	require.True(t, ok)
	assert.NotEmpty(t, p.MessageId, "MessageId must be auto-populated")
}

func TestPublisher_Publish_returnsErrPublishNacked(t *testing.T) {
	fake := newFakePubCh(false /* manual ack */)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	go func() {
		time.Sleep(5 * time.Millisecond)
		fake.sendNack(1)
	}()

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPublishNacked), "expected ErrPublishNacked, got %v", err)
}

func TestPublisher_Publish_returnsErrChannelClosed(t *testing.T) {
	fake := newFakePubCh(false)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	// stopPool waits for goroutine exit; call before goleak.
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	go func() {
		time.Sleep(5 * time.Millisecond)
		_ = fake.Close()
	}()

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrChannelClosed), "expected ErrChannelClosed, got %v", err)
}

func TestPublisher_Publish_returnsErrChannelPoolExhausted(t *testing.T) {
	fake := newFakePubCh(false)
	pub, pool, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})

	// All goroutines exit naturally before defers run because we wait for them.
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	blockRelease := make(chan struct{})
	released := make(chan struct{})

	// First goroutine holds the single pool slot indefinitely.
	started := make(chan struct{})
	go func() {
		entry, release, err := pool.acquire(context.Background())
		if err != nil {
			close(started)
			return
		}
		pool.inflight.Add(1)
		close(started)
		<-blockRelease
		pool.inflight.Add(-1)
		release()
		_ = entry
		close(released)
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := pub.Publish(ctx, Message[testPayload]{Body: &testPayload{}})

	// Unblock holder and wait for it to release (so pool is clean for defers).
	close(blockRelease)
	<-released

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrChannelPoolExhausted), "expected ErrChannelPoolExhausted, got %v", err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "ctx.Err() must be unwrappable from pool exhaustion error, got %v", err)
}

func TestPublisher_Publish_metricsInFlight(t *testing.T) {
	fake := newFakePubCh(true)
	pm := &capturePublisherMetrics{}
	pub, _, stopPool := newTestPub[testPayload](fake, pm)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.exchange = "ex"
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}}))

	pm.mu.Lock()
	defer pm.mu.Unlock()
	require.Len(t, pm.inFlight, 2, "expected +1 and -1 InFlightAdd calls")
	assert.Equal(t, int64(+1), pm.inFlight[0].delta)
	assert.Equal(t, int64(-1), pm.inFlight[1].delta)
}

func TestPublisher_Publish_metricsRecordPublish(t *testing.T) {
	fake := newFakePubCh(true)
	pm := &capturePublisherMetrics{}
	pub, _, stopPool := newTestPub[testPayload](fake, pm)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.exchange = "ex"
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}}))

	pm.mu.Lock()
	defer pm.mu.Unlock()
	require.Len(t, pm.records, 1)
	assert.Equal(t, "ex", pm.records[0].exchange)
	assert.Equal(t, "success", pm.records[0].outcome)
}

func TestPublisher_Close_drainsInFlight(t *testing.T) {
	fake := newFakePubCh(false)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	}()

	// Give Publish time to block on tracker.Wait.
	time.Sleep(20 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		_ = pub.Close(context.Background())
	}()

	// Close should be blocked while Publish is in flight.
	select {
	case <-closeDone:
		t.Fatal("Close returned before Publish completed")
	case <-time.After(30 * time.Millisecond):
		// expected: Close is still waiting
	}

	// Unblock Publish by nacking.
	fake.sendNack(1)

	<-publishDone
	<-closeDone // Close should now return
}

func TestPublisher_Publish_race(t *testing.T) {
	// Verify concurrent Publish calls are data-race-free.
	// Uses pool size=1 to ensure serial channel use (valid because the race test
	// exercises Publisher internals, not throughput).
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	const goroutines = 64
	const publishesEach = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range publishesEach {
				err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{Value: "x"}})
				if err != nil && !errors.Is(err, ErrAlreadyClosed) {
					t.Errorf("Publish failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestPublisherFor_applyBuilderDefaults(t *testing.T) {
	b := &PublisherBuilder[testPayload]{exchange: "x"}
	b.applyBuilderDefaults()
	assert.NotNil(t, b.c, "codec should be set")
	assert.NotNil(t, b.pm, "metrics should be set")
}

func TestPublisherBuilder_lastWins_codec(t *testing.T) {
	first := codec.NewJSON()
	second := codec.NewJSONLax()
	b := PublisherFor[testPayload](nil).Codec(first).Codec(second)
	assert.Equal(t, second, b.c, "last Codec call should win")
}

func TestPublisherBuilder_withoutMetrics_wins(t *testing.T) {
	pm := &capturePublisherMetrics{}
	b := PublisherFor[testPayload](nil).Metrics(pm).WithoutMetrics()
	b.applyBuilderDefaults()
	_, isNoop := b.pm.(metrics.NoOpPublisherMetrics)
	assert.True(t, isNoop, "WithoutMetrics should override Metrics")
}

func TestPublisherBuilder_Build_returnsErrInvalidOptions_onNilConn(t *testing.T) {
	_, err := PublisherFor[testPayload](nil).Build()
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidOptions), "expected ErrInvalidOptions, got %v", err)
}

func TestPublisherBuilder_Exchange_lastWins(t *testing.T) {
	b := PublisherFor[testPayload](nil).Exchange("a").Exchange("b")
	assert.Equal(t, "b", b.exchange)
}

func TestPublisherBuilder_RoutingKey_lastWins(t *testing.T) {
	b := PublisherFor[testPayload](nil).RoutingKey("rk1").RoutingKey("rk2")
	assert.Equal(t, "rk2", b.routingKey)
}

func TestPublisher_Publish_returnsErrAlreadyClosed(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()

	require.NoError(t, pub.Close(context.Background()))

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAlreadyClosed))
}

func TestPublisher_Close_returnsErrAlreadyClosed_onSecondCall(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()

	require.NoError(t, pub.Close(context.Background()))
	err := pub.Close(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrAlreadyClosed))
}

func TestPublisher_Publish_returnsErrConfirmTimeout(t *testing.T) {
	fake := newFakePubCh(false /* manual — no confirm ever sent */)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// Set a very short timeout directly on the struct so the test is deterministic.
	pub.confirmTimeout = 10 * time.Millisecond

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrConfirmTimeout), "expected ErrConfirmTimeout, got %v", err)
}

func TestPublisher_Publish_returnsErrUnroutable(t *testing.T) {
	// Use a fresh pool wired directly to a tracker we control so we can call
	// MarkReturned before the ack arrives (wireFakePool hides the tracker).
	defer goleak.VerifyNone(t)

	tracker := confirms.New()
	confirmCh := make(chan amqp091.Confirmation, 4)
	fake2 := newFakePubCh(false)
	fake2.mu.Lock()
	fake2.confirmCh = confirmCh
	fake2.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range confirmCh {
			if c.Ack {
				tracker.Ack(c.DeliveryTag, false)
			} else {
				tracker.Nack(c.DeliveryTag, false)
			}
		}
		tracker.CloseAll()
	}()
	defer func() { _ = fake2.Close(); <-done }()

	pool2 := newPublisherConnPool(1, func() (publisherEntry, error) {
		return publisherEntry{ch: fake2, tracker: tracker, closeCh: fake2.closedCh}, nil
	})

	pub2 := &Publisher[testPayload]{
		pools: []*publisherConnPool{pool2}, mcs: []*managedConn{{}},
		codec: codec.NewJSON(), pm: metrics.NoOpPublisherMetrics{}, exchange: "x",
		confirmTimeout: 2 * time.Second,
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		// Mark tag 1 as returned, then send ack.
		tracker.MarkReturned(1, 312)
		confirmCh <- amqp091.Confirmation{DeliveryTag: 1, Ack: true}
	}()

	err := pub2.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnroutable), "expected ErrUnroutable, got %v", err)
}

func TestPublisher_Publish_returnsErrInvalidMessage_onEncodeFailure(t *testing.T) {
	// Channels cannot be JSON-encoded. Encode fails before pool acquisition,
	// so a pool is not needed for this test.
	type badPayload struct {
		Ch chan int `json:"ch"`
	}
	pub := &Publisher[badPayload]{
		pools: []*publisherConnPool{}, mcs: []*managedConn{},
		codec: codec.NewJSON(), pm: metrics.NoOpPublisherMetrics{}, exchange: "x",
	}

	err := pub.Publish(context.Background(), Message[badPayload]{Body: &badPayload{Ch: make(chan int)}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidMessage), "expected ErrInvalidMessage, got %v", err)
}

func TestPublisher_Publish_returnsErrOnPublishWithContextFailure(t *testing.T) {
	fake := newFakePubCh(false)
	fake.publishErr = errors.New("network gone")
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	// The tracker slot must be cancelled — publish again to verify pool is usable.
	fake.publishErr = nil
	fake.autoAck = true
	err2 := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	assert.NoError(t, err2, "pool must be usable after a PublishWithContext failure")
}

func TestPublisherConnPool_drain_discardsFreeEntries(t *testing.T) {
	fake := newFakePubCh(true)
	pool, stopPool := wireFakePool(fake)
	defer stopPool()

	// Acquire, release back to free queue, then drain.
	entry, release, err := pool.acquire(context.Background())
	require.NoError(t, err)
	release()
	_ = entry

	pool.drain()
	assert.Equal(t, 0, len(pool.free), "drain must empty the free queue")
}

func TestPublisher_selectPool_choosesLeastInFlight(t *testing.T) {
	fake1 := newFakePubCh(true)
	fake2 := newFakePubCh(true)
	pool1, stop1 := wireFakePool(fake1)
	pool2, stop2 := wireFakePool(fake2)
	defer stop1()
	defer stop2()

	pub := &Publisher[testPayload]{
		pools: []*publisherConnPool{pool1, pool2},
		mcs:   []*managedConn{{}, {}},
	}

	// pool1 has 2 in-flight, pool2 has 0 — selectPool should pick pool2.
	pool1.inflight.Store(2)
	pool2.inflight.Store(0)

	chosen, _ := pub.selectPool()
	assert.Same(t, pool2, chosen, "selectPool should return the pool with fewer in-flight")
}

func TestAmqpCodeTable_coversAllExpectedCodes(t *testing.T) {
	expected := []uint16{311, 320, 402, 403, 404, 405, 406, 501, 502, 503, 504, 505, 506, 530, 540, 541}
	for _, code := range expected {
		_, ok := amqpCodeTable[code]
		assert.True(t, ok, "amqpCodeTable missing code %d", code)
	}
	assert.Len(t, amqpCodeTable, len(expected), "amqpCodeTable has unexpected entries")
}
