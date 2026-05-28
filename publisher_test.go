package warren

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

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/internal/confirms"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// — fake pub channel —————————————————————————————————————————————————————

// fakePubChannel is a test double for pubChannel. It is safe for use by AT MOST
// ONE goroutine at a time (matches the pool's exclusive-acquisition contract).
type fakePubChannel struct {
	mu          sync.Mutex
	seq         uint64
	confirmCh   chan amqp091.Confirmation
	returnCh    chan amqp091.Return // set by wireFakePool / NotifyReturn
	closedCh    chan *amqp091.Error
	publishes   []amqp091.Publishing
	mandatories []bool // tracks the mandatory flag passed per publish
	autoAck     bool
	publishErr  error
	// returnAll, when true, sends a basic.return (reply code 312) for every
	// mandatory publish before sending the auto-ack. Useful for testing
	// Mandatory() + Publish / PublishBatch with unroutable messages.
	returnAll bool
	// returnMsgIDs maps MessageID → reply-code. A mandatory publish whose
	// MessageID is present in this map receives a basic.return with that
	// reply code before the auto-ack. Takes precedence over returnAll.
	returnMsgIDs map[string]uint16
	// returnEmptyMsgID, when true, sends the basic.return with an empty MessageId
	// regardless of the actual message's ID. Used to test the uncorrelatable-return
	// path (openPublisherEntry's guard: if ret.MessageId != "").
	returnEmptyMsgID bool
	// failAtTag, if > 0, causes PublishWithContext to return an error when the
	// delivery tag of the current publish matches. Used to simulate mid-batch
	// PublishWithContext failures without affecting other messages in the batch.
	failAtTag uint64
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

func (f *fakePubChannel) NotifyReturn(ch chan amqp091.Return) chan amqp091.Return {
	f.mu.Lock()
	f.returnCh = ch
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

func (f *fakePubChannel) PublishWithContext(_ context.Context, _, _ string, mandatory, _ bool, msg amqp091.Publishing) error {
	f.mu.Lock()
	if f.publishErr != nil {
		err := f.publishErr
		f.mu.Unlock()
		return err
	}
	tag := f.seq - 1 // last tag returned by GetNextPublishSeqNo

	// failAtTag allows individual messages within a batch to be failed at the
	// PublishWithContext layer without affecting other messages.
	if f.failAtTag > 0 && tag == f.failAtTag {
		f.mu.Unlock()
		return errors.New("simulated targeted publish failure")
	}

	f.publishes = append(f.publishes, msg)
	f.mandatories = append(f.mandatories, mandatory)
	ch := f.confirmCh
	returnCh := f.returnCh
	isAutoAck := f.autoAck

	// Determine whether to simulate a basic.return for this message.
	var replyCode uint16
	if mandatory {
		if code, ok := f.returnMsgIDs[msg.MessageId]; ok {
			replyCode = code
		} else if f.returnAll {
			replyCode = 312 // NO_ROUTE
		}
	}

	// returnEmptyMsgID overrides the MessageId in the return frame with an empty
	// string, simulating a broker that omits MessageId in basic.return.
	returnMsgID := msg.MessageId
	if f.returnEmptyMsgID {
		returnMsgID = ""
	}
	f.mu.Unlock()

	if replyCode != 0 && returnCh != nil {
		// Send basic.return BEFORE basic.ack (RabbitMQ wire guarantee).
		// returnCh is unbuffered (see wireFakePool) so this call blocks until the
		// pool goroutine reads the return and calls tracker.MarkReturned. That
		// ensures MarkReturned always precedes the subsequent Ack in the tracker.
		returnCh <- amqp091.Return{
			ReplyCode: replyCode,
			ReplyText: "NO_ROUTE",
			MessageId: returnMsgID,
		}
	}
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

func (f *fakePubChannel) lastMandatory() (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.mandatories) == 0 {
		return false, false
	}
	return f.mandatories[len(f.mandatories)-1], true
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
// the merged confirm+return goroutine. stop() closes the goroutine and waits
// for it to exit before returning; it must be called before goleak.VerifyNone.
//
// The optional onReturn callback is invoked (synchronously, in the goroutine)
// when a basic.return arrives and before tracker.MarkReturned is called. Pass it
// to verify that the Publisher's OnReturn handler fires in batch mandatory tests.
//
// NOTE: pool size is fixed at 1 so that tests never have concurrent goroutines
// calling methods on the same fake channel (which would break the seq counter).
func wireFakePool(fake *fakePubChannel, onReturn ...func(amqp091.Return)) (*publisherConnPool, func()) {
	var returnCallback func(amqp091.Return)
	if len(onReturn) > 0 {
		returnCallback = onReturn[0]
	}

	tracker := confirms.New()
	confirmCh := make(chan amqp091.Confirmation, 16)
	// returnCh is intentionally unbuffered: PublishWithContext sends to it
	// synchronously (blocking until this goroutine reads), which guarantees that
	// tracker.MarkReturned is always called before the subsequent basic.ack is
	// processed — mirroring the RabbitMQ wire ordering guarantee.
	returnCh := make(chan amqp091.Return)

	rtm := new(sync.Map)

	fake.mu.Lock()
	fake.confirmCh = confirmCh
	fake.returnCh = returnCh
	fake.mu.Unlock()

	goroutineDone := make(chan struct{})
	go func() {
		defer close(goroutineDone)
		for {
			select {
			case ret, ok := <-returnCh:
				if !ok {
					returnCh = nil
					continue
				}
				// Correlate the return to its delivery tag via MessageID.
				if ret.MessageId != "" {
					if v, loaded := rtm.LoadAndDelete(ret.MessageId); loaded {
						tag := v.(uint64) //nolint:forcetypeassert // only uint64 stored
						if returnCallback != nil {
							returnCallback(ret)
						}
						tracker.MarkReturned(tag, ret.ReplyCode)
					}
				}
			case c, ok := <-confirmCh:
				if !ok {
					tracker.CloseAll()
					return
				}
				if c.Ack {
					tracker.Ack(c.DeliveryTag, false)
				} else {
					tracker.Nack(c.DeliveryTag, false)
				}
			}
		}
	}()

	pool := newPublisherConnPool(1, func() (publisherEntry, error) {
		return publisherEntry{ch: fake, tracker: tracker, closeCh: fake.closedCh, returnTagMap: rtm}, nil
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
		tracer: otel.NoOpTracer{},
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

func TestPublisherConnPool_acquire_wrapsContextCanceledInPoolExhausted(t *testing.T) {
	// Verify that explicit context cancellation (not timeout) is also wrapped inside
	// ErrChannelPoolExhausted so callers can distinguish via errors.Is.
	fake := newFakePubCh(false)
	pool, stopPool := wireFakePool(fake)
	defer goleak.VerifyNone(t)
	defer stopPool()

	blockRelease := make(chan struct{})
	released := make(chan struct{})

	started := make(chan struct{})
	go func() {
		entry, release, err := pool.acquire(context.Background())
		if err != nil {
			close(started)
			return
		}
		close(started)
		<-blockRelease
		release()
		_ = entry
		close(released)
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // explicit cancel, not a deadline

	_, _, err := pool.acquire(ctx)

	close(blockRelease)
	<-released

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrChannelPoolExhausted), "expected ErrChannelPoolExhausted, got %v", err)
	assert.True(t, errors.Is(err, context.Canceled), "ctx.Err() must be unwrappable (context.Canceled), got %v", err)
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
	second := codec.NewJSONStrict()
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
	// returnAll=true causes the fake to send basic.return (via the unbuffered
	// returnCh) before the auto-ack for every mandatory publish, exercising the
	// full returnTagMap correlation path in openPublisherEntry's goroutine.
	fake := newFakePubCh(true /* autoAck — sends ack after return */)
	fake.returnAll = true

	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.mandatory = true

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
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
		tracer: otel.NoOpTracer{},
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

// TestPublisher_Publish_basicReturn_emptyMessageId_resolvesAsSuccess locks in the
// documented contract at publisher.go: when a broker sends basic.return with an
// empty MessageId the return cannot be correlated (the guard `if ret.MessageId != ""`
// skips it). The subsequent basic.ack must resolve the publish as nil (success),
// not ErrUnroutable. This prevents a refactor from accidentally removing the guard.
func TestPublisher_Publish_basicReturn_emptyMessageId_resolvesAsSuccess(t *testing.T) {
	fake := newFakePubCh(true /* autoAck — sends ack after return */)
	fake.returnAll = true        // all mandatory publishes trigger a return
	fake.returnEmptyMsgID = true // but the return carries an empty MessageId

	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.mandatory = true

	// Even though a basic.return arrives, the empty MessageId means it cannot be
	// correlated to any in-flight message. The subsequent ack must resolve the
	// publish as nil (success), not ErrUnroutable.
	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.NoError(t, err, "basic.return with empty MessageId must not surface ErrUnroutable")
}

// TestPublisherConnPool_release_discardsStaleChannel verifies that when the channel
// closure signal arrives on entry.closeCh while the entry is held, release discards
// the entry (does not return it to the free queue) while still returning the
// semaphore token so the pool remains usable.
func TestPublisherConnPool_release_discardsStaleChannel(t *testing.T) {
	fake := newFakePubCh(false)
	pool, stopPool := wireFakePool(fake)
	defer goleak.VerifyNone(t)
	defer stopPool()

	// Acquire the single pool entry.
	entry, release, err := pool.acquire(context.Background())
	require.NoError(t, err)

	// Simulate a channel-level error arriving while the entry is held: send on
	// closeCh exactly as managedConn does when the AMQP channel closes.
	entry.closeCh <- &amqp091.Error{Code: 504, Reason: "channel/connection is not open"}

	// Release must detect the signal on closeCh and discard the entry.
	release()

	// free queue must be empty: the stale entry was discarded, not returned.
	assert.Equal(t, 0, len(pool.free), "release must discard entry with closed-channel signal")
}

// TestPublisherConnPool_acquire_returnsTokenOnOpenFnPanic verifies that when
// openFn panics during acquire, the semaphore token is returned to the pool so
// subsequent acquire calls are not permanently blocked. Without the fix
// (LATER-04), the token leaks and the pool becomes exhausted after a single panic.
func TestPublisherConnPool_acquire_returnsTokenOnOpenFnPanic(t *testing.T) {
	panicked := false
	pool := newPublisherConnPool(1, func() (publisherEntry, error) {
		if !panicked {
			panicked = true
			panic("simulated openFn panic")
		}
		// Second call: return a valid entry.
		fake := newFakePubCh(true)
		return publisherEntry{
			ch:           fake,
			tracker:      nil,
			closeCh:      fake.closedCh,
			returnTagMap: new(sync.Map),
		}, nil
	})

	// First acquire: openFn panics. Recover and verify the panic surfaces.
	func() {
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from openFn")
			assert.Equal(t, "simulated openFn panic", r)
		}()
		_, _, _ = pool.acquire(context.Background())
	}()

	// Second acquire: must succeed within a short deadline. If the token was
	// lost (pre-fix), this blocks until the context times out.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, release, err := pool.acquire(ctx)
	require.NoError(t, err, "acquire after panic must succeed — token must be returned")
	release()
}

// TestPublisherConnPool_acquire_returnsTokenOnOpenFnError verifies that when
// openFn returns an error (non-panic) during acquire, the semaphore token is
// returned so subsequent acquire calls are not permanently blocked.
// This covers the normal error path; the panic path is covered by
// TestPublisherConnPool_acquire_returnsTokenOnOpenFnPanic.
func TestPublisherConnPool_acquire_returnsTokenOnOpenFnError(t *testing.T) {
	callCount := 0
	pool := newPublisherConnPool(1, func() (publisherEntry, error) {
		callCount++
		if callCount == 1 {
			return publisherEntry{}, errors.New("simulated openFn error")
		}
		// Second call: return a valid entry.
		fake := newFakePubCh(true)
		return publisherEntry{
			ch:           fake,
			tracker:      nil,
			closeCh:      fake.closedCh,
			returnTagMap: new(sync.Map),
		}, nil
	})

	// First acquire: openFn returns an error. The token must be returned.
	_, _, err := pool.acquire(context.Background())
	require.Error(t, err, "expected error from openFn")

	// Second acquire: must succeed within a short deadline. If the token was
	// lost (pre-fix), this blocks until the context times out.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, release, err := pool.acquire(ctx)
	require.NoError(t, err, "acquire after error must succeed — token must be returned")
	release()
}
