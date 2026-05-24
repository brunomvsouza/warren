package warren

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/internal/confirms"
	"github.com/brunomvsouza/warren/metrics"
)

// wireReturnPool creates a pool for any pubChannel that supports NotifyReturn.
// It starts a merged confirm+return goroutine; stop must be deferred before goleak.
func wireReturnPool(ch pubChannel, onReturn func(amqp091.Return)) (*publisherConnPool, *confirms.Tracker, func()) {
	tracker := confirms.New()
	confirmCh := make(chan amqp091.Confirmation, 16)
	// returnCh is intentionally unbuffered (mirrors wireFakePool): the fake's
	// PublishWithContext sends synchronously, which blocks until this goroutine
	// reads the return. That guarantees tracker.MarkReturned is always called
	// before the subsequent basic.ack is processed — no time.Sleep required.
	returnCh := make(chan amqp091.Return)
	_ = ch.NotifyPublish(confirmCh)
	_ = ch.NotifyReturn(returnCh)

	rtm := new(sync.Map)

	done := make(chan struct{})
	go func() {
		defer close(done)
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
						if onReturn != nil {
							onReturn(ret)
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

	closeCh := ch.NotifyClose(make(chan *amqp091.Error, 1))
	entry := publisherEntry{ch: ch, tracker: tracker, closeCh: closeCh, returnTagMap: rtm}

	pool := newPublisherConnPool(1, func() (publisherEntry, error) {
		return entry, nil
	})

	stop := func() {
		_ = ch.Close()
		<-done
	}
	return pool, tracker, stop
}

// newTestPubReturn builds a Publisher[M] backed by a return-aware fake channel.
// onReturn is the user-level callback (func(Return)); the pool goroutine calls
// pub.callOnReturn which converts the raw amqp091.Return and invokes it.
func newTestPubReturn[M any](ch pubChannel, pm metrics.PublisherMetrics, onReturn func(Return)) (*Publisher[M], func()) {
	mc := &managedConn{}
	pub := &Publisher[M]{
		mcs:            []*managedConn{mc},
		codec:          codec.NewJSON(),
		pm:             pm,
		exchange:       "x",
		confirmTimeout: 2 * time.Second,
		onReturn:       onReturn,
	}
	// Pass pub.callOnReturn so that the goroutine uses the Publisher's conversion logic.
	pool, _, stopPool := wireReturnPool(ch, pub.callOnReturn)
	pub.pools = []*publisherConnPool{pool}
	return pub, stopPool
}

// — builder last-wins tests ——————————————————————————————————————————————————

func TestPublisherBuilder_Mandatory_sets(t *testing.T) {
	b := PublisherFor[testPayload](nil).Mandatory()
	assert.True(t, b.mandatory)
}

func TestPublisherBuilder_OnReturn_lastWins(t *testing.T) {
	cb1 := func(Return) {}
	cb2 := func(Return) {}
	b := PublisherFor[testPayload](nil).OnReturn(cb1).OnReturn(cb2)
	// Can't compare functions directly; just verify it was set.
	assert.NotNil(t, b.onReturn)
}

func TestPublisherBuilder_ConfirmTimeout_lastWins(t *testing.T) {
	b := PublisherFor[testPayload](nil).ConfirmTimeout(5 * time.Second).ConfirmTimeout(10 * time.Second)
	assert.Equal(t, 10*time.Second, b.confirmTimeout)
}

func TestPublisherBuilder_ConfirmTimeout_zero_disables(t *testing.T) {
	b := PublisherFor[testPayload](nil).ConfirmTimeout(0)
	assert.Equal(t, time.Duration(0), b.confirmTimeout)
}

func TestPublisherBuilder_PublishTimeout_lastWins(t *testing.T) {
	b := PublisherFor[testPayload](nil).PublishTimeout(3 * time.Second).PublishTimeout(7 * time.Second)
	assert.Equal(t, 7*time.Second, b.publishTimeout)
}

func TestPublisherBuilder_PublishBatchMaxSize_lastWins(t *testing.T) {
	b := PublisherFor[testPayload](nil).PublishBatchMaxSize(512).PublishBatchMaxSize(2048)
	assert.Equal(t, 2048, b.publishBatchMaxSize)
}

func TestPublisherBuilder_PublishRetry_lastWins(t *testing.T) {
	p1 := RetryPolicy{Min: time.Second}
	p2 := RetryPolicy{Min: 2 * time.Second}
	b := PublisherFor[testPayload](nil).PublishRetry(p1).PublishRetry(p2)
	assert.Equal(t, p2, b.retryPolicy)
}

func TestPublisherBuilder_StampUserID_sets(t *testing.T) {
	b := PublisherFor[testPayload](nil).StampUserID()
	assert.True(t, b.stampUserID)
}

func TestPublisherBuilder_applyBuilderDefaults_setsConfirmTimeout(t *testing.T) {
	b := &PublisherBuilder[testPayload]{}
	b.applyBuilderDefaults()
	assert.Equal(t, defaultConfirmTimeout, b.confirmTimeout, "default confirm timeout should be 30s")
}

func TestPublisherBuilder_applyBuilderDefaults_zeroConfirmTimeout_preserved(t *testing.T) {
	// Explicit ConfirmTimeout(0) must NOT be replaced by default in applyBuilderDefaults.
	b := &PublisherBuilder[testPayload]{confirmTimeoutSet: true, confirmTimeout: 0}
	b.applyBuilderDefaults()
	assert.Equal(t, time.Duration(0), b.confirmTimeout, "explicit zero must disable confirm timeout")
}

// — mandatory flag in PublishWithContext ——————————————————————————————————————

func TestPublisher_Publish_passesMandatoryFlag(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.mandatory = true
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}}))

	mandatory, ok := fake.lastMandatory()
	require.True(t, ok)
	assert.True(t, mandatory, "PublishWithContext must be called with mandatory=true when Publisher.mandatory is set")
}

// — ConfirmTimeout builder wires to Publisher ————————————————————————————————

func TestPublisher_Publish_confirmTimeout_zero_disables(t *testing.T) {
	// confirmTimeout=0 means no deadline; Wait blocks indefinitely until ctx is cancelled.
	fake := newFakePubCh(false /* no auto-ack */)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.confirmTimeout = 0 // disable

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	// With no confirm arriving and no timeout, ctx must cancel first.
	err := pub.Publish(ctx, Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	// Should be context cancellation, NOT ErrConfirmTimeout.
	assert.False(t, errors.Is(err, ErrConfirmTimeout), "confirmTimeout=0 must not return ErrConfirmTimeout, got %v", err)
}

// — PublishTimeout ————————————————————————————————————————————————————————————

func TestPublisher_Publish_publishTimeout_capsConfirm(t *testing.T) {
	fake := newFakePubCh(false /* withholds confirm */)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.publishTimeout = 30 * time.Millisecond
	pub.confirmTimeout = 5 * time.Second // longer than publishTimeout

	start := time.Now()
	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, elapsed < 200*time.Millisecond, "publishTimeout must cap publish within deadline, elapsed=%v", elapsed)
	// Error chain must include context.DeadlineExceeded.
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "error chain must contain context.DeadlineExceeded, got %v", err)
}

func TestPublisher_Publish_publishTimeout_zero_noDeadline(t *testing.T) {
	// publishTimeout=0 means no extra deadline; caller ctx is authoritative.
	fake := newFakePubCh(true /* auto-ack */)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.publishTimeout = 0
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}}))
}

// — PublishRetry ——————————————————————————————————————————————————————————————

func TestPublisher_Publish_publishRetry_retriesTransient(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := newFakePubChNackOnce()
	pub, stopPool := newTestPubReturn[testPayload](fake, metrics.NoOpPublisherMetrics{}, nil)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.retryPolicy = &RetryPolicy{Min: time.Millisecond, Max: 5 * time.Millisecond, WithoutJitter: true}

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.NoError(t, err, "PublishRetry must succeed after transient error is resolved")
}

func TestPublisher_Publish_publishRetry_doesNotRetryPermanent(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := newFakePubChUnroutable()
	pub, stopPool := newTestPubReturn[testPayload](fake, metrics.NoOpPublisherMetrics{}, nil)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// mandatory=true is required: basic.return only arrives for mandatory publishes.
	pub.mandatory = true
	pub.retryPolicy = &RetryPolicy{Min: time.Millisecond, WithoutJitter: true}

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnroutable), "expected ErrUnroutable without retry, got %v", err)
	assert.Equal(t, int32(1), fake.publishCount.Load(), "permanent error must not be retried")
}

func TestPublisher_Publish_publishRetry_limitsAttempts(t *testing.T) {
	// Retries=1 means 1 extra attempt after the first failure — 2 total calls.
	defer goleak.VerifyNone(t)

	fake := newFakePubChAlwaysNack()
	pub, stopPool := newTestPubReturn[testPayload](fake, metrics.NoOpPublisherMetrics{}, nil)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.retryPolicy = &RetryPolicy{Min: time.Millisecond, Max: 5 * time.Millisecond, WithoutJitter: true, Retries: 1}

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.Equal(t, int32(2), fake.publishCount.Load(), "Retries=1 must produce exactly 2 PublishWithContext calls")
}

func TestPublisher_Publish_publishRetry_ctxCancelledDuringBackoff(t *testing.T) {
	// ctx cancelled while waiting for the backoff timer must return promptly.
	defer goleak.VerifyNone(t)

	fake := newFakePubChAlwaysNack()
	pub, stopPool := newTestPubReturn[testPayload](fake, metrics.NoOpPublisherMetrics{}, nil)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.retryPolicy = &RetryPolicy{Min: 500 * time.Millisecond, Max: time.Second, WithoutJitter: true}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- pub.Publish(ctx, Message[testPayload]{Body: &testPayload{}})
	}()

	// Let the first attempt fail, then cancel before the backoff expires.
	time.Sleep(20 * time.Millisecond)
	cancel()

	start := time.Now()
	err := <-done
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, elapsed < 200*time.Millisecond, "ctx cancel must abort backoff promptly, elapsed=%v", elapsed)
}

func TestPublisher_Publish_publishRetry_incrementsMetric(t *testing.T) {
	defer goleak.VerifyNone(t)

	pm := &capturePublisherMetrics{}
	fake := newFakePubChNackOnce()
	pub, stopPool := newTestPubReturn[testPayload](fake, pm, nil)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.exchange = "ex"
	pub.retryPolicy = &RetryPolicy{Min: time.Millisecond, Max: 5 * time.Millisecond, WithoutJitter: true}

	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}}))

	pm.mu.Lock()
	defer pm.mu.Unlock()
	require.Len(t, pm.retries, 1, "expected exactly one retry")
	assert.Equal(t, "ex", pm.retries[0].exchange)
	assert.Equal(t, "nacked", pm.retries[0].reason)
}

// — StampUserID + UserID validation ————————————————————————————————————————

func TestPublisher_Publish_stampUserID_setsUserID(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.stampUserID = true
	pub.authUser = "alice"

	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}}))

	p, ok := fake.lastPublish()
	require.True(t, ok)
	assert.Equal(t, "alice", p.UserId, "StampUserID must auto-set UserID to conn.AuthenticatedUser()")
}

func TestPublisher_Publish_userIDValidation_rejectsWrongUser(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.authUser = "alice"

	err := pub.Publish(context.Background(), Message[testPayload]{
		Body:   &testPayload{},
		UserID: "bob", // wrong user
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidMessage), "expected ErrInvalidMessage for mismatched UserID, got %v", err)

	// Verify no publish frame was written.
	_, ok := fake.lastPublish()
	assert.False(t, ok, "no publish frame must be written when UserID validation fails")
}

func TestPublisher_Publish_userIDValidation_allowsMatchingUser(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.authUser = "alice"

	err := pub.Publish(context.Background(), Message[testPayload]{
		Body:   &testPayload{},
		UserID: "alice", // correct user
	})
	require.NoError(t, err)
}

func TestPublisher_Publish_userIDValidation_allowsEmptyUserID(t *testing.T) {
	// Empty UserID means "no stamp requested" — validation is skipped.
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.authUser = "alice"

	err := pub.Publish(context.Background(), Message[testPayload]{
		Body:   &testPayload{},
		UserID: "", // no stamp requested — OK
	})
	require.NoError(t, err)
}

// — AMQPCode 312/313 on ErrUnroutable ——————————————————————————————————————

func TestPublisher_Publish_amqpCode312_onErrUnroutable(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := newFakePubChReturnsWithCode(312)
	pub, stopPool := newTestPubReturn[testPayload](fake, metrics.NoOpPublisherMetrics{}, nil)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// mandatory=true is required: basic.return only arrives for mandatory publishes.
	pub.mandatory = true

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnroutable), "expected ErrUnroutable, got %v", err)
	code, ok := AMQPCode(err)
	assert.True(t, ok, "AMQPCode must be set for ErrUnroutable")
	assert.Equal(t, uint16(312), code)
}

func TestPublisher_Publish_amqpCode313_onErrUnroutable(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := newFakePubChReturnsWithCode(313)
	pub, stopPool := newTestPubReturn[testPayload](fake, metrics.NoOpPublisherMetrics{}, nil)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// mandatory=true is required: basic.return only arrives for mandatory publishes.
	pub.mandatory = true

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnroutable), "expected ErrUnroutable, got %v", err)
	code, ok := AMQPCode(err)
	assert.True(t, ok, "AMQPCode must be set for ErrUnroutable")
	assert.Equal(t, uint16(313), code)
}

// — OnReturn callback ——————————————————————————————————————————————————————

func TestPublisher_Publish_onReturn_firesBeforePublishUnblocks(t *testing.T) {
	defer goleak.VerifyNone(t)

	var returnMu sync.Mutex
	var capturedReturn Return
	var returnFired bool

	cb := func(r Return) {
		returnMu.Lock()
		capturedReturn = r
		returnFired = true
		returnMu.Unlock()
	}

	fake := newFakePubChReturnsWithCode(312)
	pub, stopPool := newTestPubReturn[testPayload](fake, metrics.NoOpPublisherMetrics{}, cb)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	// mandatory=true is required: basic.return only arrives for mandatory publishes.
	pub.mandatory = true

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{Value: "return-test"}})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnroutable), "expected ErrUnroutable, got %v", err)

	returnMu.Lock()
	defer returnMu.Unlock()
	assert.True(t, returnFired, "OnReturn callback must have been called")
	assert.Equal(t, uint16(312), capturedReturn.ReplyCode)
}

// — StampUserID edge cases ——————————————————————————————————————————————————

func TestPublisher_Publish_stampUserID_doesNotOverwriteExistingUserID(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.stampUserID = true
	pub.authUser = "alice"

	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{
		Body:   &testPayload{},
		UserID: "alice", // already set — must not be overwritten or rejected
	}))

	p, ok := fake.lastPublish()
	require.True(t, ok)
	assert.Equal(t, "alice", p.UserId)
}

func TestPublisher_Publish_userIDValidation_skippedWhenAuthUserEmpty(t *testing.T) {
	// When authUser is empty (e.g. anonymous connection), validation is skipped.
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.authUser = "" // connection cannot determine identity

	err := pub.Publish(context.Background(), Message[testPayload]{
		Body:   &testPayload{},
		UserID: "bob",
	})
	require.NoError(t, err, "UserID validation must be skipped when authUser is empty")
}

// — callOnReturn Expiration parsing ————————————————————————————————————————

func TestPublisher_callOnReturn_parsesExpiration(t *testing.T) {
	var captured Return
	pub := &Publisher[testPayload]{
		onReturn: func(r Return) { captured = r },
	}

	pub.callOnReturn(amqp091.Return{
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE",
		Expiration: "5000",
	})

	assert.Equal(t, 5*time.Second, captured.Properties.Expiration)
}

func TestPublisher_callOnReturn_ignoresInvalidExpiration(t *testing.T) {
	var captured Return
	pub := &Publisher[testPayload]{
		onReturn: func(r Return) { captured = r },
	}

	pub.callOnReturn(amqp091.Return{
		ReplyCode:  312,
		Expiration: "not-a-number",
	})

	assert.Equal(t, time.Duration(0), captured.Properties.Expiration)
}

// — buildPublishing Expiration field ———————————————————————————————————————

func TestBuildPublishing_setsExpirationField(t *testing.T) {
	msg := Message[testPayload]{Expiration: 2500 * time.Millisecond}
	pub := buildPublishing(msg, []byte("body"))
	assert.Equal(t, "2500", pub.Expiration)
}

func TestBuildPublishing_zeroExpirationOmitted(t *testing.T) {
	msg := Message[testPayload]{Expiration: 0}
	pub := buildPublishing(msg, []byte("body"))
	assert.Equal(t, "", pub.Expiration)
}

// — retryReason label coverage —————————————————————————————————————————————

func TestRetryReason_allLabels(t *testing.T) {
	tests := []struct {
		err    error
		reason string
	}{
		{ErrPublishNacked, "nacked"},
		{ErrConfirmTimeout, "confirm_timeout"},
		{ErrChannelClosed, "channel_closed"},
		{ErrChannelPoolExhausted, "pool_exhausted"},
		{ErrConnectionBlocked, "blocked"},
		{ErrReconnecting, "reconnecting"},
		{errors.New("some network error"), "network"},
	}
	for _, tc := range tests {
		t.Run(tc.reason, func(t *testing.T) {
			assert.Equal(t, tc.reason, retryReason(tc.err))
		})
	}
}

// — helper fake channels ————————————————————————————————————————————————————

// fakePubChAlwaysNack nacks every publish unconditionally.
type fakePubChAlwaysNack struct {
	mu           sync.Mutex
	seq          uint64
	confirmCh    chan amqp091.Confirmation
	closedCh     chan *amqp091.Error
	publishCount atomic.Int32
}

func newFakePubChAlwaysNack() *fakePubChAlwaysNack {
	return &fakePubChAlwaysNack{seq: 1, closedCh: make(chan *amqp091.Error, 1)}
}

func (f *fakePubChAlwaysNack) GetNextPublishSeqNo() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.seq
	f.seq++
	return s
}
func (f *fakePubChAlwaysNack) Confirm(bool) error { return nil }
func (f *fakePubChAlwaysNack) NotifyPublish(ch chan amqp091.Confirmation) chan amqp091.Confirmation {
	f.mu.Lock()
	f.confirmCh = ch
	f.mu.Unlock()
	return ch
}
func (f *fakePubChAlwaysNack) NotifyReturn(ch chan amqp091.Return) chan amqp091.Return { return ch }
func (f *fakePubChAlwaysNack) NotifyClose(ch chan *amqp091.Error) chan *amqp091.Error {
	return f.closedCh
}
func (f *fakePubChAlwaysNack) Close() error {
	f.mu.Lock()
	ch := f.confirmCh
	f.confirmCh = nil
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}
func (f *fakePubChAlwaysNack) PublishWithContext(_ context.Context, _, _ string, _, _ bool, _ amqp091.Publishing) error {
	f.publishCount.Add(1)
	f.mu.Lock()
	tag := f.seq - 1
	ch := f.confirmCh
	f.mu.Unlock()
	go func() {
		if ch != nil {
			ch <- amqp091.Confirmation{DeliveryTag: tag, Ack: false}
		}
	}()
	return nil
}

// fakePubChNackOnce nacks the first publish and auto-acks subsequent ones.
type fakePubChNackOnce struct {
	mu        sync.Mutex
	seq       uint64
	confirmCh chan amqp091.Confirmation
	closedCh  chan *amqp091.Error
	publishes []amqp091.Publishing
	callCount int
}

func newFakePubChNackOnce() *fakePubChNackOnce {
	return &fakePubChNackOnce{
		seq:      1,
		closedCh: make(chan *amqp091.Error, 1),
	}
}

func (f *fakePubChNackOnce) GetNextPublishSeqNo() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.seq
	f.seq++
	return s
}
func (f *fakePubChNackOnce) Confirm(bool) error { return nil }
func (f *fakePubChNackOnce) NotifyPublish(ch chan amqp091.Confirmation) chan amqp091.Confirmation {
	f.mu.Lock()
	f.confirmCh = ch
	f.mu.Unlock()
	return ch
}
func (f *fakePubChNackOnce) NotifyReturn(ch chan amqp091.Return) chan amqp091.Return { return ch }
func (f *fakePubChNackOnce) NotifyClose(ch chan *amqp091.Error) chan *amqp091.Error {
	return f.closedCh
}
func (f *fakePubChNackOnce) Close() error {
	f.mu.Lock()
	ch := f.confirmCh
	f.confirmCh = nil
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}
func (f *fakePubChNackOnce) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg amqp091.Publishing) error {
	f.mu.Lock()
	f.callCount++
	callN := f.callCount
	tag := f.seq - 1
	ch := f.confirmCh
	f.publishes = append(f.publishes, msg)
	f.mu.Unlock()

	go func() {
		if callN == 1 {
			ch <- amqp091.Confirmation{DeliveryTag: tag, Ack: false} // nack
		} else {
			ch <- amqp091.Confirmation{DeliveryTag: tag, Ack: true} // ack
		}
	}()
	return nil
}

// fakePubChUnroutable nacks via tracker.MarkReturned + ack (simulates basic.return).
// Returns with replyCode 312 for all publishes and never auto-acks.
type fakePubChUnroutable struct {
	mu           sync.Mutex
	seq          uint64
	confirmCh    chan amqp091.Confirmation
	returnCh     chan amqp091.Return
	closedCh     chan *amqp091.Error
	publishCount atomic.Int32
}

func newFakePubChUnroutable() *fakePubChUnroutable {
	return &fakePubChUnroutable{
		seq:      1,
		closedCh: make(chan *amqp091.Error, 1),
	}
}

func (f *fakePubChUnroutable) GetNextPublishSeqNo() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.seq
	f.seq++
	return s
}
func (f *fakePubChUnroutable) Confirm(bool) error { return nil }
func (f *fakePubChUnroutable) NotifyPublish(ch chan amqp091.Confirmation) chan amqp091.Confirmation {
	f.mu.Lock()
	f.confirmCh = ch
	f.mu.Unlock()
	return ch
}
func (f *fakePubChUnroutable) NotifyReturn(ch chan amqp091.Return) chan amqp091.Return {
	f.mu.Lock()
	f.returnCh = ch
	f.mu.Unlock()
	return ch
}
func (f *fakePubChUnroutable) NotifyClose(ch chan *amqp091.Error) chan *amqp091.Error {
	return f.closedCh
}
func (f *fakePubChUnroutable) Close() error {
	f.mu.Lock()
	ch := f.confirmCh
	f.confirmCh = nil
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}
func (f *fakePubChUnroutable) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg amqp091.Publishing) error {
	f.publishCount.Add(1)
	f.mu.Lock()
	tag := f.seq - 1
	returnCh := f.returnCh
	confirmCh := f.confirmCh
	f.mu.Unlock()

	// Send basic.return synchronously on the unbuffered returnCh: blocks until
	// the pool goroutine reads it, guaranteeing MarkReturned precedes the Ack.
	if returnCh != nil {
		returnCh <- amqp091.Return{
			ReplyCode:  312,
			ReplyText:  "NO_ROUTE",
			Exchange:   "x",
			RoutingKey: "rk",
			MessageId:  msg.MessageId,
		}
	}
	// Send the ack in a goroutine (non-blocking) after the return is processed.
	go func() {
		if confirmCh != nil {
			confirmCh <- amqp091.Confirmation{DeliveryTag: tag, Ack: true}
		}
	}()
	return nil
}

// fakePubChReturnsWithCode is like fakePubChUnroutable but with a configurable reply code.
type fakePubChReturnsWithCode struct {
	mu        sync.Mutex
	seq       uint64
	confirmCh chan amqp091.Confirmation
	returnCh  chan amqp091.Return
	closedCh  chan *amqp091.Error
	replyCode uint16
}

func newFakePubChReturnsWithCode(code uint16) *fakePubChReturnsWithCode {
	return &fakePubChReturnsWithCode{
		seq:       1,
		closedCh:  make(chan *amqp091.Error, 1),
		replyCode: code,
	}
}

func (f *fakePubChReturnsWithCode) GetNextPublishSeqNo() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.seq
	f.seq++
	return s
}
func (f *fakePubChReturnsWithCode) Confirm(bool) error { return nil }
func (f *fakePubChReturnsWithCode) NotifyPublish(ch chan amqp091.Confirmation) chan amqp091.Confirmation {
	f.mu.Lock()
	f.confirmCh = ch
	f.mu.Unlock()
	return ch
}
func (f *fakePubChReturnsWithCode) NotifyReturn(ch chan amqp091.Return) chan amqp091.Return {
	f.mu.Lock()
	f.returnCh = ch
	f.mu.Unlock()
	return ch
}
func (f *fakePubChReturnsWithCode) NotifyClose(ch chan *amqp091.Error) chan *amqp091.Error {
	return f.closedCh
}
func (f *fakePubChReturnsWithCode) Close() error {
	f.mu.Lock()
	ch := f.confirmCh
	f.confirmCh = nil
	f.mu.Unlock()
	if ch != nil {
		close(ch)
	}
	return nil
}
func (f *fakePubChReturnsWithCode) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg amqp091.Publishing) error {
	f.mu.Lock()
	tag := f.seq - 1
	returnCh := f.returnCh
	confirmCh := f.confirmCh
	code := f.replyCode
	f.mu.Unlock()

	// Send basic.return synchronously on the unbuffered returnCh: blocks until
	// the pool goroutine reads it, guaranteeing MarkReturned precedes the Ack.
	if returnCh != nil {
		returnCh <- amqp091.Return{
			ReplyCode:  code,
			ReplyText:  "NO_ROUTE",
			Exchange:   "x",
			RoutingKey: "rk",
			MessageId:  msg.MessageId,
		}
	}
	// Send the ack in a goroutine (non-blocking) after the return is processed.
	go func() {
		if confirmCh != nil {
			confirmCh <- amqp091.Confirmation{DeliveryTag: tag, Ack: true}
		}
	}()
	return nil
}
