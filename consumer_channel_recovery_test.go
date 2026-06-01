package warren

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/metrics"
)

// resubSpyMetrics counts consumer_resubscribed_total increments (T61 SRE-01).
type resubSpyMetrics struct {
	metrics.NoOpConsumerMetrics
	resubscribed atomic.Int64
}

func (m *resubSpyMetrics) RecordResubscribed(_ string) { m.resubscribed.Add(1) }

// TestConsumer_channelOnlyDeath_selfHeals_andIncrementsMetric proves that when
// the delivery channel closes while the TCP socket stays up (no reconnect hook
// fires), the consumer reopens its channel directly, keeps consuming, and
// increments consumer_resubscribed_total — instead of parking silently (T61).
func TestConsumer_channelOnlyDeath_selfHeals_andIncrementsMetric(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	cm := &resubSpyMetrics{}
	consumer, err := ConsumerFor[string](conn).Queue("testq").Metrics(cm).Build()
	require.NoError(t, err)

	// Each factory call returns a fresh live delivery sub (a new channel + done).
	// This stands in for openDeliveryCh reopening a channel on a healthy socket.
	type liveCh struct {
		ch   chan amqp091.Delivery
		done chan struct{}
	}
	var openCount atomic.Int64
	current := make(chan *liveCh, 8)
	consumer.deliverySubFactory = func(_ context.Context) (deliverySub, error) {
		openCount.Add(1)
		lc := &liveCh{ch: make(chan amqp091.Delivery, 1), done: make(chan struct{})}
		current <- lc
		return deliverySub{ch: lc.ch, done: lc.done}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received atomic.Int64
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = consumer.ConsumeRaw(ctx, func(_ context.Context, d *Delivery[string]) error {
			received.Add(1)
			return d.Ack()
		})
	}()

	// First subscription: deliver one message, then simulate a channel-only death
	// by closing its delivery channel (TCP still up — no hook fires).
	first := <-current
	first.ch <- amqp091.Delivery{Body: []byte(`"one"`), ContentType: "application/json", Acknowledger: &fakeAcker{}}
	require.Eventually(t, func() bool { return received.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
	close(first.ch) // channel-only death

	// The consumer must self-heal: reopen via the factory and keep consuming.
	var second *liveCh
	require.Eventually(t, func() bool {
		select {
		case second = <-current:
			return true
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "consumer did not reopen its channel after a channel-only death")

	second.ch <- amqp091.Delivery{Body: []byte(`"two"`), ContentType: "application/json", Acknowledger: &fakeAcker{}}
	require.Eventually(t, func() bool { return received.Load() == 2 }, 2*time.Second, 10*time.Millisecond)

	assert.GreaterOrEqual(t, cm.resubscribed.Load(), int64(1),
		"consumer_resubscribed_total must increment on a channel-level self-heal (SRE-01)")
	assert.GreaterOrEqual(t, openCount.Load(), int64(2), "factory must be called for the initial open and the self-heal")

	cancel()
	close(second.ch)
	<-consumerDone
}

// TestRecoverDeliveryChannel_churnGuard_enforcesFloor proves the T61 churn guard
// actually sleeps the floor when the channel is flapping. recoverDeliveryChannel
// only imposes channelRecoverInitialBackoff before reopening if a previous reopen
// was within channelRecoverMaxBackoff (the "recent" horizon) — without this floor a
// broker that resubscribes then drops without basic.cancel spins the recovery loop
// at broker-RTT cadence (<1ms). The only T61 test before this one never set
// lastChannelReopen, so the floor branch (consumer.go ~1198) was dead in tests: a
// regression deleting the guard (re-introducing the spin storm) would stay green.
func TestRecoverDeliveryChannel_churnGuard_enforcesFloor(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	// The factory always succeeds immediately, so the ONLY possible source of delay
	// is the churn-guard floor sleep at the top of recoverDeliveryChannel.
	consumer.deliverySubFactory = func(_ context.Context) (deliverySub, error) {
		return deliverySub{ch: make(chan amqp091.Delivery), done: make(chan struct{})}, nil
	}

	resubCh := make(chan deliverySub) // empty: no hook replacement competes
	var wg sync.WaitGroup

	// A recent reopen marks the channel as flapping → the guard must impose the floor.
	consumer.lastChannelReopen = time.Now()
	start := time.Now()
	_, _, action := consumer.recoverDeliveryChannel(context.Background(), &wg, resubCh)
	withGuard := time.Since(start)
	require.Equal(t, recoverResubscribed, action)
	assert.GreaterOrEqual(t, withGuard, channelRecoverInitialBackoff-5*time.Millisecond,
		"a flapping channel (recent reopen) must wait at least the churn-guard floor before reopening")

	// No recent reopen → the guard is skipped and recovery is immediate (the common
	// single-reconnect case is never penalised by the floor).
	consumer.lastChannelReopen = time.Time{}
	start = time.Now()
	_, _, action = consumer.recoverDeliveryChannel(context.Background(), &wg, resubCh)
	noGuard := time.Since(start)
	require.Equal(t, recoverResubscribed, action)
	assert.Less(t, noGuard, channelRecoverInitialBackoff,
		"with no recent reopen the floor must not be imposed")
}

// TestRecoverDeliveryChannel_socketDown_yieldsToHook proves the channel-vs-TCP
// arbitration at the heart of T61: when the direct reopen fails with
// ErrNotConnected (the raw socket is nil because a TCP reconnect is in flight),
// recoverDeliveryChannel must NOT stop the consumer — it backs off and lets the
// reconnect supervisor hook (resubCh) own recovery. Before this test only the
// socket-up self-heal path was exercised; a regression turning the ErrNotConnected
// branch into a recoverStop (killing the consumer on a transient socket loss) would
// have shipped green.
func TestRecoverDeliveryChannel_socketDown_yieldsToHook(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	// The direct reopen always fails with ErrNotConnected — i.e. a TCP reconnect is
	// in flight and the raw socket is nil.
	var openCount atomic.Int64
	factoryCalled := make(chan struct{})
	var once sync.Once
	consumer.deliverySubFactory = func(_ context.Context) (deliverySub, error) {
		openCount.Add(1)
		once.Do(func() { close(factoryCalled) })
		return deliverySub{}, ErrNotConnected
	}

	hookCh := make(chan amqp091.Delivery)
	resubCh := make(chan deliverySub)
	go func() {
		<-factoryCalled // ensure the direct reopen was attempted and failed first
		resubCh <- deliverySub{ch: hookCh, done: make(chan struct{})}
	}()

	var wg sync.WaitGroup
	sub, reason, action := consumer.recoverDeliveryChannel(context.Background(), &wg, resubCh)

	require.Equal(t, recoverResubscribed, action,
		"a socket-down direct reopen must yield to the reconnect hook, not stop the consumer")
	assert.Empty(t, reason)
	assert.GreaterOrEqual(t, openCount.Load(), int64(1),
		"the direct reopen must be attempted before deferring to the hook")
	// The factory never returns a usable sub, so a recoverResubscribed result can
	// only have come from the hook channel — prove it is the hook's sub, not a
	// self-healed one.
	assert.Equal(t, hookCh, sub.ch, "the returned subscription must be the hook-delivered one")
}

// TestRecoverDeliveryChannel_paused_parksWithoutSelfHeal proves a deliberate Pause
// suppresses channel-level self-heal. Pause issues a local basic.cancel that closes
// only the delivery stream; runConsume's !ok branch then enters recoverDeliveryChannel
// with c.paused==true. If it self-healed (reopened the channel) it would re-issue
// basic.consume and clearPause — silently defeating Pause and letting messages flow
// again. The recover MUST instead park on resubCh until Resume / a reconnect hook
// delivers a replacement, never calling the reopen factory. Regression guard for the
// integration PauseResume contract at unit speed.
func TestRecoverDeliveryChannel_paused_parksWithoutSelfHeal(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	// Any self-heal would call the factory (reopen + clearPause). It records calls
	// so the test can prove it is never invoked while paused.
	var openCount atomic.Int64
	consumer.deliverySubFactory = func(_ context.Context) (deliverySub, error) {
		openCount.Add(1)
		return deliverySub{ch: make(chan amqp091.Delivery), done: make(chan struct{})}, nil
	}
	consumer.paused.Store(true)

	hookCh := make(chan amqp091.Delivery)
	resubCh := make(chan deliverySub)
	// Resume / the reconnect hook delivers a replacement after a beat; until then a
	// paused recover must park (never reopen).
	go func() {
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, int64(0), openCount.Load(), "a paused consumer must not self-heal while parked")
		resubCh <- deliverySub{ch: hookCh, done: make(chan struct{})}
	}()

	var wg sync.WaitGroup
	sub, _, action := consumer.recoverDeliveryChannel(context.Background(), &wg, resubCh)

	require.Equal(t, recoverResubscribed, action)
	assert.Equal(t, hookCh, sub.ch, "the resubscription must be the Resume/hook-delivered one, not a self-heal")
	assert.Equal(t, int64(0), openCount.Load(), "the reopen factory must never be called while paused")
}

// TestRecoverDeliveryChannel_reconnecting_yieldsToHookNoSelfHeal proves the
// channel-level self-heal stands down while the socket is mid-TCP-reconnect: the
// reconnect supervisor's hook owns recovery and re-subscribes on resubCh. openChannel
// only nil-checks raw, so once the socket re-dials mid-barrier a direct reopen would
// SUCCEED and issue a SECOND basic.consume on the same consumer tag — the broker
// rejects the duplicate and kills the channel, stalling delivery. recoverDeliveryChannel
// must therefore NOT call the reopen factory while isReconnecting() is true.
func TestRecoverDeliveryChannel_reconnecting_yieldsToHookNoSelfHeal(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	var openCount atomic.Int64
	consumer.deliverySubFactory = func(_ context.Context) (deliverySub, error) {
		openCount.Add(1)
		return deliverySub{ch: make(chan amqp091.Delivery), done: make(chan struct{})}, nil
	}

	// Socket mid-reconnect: the hook owns recovery, not a direct reopen.
	consumer.mc.barrierMu.Lock()
	consumer.mc.reconnecting = true
	consumer.mc.barrierMu.Unlock()

	hookCh := make(chan amqp091.Delivery)
	resubCh := make(chan deliverySub)
	go func() {
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, int64(0), openCount.Load(), "a reconnecting socket must not self-heal; the hook owns recovery")
		resubCh <- deliverySub{ch: hookCh, done: make(chan struct{})}
	}()

	var wg sync.WaitGroup
	sub, _, action := consumer.recoverDeliveryChannel(context.Background(), &wg, resubCh)

	require.Equal(t, recoverResubscribed, action)
	assert.Equal(t, hookCh, sub.ch, "the resubscription must be the hook-delivered one")
	assert.Equal(t, int64(0), openCount.Load(), "no duplicate basic.consume self-heal while reconnecting")
}
