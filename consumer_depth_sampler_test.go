package warren

import (
	"context"
	"maps"
	"sync"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/metrics"
)

// fakeDepthChannel is a topologyChannel that answers QueueDeclarePassive with a
// per-name message count (or a per-name error, e.g. a 404 for an absent DLQ),
// counting how many passive declares ran so a test can assert the probe cadence.
type fakeDepthChannel struct {
	mu           sync.Mutex
	counts       map[string]int   // queue name -> Messages reported by the broker
	errs         map[string]error // queue name -> passive-declare error (overrides counts)
	passiveCalls map[string]int   // queue name -> number of passive declares observed
	closed       int
}

func newFakeDepthChannel() *fakeDepthChannel {
	return &fakeDepthChannel{
		counts:       map[string]int{},
		errs:         map[string]error{},
		passiveCalls: map[string]int{},
	}
}

func (f *fakeDepthChannel) ExchangeDeclare(_, _ string, _, _, _, _ bool, _ amqp091.Table) error {
	return nil
}

func (f *fakeDepthChannel) QueueDeclare(_ string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	return amqp091.Queue{}, nil
}

func (f *fakeDepthChannel) QueueBind(_, _, _ string, _ bool, _ amqp091.Table) error { return nil }

func (f *fakeDepthChannel) QueueDeclarePassive(name string, _, _, _, _ bool, _ amqp091.Table) (amqp091.Queue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.passiveCalls[name]++
	if err := f.errs[name]; err != nil {
		return amqp091.Queue{}, err
	}
	return amqp091.Queue{Name: name, Messages: f.counts[name]}, nil
}

func (f *fakeDepthChannel) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed++
	return nil
}

func (f *fakeDepthChannel) passiveCallCount(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.passiveCalls[name]
}

// captureDepthMetrics records SetQueueDepth/SetDLQDepth calls and, when notify is
// non-nil, closes it once on the first SetQueueDepth so a lifecycle test can await
// the first sample without a sleep.
type captureDepthMetrics struct {
	metrics.NoOpConsumerMetrics
	mu          sync.Mutex
	queueDepths map[string]int64
	dlqDepths   map[string]int64
	notify      chan struct{}
	notifyOnce  sync.Once
}

func newCaptureDepthMetrics() *captureDepthMetrics {
	return &captureDepthMetrics{
		queueDepths: map[string]int64{},
		dlqDepths:   map[string]int64{},
	}
}

func (m *captureDepthMetrics) SetQueueDepth(queue string, depth int64) {
	m.mu.Lock()
	m.queueDepths[queue] = depth
	m.mu.Unlock()
	if m.notify != nil {
		m.notifyOnce.Do(func() { close(m.notify) })
	}
}

func (m *captureDepthMetrics) SetDLQDepth(dlq string, depth int64) {
	m.mu.Lock()
	m.dlqDepths[dlq] = depth
	m.mu.Unlock()
}

// snapshot returns copies of the captured gauge maps for race-free assertion.
func (m *captureDepthMetrics) snapshot() (queue, dlq map[string]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return maps.Clone(m.queueDepths), maps.Clone(m.dlqDepths)
}

// newDepthSamplerConsumer builds a string consumer wired to a fake depth channel so
// sampleDepths/sampleQueueDepth can run without a live broker. The interval is long
// (the sampler is never started); these tests drive sampleDepths directly.
func newDepthSamplerConsumer(t *testing.T, ch *fakeDepthChannel, cm metrics.ConsumerMetrics) *Consumer[string] {
	t.Helper()
	conn := newFakeConsumerConn(t)
	conn.conConns[0].chanFactory = func() (topologyChannel, error) { return ch, nil }
	c, err := ConsumerFor[string](conn).
		Queue("orders").
		Metrics(cm).
		WithQueueDepthSampler(time.Hour).
		Build()
	require.NoError(t, err)
	return c
}

func TestConsumerBuilder_WithQueueDepthSampler_LastWins(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).
		Queue("orders").
		WithQueueDepthSampler(5 * time.Second).
		WithQueueDepthSampler(250 * time.Millisecond). // last-wins
		Build()
	require.NoError(t, err)
	assert.Equal(t, 250*time.Millisecond, c.depthSampleInterval)
}

func TestConsumerBuilder_WithQueueDepthSampler_DisabledByDefault(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("orders").Build()
	require.NoError(t, err)
	assert.Zero(t, c.depthSampleInterval, "no WithQueueDepthSampler leaves the sampler disabled")
}

func TestConsumer_SampleDepths_EmitsQueueAndDLQGauges(t *testing.T) {
	ch := newFakeDepthChannel()
	ch.counts["orders"] = 42
	ch.counts["orders.dlq"] = 7
	cm := newCaptureDepthMetrics()
	c := newDepthSamplerConsumer(t, ch, cm)

	c.sampleDepths()

	q, d := cm.snapshot()
	qd, ok := q["orders"]
	require.True(t, ok, "queue_depth must be emitted for the source queue")
	assert.Equal(t, int64(42), qd)

	dd, ok := d["orders.dlq"]
	require.True(t, ok, "dlq_depth must be emitted for the conventional <queue>.dlq")
	assert.Equal(t, int64(7), dd)
}

func TestConsumer_SampleDepths_SkipsDLQWhenAbsent(t *testing.T) {
	// The source queue exists; its <queue>.dlq does not (broker answers 404). The
	// sampler must emit queue_depth but NOT a phantom dlq_depth{...}=0 series.
	ch := newFakeDepthChannel()
	ch.counts["orders"] = 3
	ch.errs["orders.dlq"] = &amqp091.Error{Code: 404, Reason: "NOT_FOUND"}
	cm := newCaptureDepthMetrics()
	c := newDepthSamplerConsumer(t, ch, cm)

	c.sampleDepths()

	q, d := cm.snapshot()
	qd, ok := q["orders"]
	require.True(t, ok)
	assert.Equal(t, int64(3), qd)

	_, ok = d["orders.dlq"]
	assert.False(t, ok, "no dlq_depth series when the DLQ does not exist")
}

func TestConsumer_SampleDepths_SkipsSourceWhenAbsent(t *testing.T) {
	// The source queue itself is gone (404): emit neither gauge rather than a 0.
	ch := newFakeDepthChannel()
	ch.errs["orders"] = &amqp091.Error{Code: 404, Reason: "NOT_FOUND"}
	ch.errs["orders.dlq"] = &amqp091.Error{Code: 404, Reason: "NOT_FOUND"}
	cm := newCaptureDepthMetrics()
	c := newDepthSamplerConsumer(t, ch, cm)

	c.sampleDepths()

	q, d := cm.snapshot()
	_, ok := q["orders"]
	assert.False(t, ok, "no queue_depth when the source queue is gone")
	_, ok = d["orders.dlq"]
	assert.False(t, ok)
}

func TestConsumer_SampleQueueDepth_OpenChannelFails(t *testing.T) {
	// No chanFactory and no live socket → openChannel errors → (0,false), no panic.
	conn := newFakeConsumerConn(t)
	cm := newCaptureDepthMetrics()
	c, err := ConsumerFor[string](conn).Queue("orders").Metrics(cm).WithQueueDepthSampler(time.Hour).Build()
	require.NoError(t, err)

	depth, ok := c.sampleQueueDepth("orders")
	assert.False(t, ok)
	assert.Zero(t, depth)
}

func TestConsumer_SampleDepths_ClosesEveryProbeChannel(t *testing.T) {
	// Each passive declare runs on its own short-lived channel that is closed
	// afterwards, so a 404 (which the broker answers by closing the channel) can
	// never leak onto a shared channel. Two declares per sample → two closes.
	ch := newFakeDepthChannel()
	ch.counts["orders"] = 1
	ch.counts["orders.dlq"] = 0
	cm := newCaptureDepthMetrics()
	c := newDepthSamplerConsumer(t, ch, cm)

	c.sampleDepths()

	assert.Equal(t, 1, ch.passiveCallCount("orders"))
	assert.Equal(t, 1, ch.passiveCallCount("orders.dlq"))
	ch.mu.Lock()
	closed := ch.closed
	ch.mu.Unlock()
	assert.Equal(t, 2, closed, "every probe channel must be closed")
}

func TestConsumer_DepthSampler_Lifecycle_StopsOnCtxCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	ch := newFakeDepthChannel()
	ch.counts["orders"] = 11
	ch.counts["orders.dlq"] = 2
	cm := newCaptureDepthMetrics()
	cm.notify = make(chan struct{})

	conn := newFakeConsumerConn(t)
	conn.conConns[0].chanFactory = func() (topologyChannel, error) { return ch, nil }
	c, err := ConsumerFor[string](conn).
		Queue("orders").
		Metrics(cm).
		WithQueueDepthSampler(5 * time.Millisecond).
		Build()
	require.NoError(t, err)

	// Inject a delivery channel so Consume runs without a live broker (openDeliveryCh
	// returns it directly and never opens the sampler's probe channel itself).
	c.deliveryCh = make(chan amqp091.Delivery)

	ctx, cancel := context.WithCancel(context.Background())
	consumeDone := make(chan struct{})
	go func() {
		defer close(consumeDone)
		_ = c.Consume(ctx, func(context.Context, string) error { return nil })
	}()

	// The sampler primes the gauges immediately; await the first SetQueueDepth.
	select {
	case <-cm.notify:
	case <-time.After(2 * time.Second):
		t.Fatal("depth sampler never emitted queue_depth")
	}
	q, _ := cm.snapshot()
	qd, ok := q["orders"]
	require.True(t, ok)
	assert.Equal(t, int64(11), qd)

	// Cancelling the consume context must stop and join the sampler goroutine.
	cancel()
	select {
	case <-consumeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after ctx cancel")
	}
}
