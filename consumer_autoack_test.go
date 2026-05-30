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
)

// — T35: broker AutoAck (no-ack) dispatch semantics —————————————————————
//
// Under AutoAck the broker considers every delivery acknowledged before the
// client sees it (SPEC §6.3). The consumer must therefore NEVER call Ack/Nack:
// a handler error becomes a no-op (the message is already gone) and is surfaced
// only as a sampled warning log.

func TestConsumer_AutoAck_HandlerError_DoesNotAckOrNack(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("testq").AutoAck().Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ackOrNack := make(chan string, 2)
	handled := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			close(handled)
			cancel()
			return errors.New("boom") // would Nack(false) under manual ack
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(_ uint64, _ bool) error { ackOrNack <- "ack"; return nil },
			nackFn: func(_ uint64, _, _ bool) error { ackOrNack <- "nack"; return nil },
		},
	}

	<-handled
	<-done

	select {
	case op := <-ackOrNack:
		t.Fatalf("AutoAck consumer must NOT ack/nack on handler error; got %q", op)
	default:
		// expected: the broker already acked on dispatch
	}
}

func TestConsumer_AutoAck_HandlerError_LogsSampledWarning(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	var mu sync.Mutex
	var warnings []string
	conn.opts.logger = &captureLogger{onWarning: func(msg string) {
		mu.Lock()
		warnings = append(warnings, msg)
		mu.Unlock()
	}}

	c, err := ConsumerFor[string](conn).Queue("testq").AutoAck().Build()
	require.NoError(t, err)
	// Emit on occurrences 1 and 4 over six errors, proving suppression.
	c.autoAckDropLog.every = 3

	const total = 6
	deliveryCh := make(chan amqp091.Delivery, total)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var processed atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Consume(ctx, func(_ context.Context, _ string) error {
			if processed.Add(1) == total {
				cancel()
			}
			return errors.New("boom")
		})
	}()

	for range total {
		deliveryCh <- amqp091.Delivery{Body: []byte(`"x"`)}
	}
	<-done

	mu.Lock()
	got := len(warnings)
	first := ""
	if got > 0 {
		first = warnings[0]
	}
	mu.Unlock()

	assert.Equal(t, 2, got, "every=3 over 6 handler errors must log occurrences 1 and 4 only")
	assert.Contains(t, first, "AutoAck", "warning must name the AutoAck trade-off")
	assert.Contains(t, first, "testq", "warning must name the queue")
}

func TestConsumer_AutoAck_DecodeError_DoesNotNack_LogsWarning(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	var mu sync.Mutex
	var warnings []string
	conn.opts.logger = &captureLogger{onWarning: func(msg string) {
		mu.Lock()
		warnings = append(warnings, msg)
		mu.Unlock()
	}}

	c, err := ConsumerFor[string](conn).Queue("testq").AutoAck().Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 2)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nacked := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// ConsumeRaw so the second (valid) delivery's handler can cancel without
		// any auto-verdict interfering; the first delivery exercises the decode path.
		_ = c.ConsumeRaw(ctx, func(_ context.Context, _ *Delivery[string]) error {
			cancel()
			return nil
		})
	}()

	// First: invalid JSON → decode-error path (handler is never called).
	deliveryCh <- amqp091.Delivery{
		Body: []byte(`{not json`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, _ bool) error { nacked <- struct{}{}; return nil },
		},
	}
	// Second: valid payload; processed after the first (concurrency=1) and cancels.
	deliveryCh <- amqp091.Delivery{Body: []byte(`"ok"`)}

	<-done

	select {
	case <-nacked:
		t.Fatal("AutoAck consumer must NOT nack on decode error")
	default:
	}

	mu.Lock()
	got := len(warnings)
	mu.Unlock()
	assert.GreaterOrEqual(t, got, 1, "a silently dropped poison message under AutoAck must be logged")
}

// — dropSampler ————————————————————————————————————————————————————————

func TestDropSampler_EmitsFirstThenEveryNth(t *testing.T) {
	s := &dropSampler{every: 3}
	var emits []uint64
	for range 7 {
		if emit, total := s.sample(); emit {
			emits = append(emits, total)
		}
	}
	assert.Equal(t, []uint64{1, 4, 7}, emits)
}

func TestDropSampler_EveryOne_AlwaysEmits(t *testing.T) {
	s := &dropSampler{every: 1}
	for range 5 {
		emit, _ := s.sample()
		assert.True(t, emit, "every<=1 must always emit")
	}
}

func TestDropSampler_ReportsRunningTotal(t *testing.T) {
	s := &dropSampler{every: 100}
	var lastTotal uint64
	for range 5 {
		_, lastTotal = s.sample()
	}
	assert.Equal(t, uint64(5), lastTotal, "total must count every occurrence, logged or suppressed")
}

// TestDropSampler_ConcurrentSample_TotalIsExact exercises the atomic counter under
// real contention — the AutoAck Concurrency(N)>1 case, where multiple dispatch
// goroutines hit logAutoAckDrop → sample() at once. The running total must equal the
// exact number of calls (no increment lost or duplicated), and the emit count must
// follow the deterministic first-then-every-Nth formula regardless of interleaving.
func TestDropSampler_ConcurrentSample_TotalIsExact(t *testing.T) {
	defer goleak.VerifyNone(t)

	const (
		goroutines = 50
		perG       = 200
		every      = 7
		n          = goroutines * perG
	)
	s := &dropSampler{every: every}

	var emits atomic.Int64
	var maxTotal atomic.Uint64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range goroutines {
		wg.Go(func() {
			<-start // line every goroutine up so the calls genuinely overlap
			for range perG {
				emit, total := s.sample()
				if emit {
					emits.Add(1)
				}
				for { // track the high-water total returned across all goroutines
					cur := maxTotal.Load()
					if total <= cur || maxTotal.CompareAndSwap(cur, total) {
						break
					}
				}
			}
		})
	}
	close(start)
	wg.Wait()

	// atomic.Add hands out unique sequential values 1..n, so both the final counter
	// and the maximum value ever returned must be exactly n — anything less means a
	// lost increment, anything more a duplicate (the race detector guards reads too).
	assert.Equal(t, uint64(n), s.n.Load(), "counter must equal the call count (no lost increment)")
	assert.Equal(t, uint64(n), maxTotal.Load(), "max total returned must equal the call count (no duplicate)")

	// emit is true on total 1 and then every `every`-th value: 1 + floor((n-1)/every).
	wantEmits := int64(1 + (n-1)/every)
	assert.Equal(t, wantEmits, emits.Load(), "emit count must follow first-then-every-Nth under any interleaving")
}

// TestConsumer_AutoAck_HandlerTimeout_RecordsLabel_NoNack pins the dedicated
// brokerAutoAck branch of the HandlerTimeout switch (consumer.go): the broker already
// acked on dispatch, so a timeout cannot nack — it records the would-be verdict label
// (timeout_nack_no_requeue) for observability parity and sends NO frame. This is the
// one verdict-vs-frame divergence in AutoAck dispatch.
func TestConsumer_AutoAck_HandlerTimeout_RecordsLabel_NoNack(t *testing.T) {
	defer goleak.VerifyNone(t)

	conn := newFakeConsumerConn(t)
	capCM := &captureConsumerMetrics{}
	c, err := ConsumerFor[string](conn).
		Queue("testq").
		AutoAck().
		HandlerTimeout(20 * time.Millisecond).
		Metrics(capCM).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	c.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel the outer ctx the instant the timeout branch starts draining, so Consume
	// returns after this single delivery without racing a second select iteration.
	c.testHookBeforeTimeoutDrain = func() { cancel() }

	ackOrNack := make(chan string, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Consume(ctx, func(hctx context.Context, _ string) error {
			<-hctx.Done() // stall past the 20ms deadline; returns when the timeout fires
			return hctx.Err()
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn:  func(_ uint64, _ bool) error { ackOrNack <- "ack"; return nil },
			nackFn: func(_ uint64, _, _ bool) error { ackOrNack <- "nack"; return nil },
		},
	}

	<-done

	select {
	case op := <-ackOrNack:
		t.Fatalf("AutoAck consumer must NOT ack/nack on handler timeout; got %q", op)
	default:
		// expected: the broker already acked on dispatch
	}

	capCM.mu.Lock()
	defer capCM.mu.Unlock()
	require.Len(t, capCM.records, 1)
	assert.Equal(t, "timeout_nack_no_requeue", capCM.records[0].outcome,
		"AutoAck timeout must record the would-be verdict label for parity with manual-ack observability")
	assert.Equal(t, 1, capCM.timeouts, "RecordHandlerTimeout must fire on the deadline")
}
