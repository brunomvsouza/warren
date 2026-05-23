package warren

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/codec"
	"github.com/brunomvsouza/warren/log"
	"github.com/brunomvsouza/warren/metrics"
)

// — builder unit tests ———————————————————————————————————————————————————

func TestConsumerBuilder_DefaultTag_IsCtagUUIDv7(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(c.tag, "ctag-"), "tag must start with ctag-, got %q", c.tag)
	assert.Len(t, c.tag, len("ctag-")+36, "tag must be ctag-<uuidv7>")
}

func TestConsumerBuilder_UserSuppliedTag_PassedThrough(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Tag("my-tag").Build()
	require.NoError(t, err)
	assert.Equal(t, "my-tag", c.tag)
}

func TestConsumerBuilder_LastWins_Concurrency(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Concurrency(2).Concurrency(8).Build()
	require.NoError(t, err)
	assert.Equal(t, uint(8), c.concurrency)
}

func TestConsumerBuilder_LastWins_HandlerTimeout(t *testing.T) {
	conn := newFakeConsumerConn(t)
	// second call with 0 disables the timeout
	c, err := ConsumerFor[string](conn).Queue("q").
		HandlerTimeout(50 * time.Millisecond).
		HandlerTimeout(0).
		Build()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), c.handlerTimeout)
}

func TestConsumerBuilder_LastWins_HandlerTimeoutVerdict(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").
		HandlerTimeoutVerdict(TimeoutNackRequeue).
		HandlerTimeoutVerdict(TimeoutNackNoRequeue).
		Build()
	require.NoError(t, err)
	assert.Equal(t, TimeoutNackNoRequeue, c.timeoutVerdict)
}

func TestConsumerBuilder_LastWins_Codec(t *testing.T) {
	conn := newFakeConsumerConn(t)
	lax := codec.NewJSONLax()
	strict := codec.NewJSON()
	c, err := ConsumerFor[string](conn).Queue("q").
		Codec(strict).Codec(lax).
		Build()
	require.NoError(t, err)
	// lax codec: decoding a payload with unknown fields must succeed
	var out string
	err = c.codec.Decode([]byte(`"hello"`), &out)
	require.NoError(t, err)
	assert.Equal(t, "hello", out)
}

func TestConsumerBuilder_WarnWhenPrefetchLtConcurrency(t *testing.T) {
	conn := newFakeConsumerConn(t)
	warned := false
	conn.opts.logger = &captureLogger{onWarning: func(msg string) {
		if strings.Contains(msg, "prefetch") {
			warned = true
		}
	}}
	_, err := ConsumerFor[string](conn).Queue("q").Prefetch(1).Concurrency(4).Build()
	require.NoError(t, err)
	assert.True(t, warned, "expected a prefetch < concurrency warning")
}

func TestConsumerBuilder_DefaultPrefetchAndConcurrency(t *testing.T) {
	conn := newFakeConsumerConn(t)
	c, err := ConsumerFor[string](conn).Queue("q").Build()
	require.NoError(t, err)
	assert.Equal(t, uint16(64), c.prefetch)
	assert.Equal(t, uint(1), c.concurrency)
}

func TestConsumerBuilder_NilConn_Error(t *testing.T) {
	_, err := ConsumerFor[string](nil).Queue("q").Build()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestConsumerBuilder_EmptyQueue_Error(t *testing.T) {
	conn := newFakeConsumerConn(t)
	_, err := ConsumerFor[string](conn).Build()
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

// — Consume unit tests (fake channel injection) ———————————————————————

func TestConsumer_ConsumeRaw_AlreadyStarted_ReturnsError(t *testing.T) {
	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	// Simulate "already started" by setting the guard directly.
	consumer.started.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = consumer.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidOptions)
}

func TestConsumer_Consume_HandlerCalledWithDecodedPayload(t *testing.T) {
	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())

	var received string
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, msg string) error {
			received = msg
			cancel()
			return nil
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`"hello"`),
		ContentType: "application/json",
	}

	<-done
	assert.Equal(t, "hello", received)
}

func TestConsumer_Consume_HandlerNilReturn_Acks(t *testing.T) {
	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	acked := make(chan struct{}, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error { return nil })
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			ackFn: func(_ uint64, _ bool) error {
				close(acked)
				cancel()
				return nil
			},
		},
	}

	select {
	case <-acked:
	case <-time.After(time.Second):
		t.Fatal("expected Ack to be called")
	}
	<-done
}

func TestConsumer_Consume_HandlerErrorReturn_NackNoRequeue(t *testing.T) {
	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	nackedRequeue := make(chan bool, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			return errors.New("handler failed")
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.False(t, requeue, "generic error must nack without requeue")
	case <-time.After(time.Second):
		t.Fatal("expected Nack to be called")
	}
	<-done
}

func TestConsumer_Consume_HandlerErrRequeue_NackRequeue(t *testing.T) {
	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	nackedRequeue := make(chan bool, 1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			return ErrRequeue
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body: []byte(`"hello"`),
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.True(t, requeue, "ErrRequeue must nack with requeue=true")
	case <-time.After(time.Second):
		t.Fatal("expected Nack to be called")
	}
	<-done
}

func TestConsumer_Consume_DecodeFailure_NackNoRequeue(t *testing.T) {
	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).Queue("testq").Metrics(cm).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			cancel()
			return nil
		})
	}()

	ackErr := make(chan error, 1)
	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`not valid json`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, _ bool) error {
				ackErr <- nil
				cancel()
				return nil
			},
		},
	}

	select {
	case <-ackErr:
	case <-time.After(time.Second):
		t.Fatal("expected nack for decode failure")
	}
	<-done
	assert.Equal(t, 1, cm.decodeErrors)
}

func TestConsumer_Consume_HandlerTimeout_DefaultVerdict_NackNoRequeue(t *testing.T) {
	conn := newFakeConsumerConn(t)
	cm := &countingConsumerMetrics{}
	consumer, err := ConsumerFor[string](conn).
		Queue("testq").
		HandlerTimeout(50 * time.Millisecond).
		Metrics(cm).
		Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 1)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nackedRequeue := make(chan bool, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(hCtx context.Context, _ string) error {
			select {
			case <-hCtx.Done():
				return hCtx.Err()
			case <-time.After(500 * time.Millisecond):
				return nil
			}
		})
	}()

	deliveryCh <- amqp091.Delivery{
		Body:        []byte(`"hello"`),
		ContentType: "application/json",
		Acknowledger: &fakeAcknowledger{
			nackFn: func(_ uint64, _, requeue bool) error {
				nackedRequeue <- requeue
				cancel()
				return nil
			},
		},
	}

	select {
	case requeue := <-nackedRequeue:
		assert.False(t, requeue, "default verdict must be nack-no-requeue")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: expected nack for handler timeout")
	}
	<-done
	assert.Equal(t, 1, cm.handlerTimeouts)
}

func TestConsumer_Consume_Concurrency(t *testing.T) {
	conn := newFakeConsumerConn(t)
	consumer, err := ConsumerFor[string](conn).Queue("testq").Concurrency(3).Prefetch(8).Build()
	require.NoError(t, err)

	deliveryCh := make(chan amqp091.Delivery, 10)
	consumer.deliveryCh = deliveryCh

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	unblock := make(chan struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.Consume(ctx, func(_ context.Context, _ string) error {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)
			for {
				cur := maxInFlight.Load()
				if n <= cur || maxInFlight.CompareAndSwap(cur, n) {
					break
				}
			}
			<-unblock
			return nil
		})
	}()

	// send 3 messages; with concurrency=3 all 3 should run simultaneously
	for range 3 {
		deliveryCh <- amqp091.Delivery{
			Body:         []byte(`"x"`),
			ContentType:  "application/json",
			Acknowledger: &fakeAcknowledger{},
		}
	}

	// wait until all 3 are in flight, then release
	require.Eventually(t, func() bool {
		return inFlight.Load() == 3
	}, time.Second, 5*time.Millisecond)

	close(unblock)
	cancel()
	<-done

	assert.Equal(t, int32(3), maxInFlight.Load())
}

// — helpers ——————————————————————————————————————————————————————————————

func newFakeConsumerConn(t *testing.T) *Connection {
	t.Helper()
	conn := &Connection{}
	conn.opts.logger = log.NewNoOp()
	mc := &managedConn{opts: &conn.opts}
	conn.conConns = []*managedConn{mc}
	conn.pubConns = []*managedConn{mc}
	return conn
}

// fakeAcknowledger implements amqp091.Acknowledger.
type fakeAcknowledger struct {
	ackFn  func(tag uint64, multiple bool) error
	nackFn func(tag uint64, multiple, requeue bool) error
}

func (f *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	if f.ackFn != nil {
		return f.ackFn(tag, multiple)
	}
	return nil
}

func (f *fakeAcknowledger) Nack(tag uint64, multiple, requeue bool) error {
	if f.nackFn != nil {
		return f.nackFn(tag, multiple, requeue)
	}
	return nil
}

func (f *fakeAcknowledger) Reject(tag uint64, requeue bool) error { return nil }

// countingConsumerMetrics counts specific metric increments.
type countingConsumerMetrics struct {
	metrics.NoOpConsumerMetrics
	decodeErrors    int
	handlerTimeouts int
}

func (c *countingConsumerMetrics) RecordHandler(_, outcome string, _ time.Duration) {
	if outcome == "decode_error" {
		c.decodeErrors++
	}
}

func (c *countingConsumerMetrics) RecordHandlerTimeout(_ string) {
	c.handlerTimeouts++
}

// captureLogger captures warning log lines.
type captureLogger struct {
	onWarning func(msg string)
}

func (l *captureLogger) Debug(_ string)            {}
func (l *captureLogger) Info(_ string)             {}
func (l *captureLogger) Warning(msg string)        { l.onWarning(msg) }
func (l *captureLogger) Error(_ string)            {}
func (l *captureLogger) Debugf(_ string, _ ...any) {}
func (l *captureLogger) Infof(_ string, _ ...any)  {}
func (l *captureLogger) Warningf(format string, args ...any) {
	l.onWarning(fmt.Sprintf(format, args...))
}
func (l *captureLogger) Errorf(_ string, _ ...any) {}
