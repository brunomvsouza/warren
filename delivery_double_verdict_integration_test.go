//go:build integration

package warren_test

// T60 (R10-5 / DS-04 / CR-04) real-broker assertion for the resolved-once
// guard on Delivery[M]: a HandlerTimeout verdict followed by a LATE handler
// Ack (via ConsumeRaw) must NOT emit a second frame and must NOT channel-close
// with PRECONDITION_FAILED (406) — which pre-fix would take out every in-flight
// handler on that channel. We prove the channel survived by consuming a second
// message after the late ack.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

func TestDeliveryDoubleVerdict_lateAckAfterTimeout_channelSurvives_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx := context.Background()

	const srcQ = "test.t60.double-verdict.src"
	purgeQueues(t, url, srcQ)
	t.Cleanup(func() { deleteQueues(url, srcQ) })

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	topo := &warren.Topology{
		Queues: []warren.Queue{{Name: srcQ, Durable: false}},
	}
	require.NoError(t, topo.Declare(ctx, conn))

	pub, err := warren.PublisherFor[string](conn).Exchange("").RoutingKey(srcQ).Build()
	require.NoError(t, err)

	first := "first"
	second := "second"
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &first}))

	var (
		lateAckErr   atomic.Value // error from the late d.Ack
		secondSeen   = make(chan struct{})
		firstHandled = make(chan struct{})
	)

	// ConsumeRaw handler: on the first (slow) message, block past the 50ms
	// HandlerTimeout, then call d.Ack() LATE — after the timeout already nacked.
	// On the second message, ack normally and signal.
	consumer, err := warren.ConsumerFor[string](conn).
		Queue(srcQ).
		Prefetch(1).
		HandlerTimeout(50 * time.Millisecond).
		Build()
	require.NoError(t, err)

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	defer cancelConsume()

	var firstDone atomic.Bool
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = consumer.ConsumeRaw(consumeCtx, func(hCtx context.Context, d *warren.Delivery[string]) error {
			if !firstDone.Load() && *d.Body() == "first" {
				firstDone.Store(true)
				<-hCtx.Done()  // wait for HandlerTimeout to fire (and nack)
				err := d.Ack() // LATE verdict — must be a no-op, no second frame
				lateAckErr.Store(boxErr(err))
				close(firstHandled)
				return nil
			}
			if *d.Body() == "second" {
				_ = d.Ack()
				close(secondSeen)
			}
			return nil
		})
	}()

	// Wait for the first message's late ack to have happened.
	select {
	case <-firstHandled:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the late ack on the first message")
	}

	// The resolved-once CAS guarantees exactly one wire frame, but the WINNER is
	// an acknowledged race (SPEC §6.3 "Double-verdict guard"): the handler observes
	// only that hCtx fired (the deadline), not that nackOnTimeout has emitted yet, so
	// the late d.Ack() and the timeout verdict race on the same CAS. Either outcome
	// is correct and produces a single frame:
	//   - timeout won the CAS  → late Ack loses → ErrAlreadyResolved
	//   - late Ack won the CAS → it emitted the one frame → nil
	// What must NEVER happen is a second frame (which channel-closes with 406); that
	// invariant is proven below by consuming the second message. Asserting a specific
	// winner here over-specifies beyond the SPEC and flakes under load (the handler
	// won the CAS ~1/3 of CI runs).
	le := unboxErr(lateAckErr.Load())
	if le != nil {
		assert.True(t, errors.Is(le, warren.ErrAlreadyResolved),
			"late Ack after HandlerTimeout must be nil (won the CAS) or ErrAlreadyResolved (lost it), got %v", le)
	}

	// Publish a second message: if the channel had closed with PRECONDITION_FAILED,
	// the consumer would not receive it (until a resubscribe). Receiving it proves
	// the channel survived the double verdict.
	require.NoError(t, pub.Publish(ctx, warren.Message[string]{Body: &second}))

	select {
	case <-secondSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not process the second message — channel likely closed by a double-verdict PRECONDITION_FAILED")
	}

	cancelConsume()
	select {
	case <-consumerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumer goroutine to exit")
	}
}

// boxErr/unboxErr wrap a possibly-nil error for storage in atomic.Value, which
// rejects nil and requires a consistent concrete type.
type errBox struct{ err error }

func boxErr(err error) errBox { return errBox{err} }
func unboxErr(v any) error {
	if v == nil {
		return nil
	}
	return v.(errBox).err
}
