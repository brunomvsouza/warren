//go:build integration

package warren_test

// T59 (R10-3 / RMQ-16 / TV-02) real-broker assertion for the return/ack
// ordering invariant. A mock tracker cannot reproduce amqp091-go's two-channel
// notify-dispatch timing, so a green mock proves nothing about the race this
// contract exists to lock. Only a real broker, hammered with concurrent
// unroutable-mandatory publishes during confirm load, trips the ~50%-under-load
// race against amqp091-go's basic.return vs basic.ack dispatch.
//
// FLAKY-PRONE BY DESIGN: this exercises a timing race. A single PR run can pass
// even with a broken (buffered/split) demux; it belongs on the NIGHTLY trigger
// (T151) at high iteration/concurrency, not as a per-PR gate. The unit
// regression TestConfirmDemux_returnChannelUnbuffered is the deterministic lock
// for the unbuffered-channel half of the invariant.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
)

type unroutablePayload struct {
	Seq int `json:"seq"`
}

// TestReturnAckOrdering_concurrentUnroutable_integration publishes many
// mandatory messages to an exchange with NO matching binding, concurrently,
// while confirm traffic is in flight. Every publish MUST resolve ErrUnroutable
// — a false success (nil error) means the basic.ack was processed before the
// basic.return, i.e. the ordering invariant broke.
func TestReturnAckOrdering_concurrentUnroutable_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()

	const exchangeName = "warren-t59-unroutable-test"
	t.Cleanup(func() { deleteExchanges(url, exchangeName) })

	// A topic exchange with NO bound queue: every mandatory publish is unroutable
	// and the broker returns it with reply code 312 (NO_ROUTE).
	top := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: exchangeName, Kind: warren.ExchangeTopic, Durable: false, AutoDelete: true}},
	}
	require.NoError(t, top.Declare(ctx, conn))

	pub, err := warren.PublisherFor[unroutablePayload](conn).
		Exchange(exchangeName).
		RoutingKey("no.such.binding").
		Mandatory().
		Build()
	require.NoError(t, err)
	defer func() { _ = pub.Close(context.Background()) }()

	const (
		workers   = 8
		perWorker = 250
		total     = workers * perWorker
	)

	var unroutable, falseSuccess, otherErr atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				err := pub.Publish(ctx, warren.Message[unroutablePayload]{Body: &unroutablePayload{Seq: base + i}})
				switch {
				case err == nil:
					falseSuccess.Add(1)
				case errors.Is(err, warren.ErrUnroutable):
					unroutable.Add(1)
				default:
					otherErr.Add(1)
				}
			}
		}(w * perWorker)
	}
	wg.Wait()

	assert.Equal(t, int64(0), falseSuccess.Load(),
		"every unroutable mandatory publish must resolve ErrUnroutable; a false success means "+
			"basic.ack was processed before basic.return (ordering invariant broke)")
	assert.Equal(t, int64(0), otherErr.Load(), "no unexpected errors")
	assert.Equal(t, int64(total), unroutable.Load(), "all %d publishes must be ErrUnroutable", total)
}
