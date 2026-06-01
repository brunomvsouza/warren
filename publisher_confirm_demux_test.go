package warren

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/internal/confirms"
	"github.com/brunomvsouza/warren/metrics"
)

// TestConfirmDemux_returnChannelUnbuffered locks the load-bearing invariant
// (T59 / R10-3 / RMQ-16) that startConfirmDemux registers an UNBUFFERED
// basic.return notify channel. If a refactor makes it buffered, the
// amqp091-go reader can dispatch the basic.ack before the demux processes the
// basic.return, silently losing ErrUnroutable ~50% of the time under load.
// This assertion goes red the instant the return channel is given any capacity.
func TestConfirmDemux_returnChannelUnbuffered(t *testing.T) {
	defer goleak.VerifyNone(t)

	fake := newFakePubCh(false)
	tracker := confirms.New()
	rtm := new(sync.Map)

	done := startConfirmDemux(fake, tracker, rtm, nil, 8)
	defer func() {
		_ = fake.Close()
		<-done
	}()

	fake.mu.Lock()
	returnCh := fake.returnCh
	confirmCh := fake.confirmCh
	fake.mu.Unlock()

	require.NotNil(t, returnCh, "demux must register a basic.return channel")
	assert.Equal(t, 0, cap(returnCh),
		"basic.return notify channel MUST be unbuffered (T59): a buffered channel "+
			"lets the reader dispatch the ack before the return is processed, losing ErrUnroutable")
	assert.Equal(t, 8, cap(confirmCh), "confirm channel is buffered to the pool size")
}

// TestConfirmDemux_everyUnroutablePublishResolvesErrUnroutable drives many
// sequential mandatory-unroutable publishes through the PRODUCTION demux
// (wireFakePool now calls startConfirmDemux) and asserts every single one
// resolves ErrUnroutable — zero false successes. A split demux or a buffered
// return channel would let some publishes resolve as a (false) success.
func TestConfirmDemux_everyUnroutablePublishResolvesErrUnroutable(t *testing.T) {
	fake := newFakePubCh(true /* autoAck — sends ack after the return */)
	fake.returnAll = true

	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.mandatory = true

	const iterations = 200
	for i := 0; i < iterations; i++ {
		err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{Value: "x"}})
		require.Error(t, err, "publish %d must not be a false success", i)
		require.Truef(t, errors.Is(err, ErrUnroutable), "publish %d: expected ErrUnroutable, got %v", i, err)
	}
}
