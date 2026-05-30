package warren

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDelayedTopic_constructsValidExchangeLiteral asserts the helper builds the
// Exchange{} a delayed-topic exchange requires: Kind=ExchangeDelayed plus the
// x-delayed-type=topic arg, and that the result passes Topology.validate().
func TestDelayedTopic_constructsValidExchangeLiteral(t *testing.T) {
	e := DelayedTopic("events")

	assert.Equal(t, "events", e.Name)
	assert.Equal(t, ExchangeDelayed, e.Kind)
	assert.True(t, e.Durable, "a delayed exchange is declared durable by default")
	assert.Equal(t, "topic", e.Args["x-delayed-type"],
		"the underlying routing type must be topic")

	// The literal must be a valid topology member on its own.
	topo := &Topology{Exchanges: []Exchange{e}}
	require.NoError(t, topo.validate())
}

// TestBuildPublishing_Delay_setsXDelayHeaderMilliseconds asserts a non-zero
// Message.Delay surfaces as the x-delay header the rabbitmq_delayed_message_exchange
// plugin reads, as a signed 32-bit millisecond count.
func TestBuildPublishing_Delay_setsXDelayHeaderMilliseconds(t *testing.T) {
	pub := buildPublishing(Message[int]{Delay: 2 * time.Second}, []byte("payload"))

	require.NotNil(t, pub.Headers)
	assert.Equal(t, int32(2000), pub.Headers["x-delay"],
		"x-delay must be the delay in milliseconds as int32 (the plugin's signedint type)")
}

// TestBuildPublishing_NoDelay_omitsXDelayHeader asserts a zero Delay never injects
// an x-delay header (which would route a plain message through a delayed exchange
// the user did not ask for).
func TestBuildPublishing_NoDelay_omitsXDelayHeader(t *testing.T) {
	pub := buildPublishing(Message[int]{}, []byte("payload"))

	_, ok := pub.Headers["x-delay"]
	assert.False(t, ok, "a zero Delay must not emit an x-delay header")
}

// TestBuildPublishing_Delay_doesNotMutateCallerHeaders asserts the x-delay header
// is written to a clone — a caller reusing the same Message.Headers map across
// publishes must never observe a smuggled-in x-delay key.
func TestBuildPublishing_Delay_doesNotMutateCallerHeaders(t *testing.T) {
	original := Headers{"trace": "abc"}
	msg := Message[int]{Headers: original, Delay: 500 * time.Millisecond}

	pub := buildPublishing(msg, []byte("payload"))

	// The publishing carries both the caller's header and the injected delay.
	assert.Equal(t, "abc", pub.Headers["trace"])
	assert.Equal(t, int32(500), pub.Headers["x-delay"])

	// The caller's own map is untouched.
	_, leaked := original["x-delay"]
	assert.False(t, leaked, "x-delay must not leak back into the caller's Headers map")
	assert.Len(t, original, 1, "the caller's Headers map must be unchanged")
}
