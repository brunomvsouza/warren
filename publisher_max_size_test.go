package warren

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/metrics"
)

// — builder option ———————————————————————————————————————————————————————————

func TestPublisherBuilder_MaxMessageSizeBytes_lastWins(t *testing.T) {
	b := PublisherFor[testPayload](nil).
		MaxMessageSizeBytes(1024).
		MaxMessageSizeBytes(4096)
	assert.Equal(t, 4096, b.maxMessageSizeBytes)
	assert.True(t, b.maxMessageSizeBytesSet)
}

func TestPublisherBuilder_MaxMessageSizeBytes_defaultIs16MiB(t *testing.T) {
	b := &PublisherBuilder[testPayload]{}
	b.applyBuilderDefaults()
	assert.Equal(t, defaultMaxMessageSizeBytes, b.maxMessageSizeBytes)
	assert.Equal(t, 16*1024*1024, b.maxMessageSizeBytes,
		"default payload guardrail must be 16 MiB to protect against accidental OOM")
}

func TestPublisherBuilder_MaxMessageSizeBytes_explicitZeroDisablesGuard(t *testing.T) {
	b := PublisherFor[testPayload](nil).MaxMessageSizeBytes(0)
	b.applyBuilderDefaults()
	assert.Equal(t, 0, b.maxMessageSizeBytes, "explicit zero must disable the guard, not revert to default")
	assert.True(t, b.maxMessageSizeBytesSet)
}

func TestPublisherBuilder_Build_rejectsNegativeMaxMessageSizeBytes(t *testing.T) {
	conn := &Connection{} // any non-nil; Build validates options before touching it
	_, err := PublisherFor[testPayload](conn).MaxMessageSizeBytes(-1).Build()
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidOptions),
		"negative MaxMessageSizeBytes must be ErrInvalidOptions, got %v", err)
}

// — Publish-time enforcement —————————————————————————————————————————————————

func TestPublisher_Publish_rejectsBodyOverMax(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.maxMessageSizeBytes = 64 // tight cap; the JSON encoding of payload below exceeds it

	huge := testPayload{Value: strings.Repeat("x", 256)}
	err := pub.Publish(context.Background(), Message[testPayload]{Body: &huge})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrMessageTooLarge),
		"payload over MaxMessageSizeBytes must return ErrMessageTooLarge, got %v", err)
	assert.True(t, IsPermanent(err), "ErrMessageTooLarge must classify as permanent (payload never shrinks on retry)")

	// Channel must NOT have seen any frame — local rejection precedes broker contact.
	_, ok := fake.lastPublish()
	assert.False(t, ok, "Publish must reject before opening a channel")
}

func TestPublisher_Publish_acceptsBodyAtOrUnderMax(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.maxMessageSizeBytes = 1024
	small := testPayload{Value: "ok"}
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &small}))
}

func TestPublisher_Publish_zeroMaxDisablesGuard(t *testing.T) {
	fake := newFakePubCh(true)
	pub, _, stopPool := newTestPub[testPayload](fake, metrics.NoOpPublisherMetrics{})
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.maxMessageSizeBytes = 0 // disabled
	huge := testPayload{Value: strings.Repeat("x", 4096)}
	// With the guard disabled the publish must proceed to the broker.
	require.NoError(t, pub.Publish(context.Background(), Message[testPayload]{Body: &huge}))
}

func TestPublisher_Publish_recordsTooLargeOutcome(t *testing.T) {
	fake := newFakePubCh(true)
	pm := &capturePublisherMetrics{}
	pub, _, stopPool := newTestPub[testPayload](fake, pm)
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	pub.maxMessageSizeBytes = 32
	huge := testPayload{Value: strings.Repeat("x", 256)}
	_ = pub.Publish(context.Background(), Message[testPayload]{Body: &huge})

	pm.mu.Lock()
	defer pm.mu.Unlock()
	var sawTooLarge bool
	for _, r := range pm.records {
		if r.outcome == "too_large" {
			sawTooLarge = true
			break
		}
	}
	assert.True(t, sawTooLarge,
		"Publisher must record outcome=too_large on local size-cap rejection, got: %+v", pm.records)
}

// — Sentinel classification —————————————————————————————————————————————————

func TestErrMessageTooLarge_isPermanent(t *testing.T) {
	assert.True(t, IsPermanent(ErrMessageTooLarge),
		"ErrMessageTooLarge must classify as permanent — retrying the same body cannot succeed")
	assert.False(t, IsTransient(ErrMessageTooLarge),
		"ErrMessageTooLarge must NOT classify as transient")
}
