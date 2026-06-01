package warren

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// panicHeaderCodec panics during EncodeWithHeaders, exercising the HeaderCodec
// branch of the publisher's safeEncodeBody recover (T73).
type panicHeaderCodec struct{}

func (panicHeaderCodec) Encode(any) ([]byte, error) { return []byte("{}"), nil }
func (panicHeaderCodec) Decode([]byte, any) error   { return nil }
func (panicHeaderCodec) ContentType() string        { return "application/x-panic" }
func (panicHeaderCodec) EncodeWithHeaders(any) ([]byte, map[string]any, string, error) {
	panic("header codec exploded")
}
func (panicHeaderCodec) DecodeWithHeaders([]byte, map[string]any, string, any) error { return nil }

// TestPublisher_CodecPanic_Encode_surfacesErrInvalidMessage proves the T73
// contract: a panicking user codec's Encode is recovered, Publish returns
// ErrInvalidMessage, and NO publish frame is written (the panic happens in the
// client-side encode choke-point, before any channel is acquired). goleak clean.
func TestPublisher_CodecPanic_Encode_surfacesErrInvalidMessage(t *testing.T) {
	fake := newFakePubCh(true)
	pool, stopPool := wireFakePool(fake)
	pub := &Publisher[testPayload]{
		pools:    []*publisherConnPool{pool},
		mcs:      []*managedConn{{}},
		codec:    panicEncodeCodec{},
		pm:       metrics.NoOpPublisherMetrics{},
		exchange: "x",
		tracer:   otel.NoOpTracer{},
	}
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{Value: "x"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMessage, "a panicking codec Encode must surface ErrInvalidMessage, not crash")

	_, wrote := fake.lastPublish()
	assert.False(t, wrote, "no publish frame must be written when Encode panics")
}

// TestPublisher_CodecPanic_EncodeWithHeaders_surfacesErrInvalidMessage covers the
// HeaderCodec branch (EncodeWithHeaders) of the same recover.
func TestPublisher_CodecPanic_EncodeWithHeaders_surfacesErrInvalidMessage(t *testing.T) {
	fake := newFakePubCh(true)
	pool, stopPool := wireFakePool(fake)
	pub := &Publisher[testPayload]{
		pools:    []*publisherConnPool{pool},
		mcs:      []*managedConn{{}},
		codec:    panicHeaderCodec{},
		pm:       metrics.NoOpPublisherMetrics{},
		exchange: "x",
		tracer:   otel.NoOpTracer{},
	}
	defer goleak.VerifyNone(t)
	defer stopPool()
	defer func() { _ = pub.Close(context.Background()) }()

	err := pub.Publish(context.Background(), Message[testPayload]{Body: &testPayload{Value: "x"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMessage, "a panicking HeaderCodec EncodeWithHeaders must surface ErrInvalidMessage")

	_, wrote := fake.lastPublish()
	assert.False(t, wrote, "no publish frame must be written when EncodeWithHeaders panics")
}
