package codec_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/brunomvsouza/warren/codec"
)

func TestNewProtobuf_ContentType(t *testing.T) {
	c := codec.NewProtobuf()
	assert.Equal(t, "application/x-protobuf", c.ContentType())
}

func TestNewProtobuf_RoundTrip_Timestamp(t *testing.T) {
	c := codec.NewProtobuf()
	original := timestamppb.New(time.Unix(1716800000, 123456789).UTC())

	b, err := c.Encode(original)
	require.NoError(t, err)

	var decoded timestamppb.Timestamp
	require.NoError(t, c.Decode(b, &decoded))
	assert.True(t, proto.Equal(original, &decoded))
}

func TestNewProtobuf_RoundTrip_Struct(t *testing.T) {
	c := codec.NewProtobuf()
	original, err := structpb.NewStruct(map[string]any{
		"id":     float64(42),
		"name":   "round-trip",
		"nested": map[string]any{"ok": true},
		"list":   []any{float64(1), float64(2), float64(3)},
	})
	require.NoError(t, err)

	b, err := c.Encode(original)
	require.NoError(t, err)

	var decoded structpb.Struct
	require.NoError(t, c.Decode(b, &decoded))
	assert.True(t, proto.Equal(original, &decoded))
}

func TestNewProtobuf_RoundTrip_Wrapper(t *testing.T) {
	c := codec.NewProtobuf()
	original := wrapperspb.String("hello")

	b, err := c.Encode(original)
	require.NoError(t, err)

	var decoded wrapperspb.StringValue
	require.NoError(t, c.Decode(b, &decoded))
	assert.True(t, proto.Equal(original, &decoded))
}

func TestNewProtobuf_Encode_nonProtoMessage_returnsErrInvalidMessage(t *testing.T) {
	c := codec.NewProtobuf()
	_, err := c.Encode(order{ID: 1, Name: "test"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, codec.ErrInvalidMessage), "expected codec.ErrInvalidMessage, got: %v", err)
}

func TestNewProtobuf_Encode_nilProtoPointer_returnsErrInvalidMessage(t *testing.T) {
	c := codec.NewProtobuf()
	var ts *timestamppb.Timestamp // typed-nil proto.Message
	// A typed-nil pointer marshals to empty bytes with no error, which would
	// silently publish an empty body. Encode must reject it with
	// ErrInvalidMessage, mirroring Decode's nil-destination guard.
	_, err := c.Encode(ts)
	require.Error(t, err)
	assert.True(t, errors.Is(err, codec.ErrInvalidMessage), "expected codec.ErrInvalidMessage, got: %v", err)
}

func TestNewProtobuf_Decode_nonProtoMessage_returnsErrInvalidMessage(t *testing.T) {
	c := codec.NewProtobuf()
	var o order
	// The wire bytes are irrelevant: the type assertion rejects the non-proto
	// destination before any unmarshal is attempted.
	err := c.Decode(nil, &o)
	require.Error(t, err)
	assert.True(t, errors.Is(err, codec.ErrInvalidMessage), "expected codec.ErrInvalidMessage, got: %v", err)
}

func TestNewProtobuf_Decode_nilProtoPointer_returnsErrInvalidMessage(t *testing.T) {
	c := codec.NewProtobuf()
	var ts *timestamppb.Timestamp // typed-nil proto.Message
	// Must surface ErrInvalidMessage, never panic: proto.Unmarshal into a nil
	// pointer dereferences nil. Mirrors the JSON codec, which errors (not panics)
	// on a nil destination.
	err := c.Decode([]byte{0x08, 0x01}, ts)
	require.Error(t, err)
	assert.True(t, errors.Is(err, codec.ErrInvalidMessage), "expected codec.ErrInvalidMessage, got: %v", err)
}

func TestNewProtobuf_Decode_malformedWire_returnsErrInvalidMessage(t *testing.T) {
	c := codec.NewProtobuf()
	var ts timestamppb.Timestamp
	// 0x08 is a varint tag for field 1 with no following payload — truncated wire.
	err := c.Decode([]byte{0x08}, &ts)
	require.Error(t, err)
	assert.True(t, errors.Is(err, codec.ErrInvalidMessage), "expected codec.ErrInvalidMessage, got: %v", err)
}

func TestNewProtobuf_Decode_emptyBytes_yieldsZeroMessage(t *testing.T) {
	c := codec.NewProtobuf()
	var ts timestamppb.Timestamp
	require.NoError(t, c.Decode(nil, &ts))
	assert.True(t, proto.Equal(&timestamppb.Timestamp{}, &ts))
}
