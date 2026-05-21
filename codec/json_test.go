package codec_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/amqp/codec"
)

type order struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// NewJSON — strict mode

func TestNewJSON_ContentType(t *testing.T) {
	c := codec.NewJSON()
	assert.Equal(t, "application/json", c.ContentType())
}

func TestNewJSON_Encode(t *testing.T) {
	c := codec.NewJSON()
	b, err := c.Encode(order{ID: 1, Name: "test"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"id":1,"name":"test"}`, string(b))
}

func TestNewJSON_Decode_valid(t *testing.T) {
	c := codec.NewJSON()
	var o order
	err := c.Decode([]byte(`{"id":1,"name":"test"}`), &o)
	require.NoError(t, err)
	assert.Equal(t, order{ID: 1, Name: "test"}, o)
}

func TestNewJSON_Decode_unknownField_returnsErrInvalidMessage(t *testing.T) {
	c := codec.NewJSON()
	var o order
	err := c.Decode([]byte(`{"id":1,"name":"test","extra":true}`), &o)
	require.Error(t, err)
	assert.True(t, errors.Is(err, codec.ErrInvalidMessage), "expected codec.ErrInvalidMessage, got: %v", err)
}

// NewJSONLax — tolerates unknown fields

func TestNewJSONLax_ContentType(t *testing.T) {
	c := codec.NewJSONLax()
	assert.Equal(t, "application/json", c.ContentType())
}

func TestNewJSONLax_Decode_unknownField_ok(t *testing.T) {
	c := codec.NewJSONLax()
	var o order
	err := c.Decode([]byte(`{"id":1,"name":"test","extra":true}`), &o)
	require.NoError(t, err)
	assert.Equal(t, order{ID: 1, Name: "test"}, o)
}

func TestNewJSONLax_Decode_invalidJSON_returnsErrInvalidMessage(t *testing.T) {
	c := codec.NewJSONLax()
	var o order
	err := c.Decode([]byte(`not json`), &o)
	require.Error(t, err)
	assert.True(t, errors.Is(err, codec.ErrInvalidMessage))
}

// Encode error case

func TestNewJSON_Encode_unencodableValue_returnsErrInvalidMessage(t *testing.T) {
	c := codec.NewJSON()
	_, err := c.Encode(make(chan int)) // channels are not JSON-encodable
	require.Error(t, err)
	assert.True(t, errors.Is(err, codec.ErrInvalidMessage))
}

// Round-trip property tests

func TestNewJSON_RoundTrip(t *testing.T) {
	c := codec.NewJSON()
	original := order{ID: 42, Name: "round-trip"}
	b, err := c.Encode(original)
	require.NoError(t, err)
	var decoded order
	require.NoError(t, c.Decode(b, &decoded))
	assert.Equal(t, original, decoded)
}

func TestNewJSONLax_RoundTrip(t *testing.T) {
	c := codec.NewJSONLax()
	original := order{ID: 99, Name: "lax-round-trip"}
	b, err := c.Encode(original)
	require.NoError(t, err)
	var decoded order
	require.NoError(t, c.Decode(b, &decoded))
	assert.Equal(t, original, decoded)
}
