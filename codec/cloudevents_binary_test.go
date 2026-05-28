package codec_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/codec"
)

func newBinary(t *testing.T) codec.HeaderCodec {
	t.Helper()
	hc, ok := codec.NewCloudEventsBinary().(codec.HeaderCodec)
	require.True(t, ok, "binary codec must implement codec.HeaderCodec")
	return hc
}

func TestNewCloudEventsBinary_ImplementsHeaderCodec(t *testing.T) {
	_, ok := codec.NewCloudEventsBinary().(codec.HeaderCodec)
	assert.True(t, ok)
}

func TestNewCloudEventsBinary_ContentType(t *testing.T) {
	// datacontenttype travels as the ce-datacontenttype header, so the codec
	// reports no static AMQP content-type.
	assert.Equal(t, "", codec.NewCloudEventsBinary().ContentType())
}

func TestCloudEventsBinary_RoundTrip(t *testing.T) {
	c := newBinary(t)
	original := &codec.CloudEvent{
		ID:              "id-1",
		Source:          "/services/orders",
		SpecVersion:     "1.0",
		Type:            "com.example.order.created",
		DataContentType: "application/json",
		DataSchema:      "https://example.com/schema",
		Subject:         "order/42",
		Time:            time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Data:            []byte(`{"order_id":42}`),
	}

	body, headers, err := c.EncodeWithHeaders(original)
	require.NoError(t, err)

	// Body carries data only.
	assert.Equal(t, original.Data, body)

	// Attributes travel as ce-* headers.
	assert.Equal(t, "id-1", headers["ce-id"])
	assert.Equal(t, "/services/orders", headers["ce-source"])
	assert.Equal(t, "com.example.order.created", headers["ce-type"])
	assert.Equal(t, "1.0", headers["ce-specversion"])
	assert.Equal(t, "order/42", headers["ce-subject"])
	assert.Equal(t, "application/json", headers["ce-datacontenttype"])
	assert.Equal(t, "https://example.com/schema", headers["ce-dataschema"])
	assert.Contains(t, headers, "ce-time")

	var decoded codec.CloudEvent
	require.NoError(t, c.DecodeWithHeaders(body, headers, &decoded))
	assert.Equal(t, original.ID, decoded.ID)
	assert.Equal(t, original.Source, decoded.Source)
	assert.Equal(t, original.SpecVersion, decoded.SpecVersion)
	assert.Equal(t, original.Type, decoded.Type)
	assert.Equal(t, original.DataContentType, decoded.DataContentType)
	assert.Equal(t, original.DataSchema, decoded.DataSchema)
	assert.Equal(t, original.Subject, decoded.Subject)
	assert.True(t, original.Time.Equal(decoded.Time))
	assert.Equal(t, original.Data, decoded.Data)
}

func TestCloudEventsBinary_RoundTrip_Extensions(t *testing.T) {
	c := newBinary(t)
	original := &codec.CloudEvent{
		ID:          "id-2",
		Source:      "/x",
		SpecVersion: "1.0",
		Type:        "t",
		Extensions: map[string]string{
			"tenant":   "acme",
			"priority": "high",
		},
		Data: []byte("hello"),
	}

	body, headers, err := c.EncodeWithHeaders(original)
	require.NoError(t, err)
	assert.Equal(t, "acme", headers["ce-tenant"])
	assert.Equal(t, "high", headers["ce-priority"])

	var decoded codec.CloudEvent
	require.NoError(t, c.DecodeWithHeaders(body, headers, &decoded))
	assert.Equal(t, original.Extensions, decoded.Extensions)
	assert.Equal(t, original.Data, decoded.Data)
}

func TestCloudEventsBinary_Encode_DefaultsSpecVersion(t *testing.T) {
	c := newBinary(t)
	_, headers, err := c.EncodeWithHeaders(&codec.CloudEvent{ID: "x", Source: "s", Type: "t"})
	require.NoError(t, err)
	assert.Equal(t, "1.0", headers["ce-specversion"])
}

func TestCloudEventsBinary_Decode_MissingSpecVersionFails(t *testing.T) {
	c := newBinary(t)
	var ev codec.CloudEvent
	err := c.DecodeWithHeaders([]byte("data"), map[string]any{"ce-id": "x"}, &ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

// Cross-encoding: a structured-encoded message (full envelope in the body, no
// ce-* headers) must fail cleanly when fed to the binary decoder.
func TestCloudEventsBinary_CrossEncoding_StructuredFailsBinary(t *testing.T) {
	structured := codec.NewCloudEventsStructured()
	body, err := structured.Encode(&codec.CloudEvent{
		ID: "x", Source: "s", SpecVersion: "1.0", Type: "t", Data: []byte(`{"a":1}`),
	})
	require.NoError(t, err)

	c := newBinary(t)
	var ev codec.CloudEvent
	err = c.DecodeWithHeaders(body, map[string]any{}, &ev) // structured carries no ce-* headers
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_Decode_ByteSliceHeaderValues(t *testing.T) {
	// amqp091 may surface short/long strings as []byte; coerce to string.
	c := newBinary(t)
	var ev codec.CloudEvent
	headers := map[string]any{
		"ce-specversion": []byte("1.0"),
		"ce-id":          []byte("id-9"),
		"ce-source":      "/s",
		"ce-type":        "t",
	}
	require.NoError(t, c.DecodeWithHeaders([]byte("body"), headers, &ev))
	assert.Equal(t, "1.0", ev.SpecVersion)
	assert.Equal(t, "id-9", ev.ID)
}

func TestCloudEventsBinary_Decode_IgnoresNonCEHeaders(t *testing.T) {
	c := newBinary(t)
	var ev codec.CloudEvent
	headers := map[string]any{
		"ce-specversion": "1.0",
		"ce-id":          "id",
		"ce-source":      "/s",
		"ce-type":        "t",
		"traceparent":    "00-abc-def-01",
		"x-custom":       "keep-out",
	}
	require.NoError(t, c.DecodeWithHeaders([]byte("body"), headers, &ev))
	assert.NotContains(t, ev.Extensions, "traceparent")
	assert.NotContains(t, ev.Extensions, "x-custom")
	// Non-ce headers must not be mutated/removed by the codec.
	assert.Equal(t, "00-abc-def-01", headers["traceparent"])
}

func TestCloudEventsBinary_PlainEncodeRejected(t *testing.T) {
	c := codec.NewCloudEventsBinary()
	_, err := c.Encode(&codec.CloudEvent{ID: "x", Source: "s", SpecVersion: "1.0", Type: "t"})
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_PlainDecodeRejected(t *testing.T) {
	c := codec.NewCloudEventsBinary()
	var ev codec.CloudEvent
	err := c.Decode([]byte("body"), &ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_Encode_RejectsNonCloudEvent(t *testing.T) {
	c := newBinary(t)
	_, _, err := c.EncodeWithHeaders("not an event")
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_Decode_RejectsNilDestination(t *testing.T) {
	c := newBinary(t)
	err := c.DecodeWithHeaders([]byte("body"), map[string]any{"ce-specversion": "1.0"}, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}
