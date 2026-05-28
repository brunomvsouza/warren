package codec_test

import (
	"encoding/json"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/codec"
)

func TestNewCloudEventsStructured_ContentType(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	assert.Equal(t, "application/cloudevents+json", c.ContentType())
}

func TestCloudEventsStructured_RoundTrip_JSONData(t *testing.T) {
	c := codec.NewCloudEventsStructured()

	original := cloudevents.NewEvent()
	original.SetID("id-1")
	original.SetSource("/services/orders")
	original.SetType("com.example.order.created")
	original.SetSubject("order/42")
	original.SetTime(time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC))
	require.NoError(t, original.SetData(cloudevents.ApplicationJSON, map[string]any{"order_id": 42, "total": 9.5}))

	body, err := c.Encode(&original)
	require.NoError(t, err)

	// JSON data is inlined under "data" (not data_base64).
	var envelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &envelope))
	assert.Equal(t, "application/cloudevents+json", c.ContentType())
	assert.Contains(t, envelope, "data")
	assert.NotContains(t, envelope, "data_base64")

	var decoded codec.CloudEvent
	require.NoError(t, c.Decode(body, &decoded))
	assert.Equal(t, original.ID(), decoded.ID())
	assert.Equal(t, original.Source(), decoded.Source())
	assert.Equal(t, original.Type(), decoded.Type())
	assert.Equal(t, original.Subject(), decoded.Subject())
	assert.Equal(t, original.DataContentType(), decoded.DataContentType())
	assert.True(t, original.Time().Equal(decoded.Time()))
	assert.JSONEq(t, string(original.Data()), string(decoded.Data()))
}

func TestCloudEventsStructured_RoundTrip_BinaryDataBase64(t *testing.T) {
	c := codec.NewCloudEventsStructured()

	original := cloudevents.NewEvent()
	original.SetID("id-2")
	original.SetSource("/services/files")
	original.SetType("com.example.blob")
	require.NoError(t, original.SetData("application/octet-stream", []byte{0x00, 0x01, 0x02, 0xff, 0xfe}))

	body, err := c.Encode(&original)
	require.NoError(t, err)

	// Non-JSON data is base64-encoded under "data_base64".
	var envelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &envelope))
	assert.Contains(t, envelope, "data_base64")

	var decoded codec.CloudEvent
	require.NoError(t, c.Decode(body, &decoded))
	assert.Equal(t, original.Data(), decoded.Data())
}

func TestCloudEventsStructured_RoundTrip_Extensions(t *testing.T) {
	c := codec.NewCloudEventsStructured()

	original := cloudevents.NewEvent()
	original.SetID("id-3")
	original.SetSource("/x")
	original.SetType("t")
	original.SetExtension("tenant", "acme")

	body, err := c.Encode(&original)
	require.NoError(t, err)

	var decoded codec.CloudEvent
	require.NoError(t, c.Decode(body, &decoded))
	assert.Equal(t, "acme", decoded.Extensions()["tenant"])
}

func TestCloudEventsStructured_Encode_RejectsNonEvent(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	_, err := c.Encode(map[string]any{"not": "an event"})
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsStructured_Encode_RejectsInvalidEvent(t *testing.T) {
	// Missing required attributes (id/source/type) -> Validate fails -> ErrInvalidMessage.
	c := codec.NewCloudEventsStructured()
	ev := cloudevents.NewEvent()
	_, err := c.Encode(&ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsStructured_Decode_RejectsNilDestination(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	err := c.Decode([]byte(`{"specversion":"1.0","id":"x","source":"s","type":"t"}`), nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsStructured_Decode_InvalidJSON(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	var ev codec.CloudEvent
	err := c.Decode([]byte(`{not json`), &ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsStructured_Decode_MissingSpecVersion(t *testing.T) {
	// Valid JSON but not a CloudEvent (no specversion) -> the SDK rejects it.
	c := codec.NewCloudEventsStructured()
	var ev codec.CloudEvent
	err := c.Decode([]byte(`{"id":"x","source":"/s","type":"t"}`), &ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

// Structured mode serialises through the SDK's JSON event format, which preserves
// the JSON type of an extension (contrast with binary mode, which narrows it to a
// string — see TestCloudEventsBinary_RoundTrip_NonStringExtensionNarrowsToString).
func TestCloudEventsStructured_RoundTrip_PreservesExtensionType(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	original := cloudevents.NewEvent()
	original.SetID("id-x")
	original.SetSource("/s")
	original.SetType("t")
	original.SetExtension("count", 7)

	body, err := c.Encode(&original)
	require.NoError(t, err)

	var decoded codec.CloudEvent
	require.NoError(t, c.Decode(body, &decoded))
	assert.Equal(t, int32(7), decoded.Extensions()["count"], "structured mode preserves the numeric extension type")
}

func TestCloudEventsStructured_NotHeaderCodec(t *testing.T) {
	// Structured mode carries everything in the body; it must NOT be a HeaderCodec.
	_, ok := codec.NewCloudEventsStructured().(codec.HeaderCodec)
	assert.False(t, ok)
}

// ensure the structured codec satisfies the Codec interface.
var _ codec.Codec = codec.NewCloudEventsStructured()
