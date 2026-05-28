package codec_test

import (
	"encoding/json"
	"testing"
	"time"

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
	original := &codec.CloudEvent{
		ID:              "id-1",
		Source:          "/services/orders",
		SpecVersion:     "1.0",
		Type:            "com.example.order.created",
		DataContentType: "application/json",
		Subject:         "order/42",
		Time:            time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC),
		Data:            []byte(`{"order_id":42,"total":9.5}`),
	}

	body, err := c.Encode(original)
	require.NoError(t, err)

	// data must be inlined under "data" (not data_base64) for JSON content types.
	var envelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &envelope))
	assert.Contains(t, envelope, "data")
	assert.NotContains(t, envelope, "data_base64")

	var decoded codec.CloudEvent
	require.NoError(t, c.Decode(body, &decoded))

	assert.Equal(t, original.ID, decoded.ID)
	assert.Equal(t, original.Source, decoded.Source)
	assert.Equal(t, original.SpecVersion, decoded.SpecVersion)
	assert.Equal(t, original.Type, decoded.Type)
	assert.Equal(t, original.DataContentType, decoded.DataContentType)
	assert.Equal(t, original.Subject, decoded.Subject)
	assert.True(t, original.Time.Equal(decoded.Time), "time mismatch: %s vs %s", original.Time, decoded.Time)
	assert.JSONEq(t, string(original.Data), string(decoded.Data))
}

func TestCloudEventsStructured_RoundTrip_Base64Data(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	original := &codec.CloudEvent{
		ID:              "id-2",
		Source:          "/services/files",
		SpecVersion:     "1.0",
		Type:            "com.example.blob",
		DataContentType: "application/octet-stream",
		Data:            []byte{0x00, 0x01, 0x02, 0xff, 0xfe},
	}

	body, err := c.Encode(original)
	require.NoError(t, err)

	// Non-JSON data must be base64-encoded under "data_base64".
	var envelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &envelope))
	assert.Contains(t, envelope, "data_base64")
	assert.NotContains(t, envelope, "data")

	var decoded codec.CloudEvent
	require.NoError(t, c.Decode(body, &decoded))
	assert.Equal(t, original.Data, decoded.Data)
	assert.Equal(t, original.DataContentType, decoded.DataContentType)
}

func TestCloudEventsStructured_RoundTrip_Extensions(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	original := &codec.CloudEvent{
		ID:          "id-3",
		Source:      "/x",
		SpecVersion: "1.0",
		Type:        "t",
		Extensions: map[string]string{
			"traceparentish": "abc",
			"tenant":         "acme",
		},
	}

	body, err := c.Encode(original)
	require.NoError(t, err)

	var decoded codec.CloudEvent
	require.NoError(t, c.Decode(body, &decoded))
	assert.Equal(t, original.Extensions, decoded.Extensions)
}

func TestCloudEventsStructured_Encode_RejectsNonCloudEvent(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	_, err := c.Encode(map[string]any{"not": "a cloudevent"})
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

func TestCloudEventsStructured_Decode_RejectsBothDataMembers(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	var ev codec.CloudEvent
	body := []byte(`{"specversion":"1.0","id":"x","source":"s","type":"t","data":{"a":1},"data_base64":"AAEC"}`)
	err := c.Decode(body, &ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsStructured_Encode_JSONContentTypeRejectsNonJSONData(t *testing.T) {
	c := codec.NewCloudEventsStructured()
	ev := &codec.CloudEvent{
		ID:              "id",
		Source:          "s",
		SpecVersion:     "1.0",
		Type:            "t",
		DataContentType: "application/json",
		Data:            []byte("not json at all"),
	}
	_, err := c.Encode(ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

// ensure the structured codec satisfies the Codec interface.
var _ codec.Codec = codec.NewCloudEventsStructured()
