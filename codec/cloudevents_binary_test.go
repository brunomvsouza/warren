package codec_test

import (
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
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
	// The per-event datacontenttype is supplied dynamically by EncodeWithHeaders.
	assert.Equal(t, "", codec.NewCloudEventsBinary().ContentType())
}

func TestCloudEventsBinary_RoundTrip(t *testing.T) {
	c := newBinary(t)

	original := cloudevents.NewEvent()
	original.SetID("id-1")
	original.SetSource("/services/orders")
	original.SetType("com.example.order.created")
	original.SetSubject("order/42")
	original.SetTime(time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC))
	require.NoError(t, original.SetData(cloudevents.ApplicationJSON, map[string]any{"order_id": 42}))

	body, headers, contentType, err := c.EncodeWithHeaders(&original)
	require.NoError(t, err)

	// Body carries data only; datacontenttype is the content-type property.
	assert.Equal(t, original.Data(), body)
	assert.Equal(t, "application/json", contentType)

	// Per the CloudEvents AMQP binding, attributes use the cloudEvents: prefix.
	assert.Equal(t, "id-1", headers["cloudEvents:id"])
	assert.Equal(t, "/services/orders", headers["cloudEvents:source"])
	assert.Equal(t, "com.example.order.created", headers["cloudEvents:type"])
	assert.Equal(t, "1.0", headers["cloudEvents:specversion"])
	assert.Equal(t, "order/42", headers["cloudEvents:subject"])
	assert.Contains(t, headers, "cloudEvents:time")
	// datacontenttype is NOT a header — it is the content-type property.
	assert.NotContains(t, headers, "cloudEvents:datacontenttype")

	var decoded codec.CloudEvent
	require.NoError(t, c.DecodeWithHeaders(body, headers, contentType, &decoded))
	assert.Equal(t, original.ID(), decoded.ID())
	assert.Equal(t, original.Source(), decoded.Source())
	assert.Equal(t, original.Type(), decoded.Type())
	assert.Equal(t, original.SpecVersion(), decoded.SpecVersion())
	assert.Equal(t, original.Subject(), decoded.Subject())
	assert.Equal(t, original.DataContentType(), decoded.DataContentType())
	assert.True(t, original.Time().Equal(decoded.Time()))
	assert.Equal(t, original.Data(), decoded.Data())
}

func TestCloudEventsBinary_RoundTrip_Extensions(t *testing.T) {
	c := newBinary(t)

	original := cloudevents.NewEvent()
	original.SetID("id-2")
	original.SetSource("/x")
	original.SetType("t")
	original.SetExtension("tenant", "acme")
	original.SetExtension("priority", "high")
	require.NoError(t, original.SetData(cloudevents.ApplicationJSON, map[string]any{"k": 1}))

	body, headers, contentType, err := c.EncodeWithHeaders(&original)
	require.NoError(t, err)
	assert.Equal(t, "acme", headers["cloudEvents:tenant"])
	assert.Equal(t, "high", headers["cloudEvents:priority"])

	var decoded codec.CloudEvent
	require.NoError(t, c.DecodeWithHeaders(body, headers, contentType, &decoded))
	assert.Equal(t, "acme", decoded.Extensions()["tenant"])
	assert.Equal(t, "high", decoded.Extensions()["priority"])
}

func TestCloudEventsBinary_RoundTrip_NoDataNoContentType(t *testing.T) {
	c := newBinary(t)

	// An event with no data carries no datacontenttype; per the binding the body
	// is empty and the content-type property is absent.
	original := cloudevents.NewEvent()
	original.SetID("id-empty")
	original.SetSource("/s")
	original.SetType("t")

	body, headers, contentType, err := c.EncodeWithHeaders(&original)
	require.NoError(t, err)
	assert.Empty(t, body)
	assert.Empty(t, contentType)
	assert.NotContains(t, headers, "cloudEvents:datacontenttype")
	assert.Equal(t, "id-empty", headers["cloudEvents:id"])

	var decoded codec.CloudEvent
	require.NoError(t, c.DecodeWithHeaders(body, headers, contentType, &decoded))
	assert.Equal(t, "id-empty", decoded.ID())
	assert.Empty(t, decoded.DataContentType())
	assert.Empty(t, decoded.Data())
}

func TestCloudEventsBinary_Decode_MissingSpecVersionFails(t *testing.T) {
	c := newBinary(t)
	var ev codec.CloudEvent
	err := c.DecodeWithHeaders([]byte("data"), map[string]any{"cloudEvents:id": "x"}, "", &ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

// Cross-encoding: a structured-encoded message (full envelope in the body, no
// cloudEvents: headers) must fail cleanly when fed to the binary decoder.
func TestCloudEventsBinary_CrossEncoding_StructuredFailsBinary(t *testing.T) {
	structured := codec.NewCloudEventsStructured()
	ev := cloudevents.NewEvent()
	ev.SetID("x")
	ev.SetSource("s")
	ev.SetType("t")
	require.NoError(t, ev.SetData(cloudevents.ApplicationJSON, map[string]any{"a": 1}))
	body, err := structured.Encode(&ev)
	require.NoError(t, err)

	c := newBinary(t)
	var got codec.CloudEvent
	err = c.DecodeWithHeaders(body, map[string]any{}, "application/cloudevents+json", &got)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_Decode_ByteSliceHeaderValues(t *testing.T) {
	// amqp091 may surface short/long strings as []byte; coerce to string.
	c := newBinary(t)
	var ev codec.CloudEvent
	headers := map[string]any{
		"cloudEvents:specversion": []byte("1.0"),
		"cloudEvents:id":          []byte("id-9"),
		"cloudEvents:source":      "/s",
		"cloudEvents:type":        "t",
	}
	require.NoError(t, c.DecodeWithHeaders([]byte("body"), headers, "", &ev))
	assert.Equal(t, "1.0", ev.SpecVersion())
	assert.Equal(t, "id-9", ev.ID())
}

func TestCloudEventsBinary_Decode_IgnoresNonCEHeaders(t *testing.T) {
	c := newBinary(t)
	var ev codec.CloudEvent
	headers := map[string]any{
		"cloudEvents:specversion": "1.0",
		"cloudEvents:id":          "id",
		"cloudEvents:source":      "/s",
		"cloudEvents:type":        "t",
		"traceparent":             "00-abc-def-01",
		"x-custom":                "keep-out",
	}
	require.NoError(t, c.DecodeWithHeaders([]byte("body"), headers, "", &ev))
	assert.NotContains(t, ev.Extensions(), "traceparent")
	assert.NotContains(t, ev.Extensions(), "x-custom")
	// Non-ce headers must not be mutated/removed by the codec.
	assert.Equal(t, "00-abc-def-01", headers["traceparent"])
}

func TestCloudEventsBinary_PlainEncodeRejected(t *testing.T) {
	c := codec.NewCloudEventsBinary()
	ev := cloudevents.NewEvent()
	ev.SetID("x")
	ev.SetSource("s")
	ev.SetType("t")
	_, err := c.Encode(&ev)
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

func TestCloudEventsBinary_Encode_RejectsNonEvent(t *testing.T) {
	c := newBinary(t)
	_, _, _, err := c.EncodeWithHeaders("not an event")
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_Decode_RejectsNilDestination(t *testing.T) {
	c := newBinary(t)
	err := c.DecodeWithHeaders([]byte("body"), map[string]any{"cloudEvents:specversion": "1.0"}, "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

// A header that names an invalid CloudEvents extension (e.g. with a hyphen) is
// rejected by the SDK setter, which records the error internally rather than
// returning it. DecodeWithHeaders must surface it via Validate() instead of
// silently dropping the attribute and returning a malformed event.
func TestCloudEventsBinary_Decode_InvalidExtensionName_Rejected(t *testing.T) {
	c := newBinary(t)
	headers := map[string]any{
		"cloudEvents:specversion":  "1.0",
		"cloudEvents:id":           "id-1",
		"cloudEvents:source":       "/s",
		"cloudEvents:type":         "t",
		"cloudEvents:bad-ext-name": "v", // hyphen: invalid CE attribute name
	}
	var ev codec.CloudEvent
	err := c.DecodeWithHeaders([]byte("body"), headers, "", &ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

// A header whose value is neither string nor []byte (amqp091 may surface ints,
// bools, etc. from a non-conformant producer) is treated as absent rather than
// coerced via fmt. Here a required attribute (specversion) arriving non-string is
// missing -> clean rejection; an optional extension arriving non-string is dropped.
func TestCloudEventsBinary_Decode_NonStringExtensionTreatedAsAbsent(t *testing.T) {
	c := newBinary(t)
	headers := map[string]any{
		"cloudEvents:specversion": "1.0",
		"cloudEvents:id":          "id-1",
		"cloudEvents:source":      "/s",
		"cloudEvents:type":        "t",
		"cloudEvents:count":       int32(7), // non-string extension value
	}
	var ev codec.CloudEvent
	require.NoError(t, c.DecodeWithHeaders([]byte("body"), headers, "", &ev))
	assert.NotContains(t, ev.Extensions(), "count", "non-string extension value must not be coerced into the event")
}

func TestCloudEventsBinary_Encode_RejectsInvalidEvent(t *testing.T) {
	// Missing required attributes (id/source/type) -> Validate fails -> ErrInvalidMessage.
	c := newBinary(t)
	ev := cloudevents.NewEvent()
	_, _, _, err := c.EncodeWithHeaders(&ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_Encode_RejectsNilEvent(t *testing.T) {
	c := newBinary(t)
	_, _, _, err := c.EncodeWithHeaders((*cloudevents.Event)(nil))
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_Encode_AcceptsEventByValue(t *testing.T) {
	// asEvent accepts a value cloudevents.Event in addition to a pointer.
	c := newBinary(t)
	ev := cloudevents.NewEvent()
	ev.SetID("id-v")
	ev.SetSource("/s")
	ev.SetType("t")
	_, headers, _, err := c.EncodeWithHeaders(ev) // value, not pointer
	require.NoError(t, err)
	assert.Equal(t, "id-v", headers["cloudEvents:id"])
}

func TestCloudEventsBinary_Encode_RejectsUnformattableExtension(t *testing.T) {
	// An extension value that has no canonical CloudEvents type (here a struct) is
	// rejected with ErrInvalidMessage. The SDK's SetExtension does not even store
	// such a value, so ev.Validate() reports it before the encode loop reaches
	// types.Format; that types.Format error path is therefore defensive only.
	c := newBinary(t)
	ev := cloudevents.NewEvent()
	ev.SetID("id-e")
	ev.SetSource("/s")
	ev.SetType("t")
	ev.SetExtension("bad", struct{ X int }{X: 1})

	_, _, _, err := c.EncodeWithHeaders(&ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, codec.ErrInvalidMessage)
}

func TestCloudEventsBinary_RoundTrip_DataSchema(t *testing.T) {
	c := newBinary(t)
	original := cloudevents.NewEvent()
	original.SetID("id-ds")
	original.SetSource("/s")
	original.SetType("t")
	original.SetDataSchema("https://example.com/schema.json")
	require.NoError(t, original.SetData(cloudevents.ApplicationJSON, map[string]any{"k": 1}))

	body, headers, contentType, err := c.EncodeWithHeaders(&original)
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/schema.json", headers["cloudEvents:dataschema"])

	var decoded codec.CloudEvent
	require.NoError(t, c.DecodeWithHeaders(body, headers, contentType, &decoded))
	assert.Equal(t, original.DataSchema(), decoded.DataSchema())
}

// Binary mode carries every attribute as an AMQP string (per EncodeWithHeaders'
// types.Format), so a non-string extension round-trips back as its string form.
// This locks the documented type-narrowing behaviour (contrast with structured
// mode, which preserves the JSON type).
func TestCloudEventsBinary_RoundTrip_NonStringExtensionNarrowsToString(t *testing.T) {
	c := newBinary(t)
	original := cloudevents.NewEvent()
	original.SetID("id-n")
	original.SetSource("/s")
	original.SetType("t")
	original.SetExtension("count", 7) // int extension

	body, headers, contentType, err := c.EncodeWithHeaders(&original)
	require.NoError(t, err)
	assert.Equal(t, "7", headers["cloudEvents:count"], "extension is formatted to its canonical string form on the wire")

	var decoded codec.CloudEvent
	require.NoError(t, c.DecodeWithHeaders(body, headers, contentType, &decoded))
	assert.Equal(t, "7", decoded.Extensions()["count"], "binary mode narrows a non-string extension to its string form")
}
