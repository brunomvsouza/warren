package codec

import (
	"errors"
	"strconv"
	"testing"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ceBinaryHeadersWithExtensions builds a minimal valid CloudEvents binary header
// set (the four required core attributes) plus n string extensions, so the
// extension-count bound can be exercised at and just past the cap.
func ceBinaryHeadersWithExtensions(n int) map[string]any {
	h := map[string]any{
		ceAMQPPrefix + "specversion": "1.0",
		ceAMQPPrefix + "id":          "id-cap",
		ceAMQPPrefix + "source":      "/cap",
		ceAMQPPrefix + "type":        "t",
	}
	for i := range n {
		h[ceAMQPPrefix+"ext"+strconv.Itoa(i)] = "v"
	}
	return h
}

// — LATER-60: extension-count upper bound on the binary decode path ————————————

func TestCloudEventsBinary_DecodeWithHeaders_RejectsTooManyExtensions(t *testing.T) {
	c := &ceBinaryCodec{}

	var decoded event.Event
	err := c.DecodeWithHeaders(nil, ceBinaryHeadersWithExtensions(maxCEBinaryExtensions+1), "", &decoded)
	require.Error(t, err, "a delivery exceeding the extension cap must be rejected")
	assert.ErrorIs(t, err, ErrInvalidMessage)
}

func TestCloudEventsBinary_DecodeWithHeaders_AcceptsExtensionsAtCap(t *testing.T) {
	c := &ceBinaryCodec{}

	var decoded event.Event
	err := c.DecodeWithHeaders(nil, ceBinaryHeadersWithExtensions(maxCEBinaryExtensions), "", &decoded)
	require.NoError(t, err, "exactly the cap number of extensions must still decode")
	assert.Len(t, decoded.Extensions(), maxCEBinaryExtensions,
		"every extension up to the cap must be preserved")
}

// — LATER-61: structured Encode marshal-failure branch via the injected marshaler —

func TestCloudEventsStructured_Encode_MarshalFailureWrapsErrInvalidMessage(t *testing.T) {
	sentinel := errors.New("synthetic marshal failure")
	// Inject a failing marshaler per-instance (no mutable package global): a
	// validated event always marshals in practice, so this is the only way to
	// reach the json.Marshal error branch.
	c := &ceStructuredCodec{marshal: func(any) ([]byte, error) { return nil, sentinel }}
	ev := event.New()
	ev.SetID("id-1")
	ev.SetSource("/x")
	ev.SetType("t")
	// A validated event normally always marshals; the seam forces the otherwise
	// unreachable json.Marshal error branch so the wrap contract is covered.
	_, err := c.Encode(&ev)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMessage, "a marshal failure must wrap ErrInvalidMessage")
	assert.ErrorIs(t, err, sentinel, "the underlying marshal error must remain in the chain")
}

// TestCloudEventsStructured_ZeroValue_Encodes pins that a zero-value codec (no
// injected marshaler) still encodes by falling back to json.Marshal, so the
// per-instance marshal field cannot become a nil-call footgun for an internal
// caller that bypasses NewCloudEventsStructured (security audit INFO-1).
func TestCloudEventsStructured_ZeroValue_Encodes(t *testing.T) {
	c := &ceStructuredCodec{} // marshal field left nil
	ev := event.New()
	ev.SetID("id-zero")
	ev.SetSource("/x")
	ev.SetType("t")

	out, err := c.Encode(&ev)
	require.NoError(t, err, "a zero-value codec must fall back to json.Marshal, not panic")
	assert.NotEmpty(t, out)
}

// — LATER-60: non-string cloudEvents: headers must not count toward the cap ————

// TestCloudEventsBinary_DecodeWithHeaders_NonStringHeadersDoNotCountTowardCap
// pins that a delivery carrying exactly maxCEBinaryExtensions string extensions
// PLUS several non-string-typed cloudEvents: headers still decodes: ceHeaderString
// treats a value that is neither string nor []byte as absent (returns ok=false),
// so the decode loop `continue`s before extCount++, and such headers cannot push
// a legitimate payload over the cap (test-engineer Gap-1). Note: []byte IS the
// AMQP wire form of a string, so it counts — only genuinely non-string types
// (int/bool/table/slice) are skipped.
func TestCloudEventsBinary_DecodeWithHeaders_NonStringHeadersDoNotCountTowardCap(t *testing.T) {
	c := &ceBinaryCodec{}

	headers := ceBinaryHeadersWithExtensions(maxCEBinaryExtensions)
	// Non-string-typed cloudEvents: headers: treated as absent on decode, so they
	// are skipped before the extension counter increments.
	headers[ceAMQPPrefix+"intext"] = int32(7)
	headers[ceAMQPPrefix+"boolext"] = true

	var decoded event.Event
	err := c.DecodeWithHeaders(nil, headers, "", &decoded)
	require.NoError(t, err,
		"non-string-typed cloudEvents: headers must not count toward the extension cap")
	assert.Len(t, decoded.Extensions(), maxCEBinaryExtensions,
		"only the string extensions up to the cap are reconstructed")
}
