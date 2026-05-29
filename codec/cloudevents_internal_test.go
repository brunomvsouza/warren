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
	for i := 0; i < n; i++ {
		h[ceAMQPPrefix+"ext"+strconv.Itoa(i)] = "v"
	}
	return h
}

// — LATER-60: extension-count upper bound on the binary decode path ————————————

func TestCloudEventsBinary_DecodeWithHeaders_RejectsTooManyExtensions(t *testing.T) {
	c := &ceBinaryCodec{}

	var decoded CloudEvent
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

// — LATER-61: structured Encode marshal-failure branch via the marshalEvent seam —

func TestCloudEventsStructured_Encode_MarshalFailureWrapsErrInvalidMessage(t *testing.T) {
	sentinel := errors.New("synthetic marshal failure")
	orig := marshalEvent
	marshalEvent = func(any) ([]byte, error) { return nil, sentinel }
	t.Cleanup(func() { marshalEvent = orig })

	c := &ceStructuredCodec{}
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
