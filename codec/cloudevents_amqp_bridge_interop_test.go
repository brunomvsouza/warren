package codec_test

import (
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/codec"
)

// TestCloudEventsBinary_AMQP091To10BridgeInterop pins the cross-protocol interop
// contract documented in SPEC §6.9: warren publishes binary-mode CloudEvents over
// AMQP 0-9-1 (amqp091-go) as cloudEvents:-prefixed message headers, and RabbitMQ
// bridges those headers one-to-one to AMQP 1.0 application-properties of the
// identical name (native AMQP 1.0 is a core protocol in RabbitMQ 4.0; a plugin on
// 3.13 — the property-name mapping is the same on both). A non-Go AMQP-1.0
// CloudEvents client therefore reads warren's attributes unchanged.
//
// The test asserts the bridge from the FOREIGN side rather than through warren's
// own encoder: it constructs the application-property set exactly as the
// CloudEvents AMQP Protocol Binding mandates (prefix + lowercase attribute name),
// delivers the values as []byte (the form amqp091 yields for an AMQP longstr
// arriving over the bridge), and asserts warren reconstructs the event. It then
// re-encodes with warren and asserts the emitted header key set is byte-identical
// to the binding's property set — proving the 0-9-1 header names equal the
// AMQP-1.0 application-property names, so the bridge is name-preserving in both
// directions. Both CloudEvents spec versions (1.0 and 0.3) are exercised because
// the bridge must carry either across unchanged.
func TestCloudEventsBinary_AMQP091To10BridgeInterop(t *testing.T) {
	hc, ok := codec.NewCloudEventsBinary().(codec.HeaderCodec)
	require.True(t, ok, "binary codec must implement codec.HeaderCodec")

	for _, specVersion := range []string{cloudevents.VersionV1, cloudevents.VersionV03} {
		t.Run("specversion="+specVersion, func(t *testing.T) {
			const (
				id          = "id-bridge-1"
				source      = "/services/orders"
				eventType   = "com.example.order.created"
				contentType = "application/json"
			)
			body := []byte(`{"order_id":42}`)

			// Foreign AMQP-1.0 producer: application-properties exactly as the
			// CloudEvents AMQP binding defines them, delivered as []byte (the AMQP
			// longstr form amqp091 surfaces after the 1.0->0-9-1 header bridge).
			foreignProps := map[string]any{
				"cloudEvents:specversion": []byte(specVersion),
				"cloudEvents:id":          []byte(id),
				"cloudEvents:source":      []byte(source),
				"cloudEvents:type":        []byte(eventType),
			}

			// Inbound bridge: warren reconstructs the event from foreign properties.
			var decoded codec.CloudEvent
			require.NoError(t, hc.DecodeWithHeaders(body, foreignProps, contentType, &decoded))
			assert.Equal(t, specVersion, decoded.SpecVersion())
			assert.Equal(t, id, decoded.ID())
			assert.Equal(t, source, decoded.Source())
			assert.Equal(t, eventType, decoded.Type())
			assert.Equal(t, contentType, decoded.DataContentType())
			assert.Equal(t, body, decoded.Data())

			// Outbound bridge: warren's emitted header key set is identical to the
			// binding's property set, so a foreign AMQP-1.0 consumer reads it back.
			outBody, outHeaders, outContentType, err := hc.EncodeWithHeaders(&decoded)
			require.NoError(t, err)
			assert.Equal(t, body, outBody)
			assert.Equal(t, contentType, outContentType)

			gotKeys := make([]string, 0, len(outHeaders))
			for k := range outHeaders {
				gotKeys = append(gotKeys, k)
			}
			assert.ElementsMatch(t, []string{
				"cloudEvents:specversion",
				"cloudEvents:id",
				"cloudEvents:source",
				"cloudEvents:type",
			}, gotKeys, "emitted header names must equal the AMQP-1.0 application-property names")
			for k, v := range outHeaders {
				// The bridge carries every core attribute as a string property; a foreign
				// consumer reads strings, so warren must not emit a non-string value.
				_, isString := v.(string)
				assert.Truef(t, isString, "property %q must be a string for the AMQP-1.0 bridge, got %T", k, v)
			}
		})
	}
}
