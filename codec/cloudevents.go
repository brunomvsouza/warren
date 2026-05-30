package codec

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/types"
)

// CloudEvent is the canonical CloudEvents event type from the official Go SDK
// (github.com/cloudevents/sdk-go/v2). Use it as the message type M with the
// CloudEvents codecs; construct one with cloudevents.NewEvent(). Re-exporting
// the upstream type keeps the wire format faithful to clients in other
// languages.
type CloudEvent = event.Event

const (
	ceStructuredContentType = "application/cloudevents+json"
	// ceAMQPPrefix is the AMQP application-property prefix for CloudEvents binary
	// mode, matching the official Go SDK default (protocol/amqp/v2). RabbitMQ
	// bridges 0-9-1 headers to AMQP 1.0 application-properties, so a non-Go
	// AMQP-1.0 CloudEvents client reads these attributes unchanged.
	ceAMQPPrefix = "cloudEvents:"
	// maxCEBinaryExtensions bounds the number of cloudEvents:-prefixed extension
	// attributes the binary decoder reconstructs from one delivery's headers,
	// mirroring the maxHeaderDepth bounding philosophy. Realistic CloudEvents carry
	// a handful of extensions; the AMQP frame size and shortstr (≤255B) header-key
	// limits already bound the input, so this is defense-in-depth against a
	// pathological extension count rather than a live vulnerability.
	maxCEBinaryExtensions = 128
)

// ceStandardAttrs are the context-attribute names handled as typed event fields
// (the rest of the cloudEvents:-prefixed headers are extensions). datacontenttype
// is listed so a stray cloudEvents:datacontenttype header is not mistaken for an
// extension; per the binding it travels on the content-type property instead.
var ceStandardAttrs = map[string]struct{}{
	"specversion":     {},
	"id":              {},
	"source":          {},
	"type":            {},
	"subject":         {},
	"dataschema":      {},
	"time":            {},
	"datacontenttype": {},
}

func asEvent(v any) (*event.Event, error) {
	switch e := v.(type) {
	case *event.Event:
		if e == nil {
			return nil, fmt.Errorf("%w: Encode requires a non-nil *cloudevents.Event", ErrInvalidMessage)
		}
		return e, nil
	case event.Event:
		return &e, nil
	default:
		return nil, fmt.Errorf("%w: value of type %T is not a cloudevents.Event", ErrInvalidMessage, v)
	}
}

func asEventDest(v any) (*event.Event, error) {
	e, ok := v.(*event.Event)
	if !ok || e == nil {
		return nil, fmt.Errorf("%w: Decode requires a non-nil *cloudevents.Event destination", ErrInvalidMessage)
	}
	return e, nil
}

// ceStructuredCodec serialises the full CloudEvent JSON envelope into the body,
// delegating to the SDK's JSON event format.
type ceStructuredCodec struct {
	// marshal serialises the validated event. It is a field — set to json.Marshal
	// by NewCloudEventsStructured — so a test can inject a failure and cover the
	// otherwise-unreachable json.Marshal error branch in Encode (a validated
	// cloudevents.Event always marshals in practice). Injecting per-instance avoids
	// a mutable package global; construct via NewCloudEventsStructured (the zero
	// value carries no marshaler).
	marshal func(any) ([]byte, error)
}

// NewCloudEventsStructured returns a codec that serialises a cloudevents.Event as
// a full CloudEvents JSON envelope in the message body (content-type
// application/cloudevents+json). Serialization is delegated to the SDK event
// format, so data / data_base64, extensions, and time follow the spec exactly.
func NewCloudEventsStructured() Codec {
	return &ceStructuredCodec{marshal: json.Marshal}
}

func (c *ceStructuredCodec) ContentType() string { return ceStructuredContentType }

func (c *ceStructuredCodec) Encode(v any) ([]byte, error) {
	ev, err := asEvent(v)
	if err != nil {
		return nil, err
	}
	if err := ev.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	out, err := c.marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return out, nil
}

func (c *ceStructuredCodec) Decode(data []byte, v any) error {
	ev, err := asEventDest(v)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, ev); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	// The SDK's UnmarshalJSON rejects a missing/unknown specversion but not the
	// other required attributes, so an envelope like {"specversion":"1.0"} would
	// otherwise decode into a malformed event with no signal. Validate symmetrically
	// mirrors Encode and the binary DecodeWithHeaders path.
	if err := ev.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return nil
}

// ceBinaryCodec implements CloudEvents binary content mode of the CloudEvents
// AMQP Protocol Binding.
type ceBinaryCodec struct{}

// NewCloudEventsBinary returns a codec for CloudEvents binary content mode (the
// CloudEvents AMQP Protocol Binding): the event data is the AMQP body,
// datacontenttype maps to the AMQP content-type property, and every other
// context attribute (and extension) maps to a cloudEvents:-prefixed AMQP header.
//
// Attributes are carried as strings (formatted via the SDK's canonical
// types.Format), so a non-string extension is narrowed to its string form on the
// wire and round-trips back as a string; on decode, a cloudEvents: header whose
// value is not a string is treated as absent. For type-preserving extensions use
// NewCloudEventsStructured. The decoder validates the reconstructed event and
// returns ErrInvalidMessage for any attribute the CloudEvents spec rejects.
//
// It implements HeaderCodec and is meant to be used through the library's
// publisher and consumer, which route the headers and content-type
// automatically. Its plain Encode/Decode reject use (with ErrInvalidMessage) so
// the cloudEvents: attributes can never be silently dropped by a caller that
// bypasses the header-aware path. ContentType returns "" because the per-event
// content type is supplied dynamically by EncodeWithHeaders.
func NewCloudEventsBinary() Codec {
	return &ceBinaryCodec{}
}

func (c *ceBinaryCodec) ContentType() string { return "" }

func (c *ceBinaryCodec) Encode(any) ([]byte, error) {
	return nil, fmt.Errorf("%w: CloudEvents binary mode requires a header-aware publisher; attributes cannot be carried by Encode alone", ErrInvalidMessage)
}

func (c *ceBinaryCodec) Decode([]byte, any) error {
	return fmt.Errorf("%w: CloudEvents binary mode requires a header-aware consumer; attributes cannot be read by Decode alone", ErrInvalidMessage)
}

func (c *ceBinaryCodec) EncodeWithHeaders(v any) ([]byte, map[string]any, string, error) {
	ev, err := asEvent(v)
	if err != nil {
		return nil, nil, "", err
	}
	if err := ev.Validate(); err != nil {
		return nil, nil, "", fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}

	headers := make(map[string]any, 8)
	headers[ceAMQPPrefix+"specversion"] = ev.SpecVersion()
	headers[ceAMQPPrefix+"id"] = ev.ID()
	headers[ceAMQPPrefix+"source"] = ev.Source()
	headers[ceAMQPPrefix+"type"] = ev.Type()
	if s := ev.Subject(); s != "" {
		headers[ceAMQPPrefix+"subject"] = s
	}
	if s := ev.DataSchema(); s != "" {
		headers[ceAMQPPrefix+"dataschema"] = s
	}
	if t := ev.Time(); !t.IsZero() {
		headers[ceAMQPPrefix+"time"] = types.FormatTime(t)
	}
	for name, val := range ev.Extensions() {
		// Defensive: ev.Validate() above already rejects (and SetExtension never
		// stores) any value without a canonical CloudEvents type, so types.Format
		// should not fail here. Kept to guard against SDK type-handling drift.
		s, ferr := types.Format(val)
		if ferr != nil {
			return nil, nil, "", fmt.Errorf("%w: extension %q: %w", ErrInvalidMessage, name, ferr)
		}
		headers[ceAMQPPrefix+name] = s
	}

	return ev.Data(), headers, ev.DataContentType(), nil
}

func (c *ceBinaryCodec) DecodeWithHeaders(body []byte, headers map[string]any, contentType string, v any) error {
	out, err := asEventDest(v)
	if err != nil {
		return err
	}

	// specversion presence is what distinguishes a binary CloudEvent from any
	// other message: a structured envelope (no cloudEvents: headers) fails here.
	specVersion, ok := ceHeaderString(headers, ceAMQPPrefix+"specversion")
	if !ok {
		return fmt.Errorf("%w: missing %sspecversion header; not a binary CloudEvent", ErrInvalidMessage, ceAMQPPrefix)
	}

	ev := event.New(specVersion)
	if ev.Context == nil {
		// event.New leaves Context nil for an unrecognised spec version; reject
		// before any setter dereferences it.
		return fmt.Errorf("%w: unsupported CloudEvents specversion %q", ErrInvalidMessage, specVersion)
	}

	if s, ok := ceHeaderString(headers, ceAMQPPrefix+"id"); ok {
		ev.SetID(s)
	}
	if s, ok := ceHeaderString(headers, ceAMQPPrefix+"source"); ok {
		ev.SetSource(s)
	}
	if s, ok := ceHeaderString(headers, ceAMQPPrefix+"type"); ok {
		ev.SetType(s)
	}
	if s, ok := ceHeaderString(headers, ceAMQPPrefix+"subject"); ok {
		ev.SetSubject(s)
	}
	if s, ok := ceHeaderString(headers, ceAMQPPrefix+"dataschema"); ok {
		ev.SetDataSchema(s)
	}
	if s, ok := ceHeaderString(headers, ceAMQPPrefix+"time"); ok {
		ts, perr := types.ParseTime(s)
		if perr != nil {
			return fmt.Errorf("%w: invalid %stime %q: %w", ErrInvalidMessage, ceAMQPPrefix, s, perr)
		}
		ev.SetTime(ts)
	}

	extCount := 0
	for k := range headers {
		if !strings.HasPrefix(k, ceAMQPPrefix) {
			continue
		}
		name := strings.TrimPrefix(k, ceAMQPPrefix)
		if _, std := ceStandardAttrs[name]; std {
			continue
		}
		s, ok := ceHeaderString(headers, k)
		if !ok {
			continue
		}
		extCount++
		if extCount > maxCEBinaryExtensions {
			return fmt.Errorf("%w: CloudEvents binary message carries more than %d extension attributes", ErrInvalidMessage, maxCEBinaryExtensions)
		}
		ev.SetExtension(name, s)
	}

	if contentType != "" {
		ev.SetDataContentType(contentType)
	}
	if len(body) > 0 {
		ev.DataEncoded = body
		ev.DataBase64 = false
	}

	// The SDK setters record invalid attribute names/values internally instead of
	// returning an error; Validate() is the only place they surface. Without this,
	// a header the SDK rejects (e.g. an extension name with a hyphen) would be
	// silently dropped and the consumer would receive a malformed event with no
	// signal. Validate symmetrically mirrors EncodeWithHeaders, which also validates.
	if err := ev.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}

	*out = ev
	return nil
}

// ceHeaderString reads a cloudEvents: header value as a string. EncodeWithHeaders
// always writes attributes as strings (via types.Format), and the CloudEvents AMQP
// binding carries the core attributes as strings, so only string and []byte (the
// two forms amqp091 produces for AMQP short/long strings) are accepted. Any other
// value type is treated as absent rather than coerced via fmt: a required attribute
// arriving non-string is then reported as missing by Validate, and a non-string
// extension is dropped rather than turned into a surprising stringified value.
func ceHeaderString(headers map[string]any, key string) (string, bool) {
	v, ok := headers[key]
	if !ok {
		return "", false
	}
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

// ensure interfaces are satisfied at compile time.
var (
	_ Codec       = (*ceStructuredCodec)(nil)
	_ Codec       = (*ceBinaryCodec)(nil)
	_ HeaderCodec = (*ceBinaryCodec)(nil)
)
