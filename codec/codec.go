// Package codec provides message encoding and decoding for AMQP publishers and consumers.
//
// The default JSON codec (NewJSON) follows Postel's Law: conservative on send,
// liberal on receive — Encode emits exactly the declared fields, Decode tolerates
// unknown fields on the wire so producer-first deploys do not poison v1
// consumers' DLQs. NewJSONStrict opts into DisallowUnknownFields when consumer-side
// schema drift must be a hard error.
package codec

import "errors"

// ErrInvalidMessage is returned by Encode/Decode when the payload cannot be
// processed. Publishers and consumers in the amqp package wrap this error into
// warren.ErrInvalidMessage so callers can use either sentinel with errors.Is.
var ErrInvalidMessage = errors.New("codec: invalid message")

// Codec encodes and decodes message payloads.
type Codec interface {
	// Encode serialises v into bytes. Returns ErrInvalidMessage on failure.
	Encode(v any) ([]byte, error)
	// Decode deserialises data into v. Returns ErrInvalidMessage on failure.
	Decode(data []byte, v any) error
	// ContentType returns the MIME content-type produced by Encode (e.g. "application/json").
	ContentType() string
}

// HeaderCodec is an optional interface a Codec may implement when its wire
// format spans the message body, AMQP headers, and the content-type property.
// Publishers and consumers detect it by type assertion: on publish the headers
// returned by EncodeWithHeaders are merged into Message.Headers and a non-empty
// contentType overrides Message.ContentType; on consume the delivery's headers
// and content-type property are passed to DecodeWithHeaders. A Codec that does
// not implement HeaderCodec uses the plain Encode/Decode path unchanged.
//
// The CloudEvents binary-mode codec (NewCloudEventsBinary) is the built-in
// implementation: per the CloudEvents AMQP Protocol Binding, the event data
// travels as the body, datacontenttype as the content-type property, and every
// other context attribute as a cloudEvents:-prefixed header.
type HeaderCodec interface {
	Codec
	// EncodeWithHeaders serialises v into a body, a set of AMQP headers, and a
	// content-type. Returns ErrInvalidMessage on failure. Header values must be
	// AMQP field-table compatible types; the publisher validates them and rejects
	// the publish with ErrInvalidMessage otherwise.
	EncodeWithHeaders(v any) (body []byte, headers map[string]any, contentType string, err error)
	// DecodeWithHeaders deserialises body plus headers plus content-type into v.
	// It must only read headers, never mutate the provided map. Returns
	// ErrInvalidMessage on failure.
	DecodeWithHeaders(body []byte, headers map[string]any, contentType string, v any) error
}
