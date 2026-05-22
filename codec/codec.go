// Package codec provides message encoding and decoding for AMQP publishers and consumers.
// The default JSON codec is strict (DisallowUnknownFields); use NewJSONLax for back-compat scenarios.
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
