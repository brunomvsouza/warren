// Package codec provides message encoding and decoding for AMQP publishers and consumers.
// The default JSON codec is strict (DisallowUnknownFields); use NewJSONLax for back-compat scenarios.
package codec

// Codec encodes and decodes message payloads.
type Codec interface {
	// Encode serialises v into bytes. Returns ErrInvalidMessage on failure.
	Encode(v any) ([]byte, error)
	// Decode deserialises data into v. Returns ErrInvalidMessage on failure.
	Decode(data []byte, v any) error
	// ContentType returns the MIME content-type produced by Encode (e.g. "application/json").
	ContentType() string
}
