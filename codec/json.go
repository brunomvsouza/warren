package codec

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type jsonCodec struct {
	strict bool
}

// NewJSON returns the default JSON codec.
//
// The codec follows Postel's Law: it is conservative in what it sends (Encode
// emits exactly the fields declared on M) and liberal in what it accepts
// (Decode tolerates unknown fields on the wire). Producer-first deploys — a v2
// service publishing a new field alongside v1 services that have not yet
// rolled — therefore do not poison v1 consumers' DLQs.
//
// For consumer-side schema enforcement (e.g. compliance pipelines where every
// drift must surface), use NewJSONStrict.
func NewJSON() Codec {
	return &jsonCodec{strict: false}
}

// NewJSONStrict returns a JSON codec that rejects unknown fields on Decode.
// Unknown fields surface as ErrInvalidMessage wrapping the json decoder error.
//
// Use this only when consumer-side schema drift MUST be a hard error (e.g.
// regulated pipelines). For the common case prefer NewJSON, which is liberal
// in what it receives so producer-first deploys do not break v1 consumers.
func NewJSONStrict() Codec {
	return &jsonCodec{strict: true}
}

func (c *jsonCodec) ContentType() string { return "application/json" }

func (c *jsonCodec) Encode(v any) (out []byte, err error) {
	out, err = json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return out, nil
}

func (c *jsonCodec) Decode(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	if c.strict {
		dec.DisallowUnknownFields()
	}
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return nil
}

// ensure interface is satisfied at compile time.
var _ Codec = (*jsonCodec)(nil)
