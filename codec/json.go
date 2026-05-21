package codec

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type jsonCodec struct {
	strict bool
}

// NewJSON returns a strict JSON codec that rejects unknown fields on Decode.
// Unknown fields surface as ErrInvalidMessage wrapping the json decoder error.
func NewJSON() Codec {
	return &jsonCodec{strict: true}
}

// NewJSONLax returns a JSON codec that tolerates unknown fields on Decode.
// Use for back-compat scenarios where schema drift must be silent; document the trade-off.
func NewJSONLax() Codec {
	return &jsonCodec{strict: false}
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
