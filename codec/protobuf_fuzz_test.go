package codec_test

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/brunomvsouza/warren/codec"
)

func FuzzCodecProtobuf(f *testing.F) {
	c := codec.NewProtobuf()

	// valid encodings as seeds
	if b, err := c.Encode(timestamppb.New(time.Unix(1716800000, 0).UTC())); err == nil {
		f.Add(b)
	}
	if s, err := structpb.NewStruct(map[string]any{"k": "v"}); err == nil {
		if b, err := c.Encode(s); err == nil {
			f.Add(b)
		}
	}
	// adversarial / malformed wire inputs
	f.Add([]byte{})
	f.Add([]byte{0x08})                         // truncated varint field
	f.Add([]byte{0x08, 0x01})                   // field 1 varint = 1
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}) // invalid varint run
	f.Add([]byte(strings.Repeat("\x0a", 1000))) // many length-delimited tags
	f.Add([]byte("\xff\xfe"))                   // invalid UTF-8 bytes

	f.Fuzz(func(t *testing.T, data []byte) {
		// Decode into a proto message must never panic; an error is acceptable.
		var ts timestamppb.Timestamp
		if err := c.Decode(data, &ts); err == nil {
			// Re-encoding a successfully decoded message must also never panic.
			_, _ = c.Encode(&ts)
		}
	})
}
