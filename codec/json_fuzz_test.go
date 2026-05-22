package codec_test

import (
	"testing"

	"github.com/brunomvsouza/warren/codec"
)

func FuzzCodecJSON(f *testing.F) {
	f.Add([]byte(`{"id":1,"name":"test"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(``))

	c := codec.NewJSON()
	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		// must not panic — error is acceptable
		_ = c.Decode(data, &v)
	})
}
