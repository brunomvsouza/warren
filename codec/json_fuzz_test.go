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

func FuzzCodecJSONStrict(f *testing.F) {
	// valid single-value inputs
	f.Add([]byte(`{"id":1,"name":"test"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(`42`))
	// trailing-data cases that must be rejected
	f.Add([]byte(`{}{}"`))
	f.Add([]byte(`{} garbage`))
	f.Add([]byte(`{"a":1}{"b":2}`))
	// unknown-field cases that must be rejected
	f.Add([]byte(`{"unknown":true}`))
	// empty / whitespace
	f.Add([]byte(``))
	f.Add([]byte("   "))

	// fuzzStrictTarget has two known fields; unknown fields must be rejected by
	// NewJSONStrict (DisallowUnknownFields has no effect on interface{}).
	type fuzzStrictTarget struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	c := codec.NewJSONStrict()
	f.Fuzz(func(t *testing.T, data []byte) {
		var v fuzzStrictTarget
		// must not panic — error is acceptable
		_ = c.Decode(data, &v)
	})
}
