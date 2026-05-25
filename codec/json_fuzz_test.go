package codec_test

import (
	"strings"
	"testing"

	"github.com/brunomvsouza/warren/codec"
)

func FuzzCodecJSON(f *testing.F) {
	// basic valid inputs
	f.Add([]byte(`{"id":1,"name":"test"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`"string"`))
	f.Add([]byte(``))
	// lax codec must accept unknown fields — exercise the Postel's Law path
	f.Add([]byte(`{"id":1,"name":"test","extra":true}`))
	f.Add([]byte(`{"id":2,"name":"foo","unknown_field":"bar","another":42}`))
	// adversarial inputs
	f.Add([]byte(strings.Repeat("[", 500)))                    // deeply nested array
	f.Add([]byte(`{"id":` + strings.Repeat("9", 100) + `}`))   // large integer
	f.Add([]byte("\xff\xfe"))                                  // invalid UTF-8
	f.Add([]byte(`{"id":1` + strings.Repeat(`,`, 1000) + `}`)) // many commas

	// fuzzOrder has two known fields; the lax codec must silently accept extra fields.
	type fuzzOrder struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	c := codec.NewJSON()
	f.Fuzz(func(t *testing.T, data []byte) {
		var v fuzzOrder
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
	// adversarial inputs
	f.Add([]byte(strings.Repeat("[", 500)))                  // deeply nested array
	f.Add([]byte(`{"id":` + strings.Repeat("9", 100) + `}`)) // large integer
	f.Add([]byte("\xff\xfe"))                                // invalid UTF-8

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
