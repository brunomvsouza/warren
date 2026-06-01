package codec_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/brunomvsouza/warren/codec"
)

// TestNewJSON_UnknownFieldObserver_FiresOnUnknownField is the T56 acceptance test:
// decoding a payload with an unknown field triggers the observer WITHOUT failing the
// lax decode.
func TestNewJSON_UnknownFieldObserver_FiresOnUnknownField(t *testing.T) {
	var paths []string
	c := codec.NewJSON(codec.WithUnknownFieldObserver(func(path string) {
		paths = append(paths, path)
	}))

	var o order
	err := c.Decode([]byte(`{"id":1,"unknown_new_field":"test"}`), &o)
	require.NoError(t, err, "lax decode must still succeed despite the unknown field")
	assert.Equal(t, 1, o.ID, "known fields must still decode")
	assert.Equal(t, []string{"unknown_new_field"}, paths, "observer must fire once for the unknown field")
}

func TestNewJSON_UnknownFieldObserver_NotFiredWhenAllKnown(t *testing.T) {
	called := false
	c := codec.NewJSON(codec.WithUnknownFieldObserver(func(string) { called = true }))

	var o order
	err := c.Decode([]byte(`{"id":1,"name":"test"}`), &o)
	require.NoError(t, err)
	assert.False(t, called, "observer must not fire when every field is known")
}

func TestNewJSON_UnknownFieldObserver_MultipleUnknown(t *testing.T) {
	var paths []string
	c := codec.NewJSON(codec.WithUnknownFieldObserver(func(path string) {
		paths = append(paths, path)
	}))

	var o order
	err := c.Decode([]byte(`{"id":1,"drift_a":1,"drift_b":2}`), &o)
	require.NoError(t, err)
	sort.Strings(paths)
	assert.Equal(t, []string{"drift_a", "drift_b"}, paths, "observer must fire once per unknown field")
}

// encoding/json matches field names case-insensitively, so a wire key that differs
// only in case from a known field is NOT schema drift and must not be reported.
func TestNewJSON_UnknownFieldObserver_CaseInsensitiveKnownNotReported(t *testing.T) {
	var paths []string
	c := codec.NewJSON(codec.WithUnknownFieldObserver(func(path string) {
		paths = append(paths, path)
	}))

	var o order
	err := c.Decode([]byte(`{"ID":1,"Name":"test"}`), &o)
	require.NoError(t, err)
	assert.Empty(t, paths, "case-only differences match a known field and are not drift")
}

// A non-struct target (e.g. a map) has no fixed schema, so the observer must never
// fire and decode must behave exactly as before.
func TestNewJSON_UnknownFieldObserver_NonStructTargetIgnored(t *testing.T) {
	called := false
	c := codec.NewJSON(codec.WithUnknownFieldObserver(func(string) { called = true }))

	var m map[string]any
	err := c.Decode([]byte(`{"id":1,"anything":2}`), &m)
	require.NoError(t, err)
	assert.False(t, called, "no fixed schema → no drift signal for a map target")
}

// Without an observer the lax codec keeps tolerating unknown fields silently and the
// extra detection pass is skipped entirely (no behaviour change).
func TestNewJSON_NoObserver_StillLax(t *testing.T) {
	c := codec.NewJSON()
	var o order
	err := c.Decode([]byte(`{"id":1,"extra":true}`), &o)
	require.NoError(t, err)
	assert.Equal(t, order{ID: 1}, o)
}
