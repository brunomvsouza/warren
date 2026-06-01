package codec

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// I1: knownJSONFields is a pure function of the static type, so its result is
// memoized per reflect.Type. Repeated calls must return the SAME cached map
// instead of rebuilding (reflection walk + allocation) on every Decode.
func TestKnownJSONFields_MemoizedPerType(t *testing.T) {
	type cachedProbe struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
		skip string //nolint:unused // exercises the unexported-field path
	}
	rt := reflect.TypeOf(cachedProbe{})

	first := knownJSONFields(rt)
	second := knownJSONFields(rt)

	// Same instance → the second call hit the cache rather than rebuilding.
	require.True(t, reflect.ValueOf(first).Pointer() == reflect.ValueOf(second).Pointer(),
		"expected the second call to return the cached map instance")

	// And the cached content is still correct (lowercased tag names, unexported dropped).
	assert.Equal(t, map[string]struct{}{"id": {}, "name": {}}, first)
}
