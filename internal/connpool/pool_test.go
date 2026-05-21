package connpool_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/brunomvsouza/amqp/internal/connpool"
)

// — PinIndex ——————————————————————————————————————————————————————————————

func TestPinIndex_sameTagReturnsSameIndex(t *testing.T) {
	assert.Equal(t, connpool.PinIndex("my-consumer", 4), connpool.PinIndex("my-consumer", 4))
}

func TestPinIndex_sizeOne_alwaysZero(t *testing.T) {
	assert.Equal(t, 0, connpool.PinIndex("any-tag", 1))
	assert.Equal(t, 0, connpool.PinIndex("", 1))
}

func TestPinIndex_sizeZero_alwaysZero(t *testing.T) {
	assert.Equal(t, 0, connpool.PinIndex("any-tag", 0))
}

func TestPinIndex_emptyTag_isStable(t *testing.T) {
	assert.Equal(t, connpool.PinIndex("", 8), connpool.PinIndex("", 8))
}

func TestPinIndex_differentTags_distributeAcrossIndices(t *testing.T) {
	// With n=128 the FNV-1a hash should scatter these tags across multiple buckets.
	seen := make(map[int]bool)
	tags := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	for _, tag := range tags {
		seen[connpool.PinIndex(tag, 128)] = true
	}
	assert.Greater(t, len(seen), 1, "different tags should map to more than one index")
}

func TestPinIndex_inBounds(t *testing.T) {
	for _, n := range []int{2, 3, 5, 10, 100} {
		for _, tag := range []string{"a", "b", "c", "ctag-uuid-1", ""} {
			idx := connpool.PinIndex(tag, n)
			assert.GreaterOrEqual(t, idx, 0, "index must be non-negative (tag=%q, n=%d)", tag, n)
			assert.Less(t, idx, n, "index must be < n (tag=%q, n=%d)", tag, n)
		}
	}
}

// — ConnName ——————————————————————————————————————————————————————————————

func TestConnName_publisherSuffix(t *testing.T) {
	assert.Equal(t, "base-pub-0", connpool.ConnName("base", "publisher", 0))
	assert.Equal(t, "base-pub-3", connpool.ConnName("base", "publisher", 3))
}

func TestConnName_consumerSuffix(t *testing.T) {
	assert.Equal(t, "base-con-0", connpool.ConnName("base", "consumer", 0))
	assert.Equal(t, "base-con-1", connpool.ConnName("base", "consumer", 1))
}

func TestConnName_includesBaseAndIndex(t *testing.T) {
	name := connpool.ConnName("myapp-host-12345", "publisher", 2)
	assert.Equal(t, "myapp-host-12345-pub-2", name)
}

func TestConnName_unknownRole_fallsBackToPubSuffix(t *testing.T) {
	// Only "consumer" maps to "con"; anything else uses "pub".
	assert.Equal(t, "base-pub-0", connpool.ConnName("base", "other", 0))
}
